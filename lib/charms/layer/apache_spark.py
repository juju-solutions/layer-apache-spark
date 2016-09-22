import os
from glob import glob
from path import Path
from subprocess import CalledProcessError
from subprocess import check_call

import jujuresources
from charmhelpers.core import hookenv
from charmhelpers.core import host
from charmhelpers.core import unitdata
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler
from jujubigdata import utils
from charms.templating.jinja2 import render


class ResourceError(Exception):
    pass


# Main Spark class for callbacks
class Spark(object):
    def __init__(self, dist_config):
        self.dist_config = dist_config
        self.resources = {
            'spark-1.6.1-hadoop2.6.0': 'spark-1.6.1-hadoop2.6.0',
        }

    def install(self):
        version = hookenv.config()['spark_version']
        spark_path = self.extract_spark_binary(version)
        os.symlink(spark_path, self.dist_config.path('spark'))
        unitdata.kv().set('spark.version', version)

        self.dist_config.add_users()
        self.dist_config.add_dirs()
        self.dist_config.add_packages()

        # allow ubuntu user to ssh to itself so spark can ssh to its worker
        # in local/standalone modes
        utils.install_ssh_key('ubuntu', utils.get_ssh_key('ubuntu'))

        utils.initialize_kv_host()
        utils.manage_etc_hosts()
        hostname = hookenv.local_unit().replace('/', '-')
        etc_hostname = Path('/etc/hostname')
        etc_hostname.write_text(hostname)
        check_call(['hostname', '-F', etc_hostname])
        unitdata.kv().set('spark.installed', True)
        unitdata.kv().flush(True)

    def extract_spark_binary(self, version):
        spark_path = Path("{}-{}".format(self.dist_config.path('spark'), version))
        try:
            filename = hookenv.resource_get('spark')
            if not filename:
                raise ResourceError("unable to fetch spark resource")
            if Path(filename).size == 0:
                # work around charm store resource upload issue
                # by falling-back to pulling from S3
                raise NotImplementedError()
            extracted = Path(ArchiveUrlFetchHandler().install('file://' + filename))
            extracted.dirs()[0].copytree(spark_path)  # only copy nested dir
        except NotImplementedError:
            resource = self.resources['spark-{}'.format(version)]
            if not utils.verify_resources(resource)():
                raise ResourceError("Failed to fetch Spark {} binary".format(version))
            jujuresources.install(resource,
                                  destination=spark_path,
                                  skip_top_level=True)

        spark_conf = spark_path / "conf"
        spark_conf_orig = spark_path / "conf.orig"
        spark_conf_orig.rmtree_p()
        spark_conf.copytree(spark_conf_orig)

        return spark_path

    def get_spark_versions(self):
        l = []
        for i in self.resources.keys():
            l.append(i.replace('spark-', ''))
        return l

    def get_current_version(self):
        current_version = unitdata.kv().get('spark.version')
        return current_version

    def switch_version(self, to_version):
        spark_resource = 'spark-{}'.format(to_version)
        if not (spark_resource in self.resources):
            raise ResourceError("No resource for spark version {}".format(to_version))

        unitdata.kv().set('spark.upgrading', True)
        unitdata.kv().flush(True)
        self.stop()
        new_spark_path = self.extract_spark_binary(self.resources[spark_resource], to_version)
        os.unlink(self.dist_config.path('spark'))
        os.symlink(new_spark_path, self.dist_config.path('spark'))
        self.setup_spark_config()
        prev_version = unitdata.kv().get('spark.version', '')
        unitdata.kv().set('spark.previous-version', prev_version)
        unitdata.kv().set('spark.version', to_version)

        if unitdata.kv().get('zookeepers.available', False):
            zk_units = unitdata.kv().get('zookeeper.units', '')
            self.configure_ha(zk_units)
        if unitdata.kv().get('hdfs.available', False):
            self.configure_yarn()

        self.configure()
        unitdata.kv().set('spark.upgrading', False)
        unitdata.kv().flush(True)
        self.start()

    def upgrading(self):
        return unitdata.kv().get('spark.upgrading', False)

    def configure_yarn_mode(self):
        # put the spark jar in hdfs
        spark_assembly_jar = glob('{}/lib/spark-assembly-*.jar'.format(
                                  self.dist_config.path('spark')))[0]
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p',
                     '/user/ubuntu/share/lib')
        try:
            utils.run_as('hdfs', 'hdfs', 'dfs', '-put', spark_assembly_jar,
                         '/user/ubuntu/share/lib/spark-assembly.jar')
        except CalledProcessError:
            pass  # jar already in HDFS from another Spark

        with utils.environment_edit_in_place('/etc/environment') as env:
            env['SPARK_JAR'] = "hdfs:///user/ubuntu/share/lib/spark-assembly.jar"

        # create hdfs storage space for history server
        dc = self.dist_config
        prefix = dc.path('log_prefix')
        events_dir = dc.path('spark_events')
        events_dir = 'hdfs:///{}'.format(events_dir.replace(prefix, ''))
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p', events_dir)
        utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', '-R', 'ubuntu:hadoop',
                     events_dir)

        # create hdfs storage space for spark-bench
        utils.run_as('hdfs', 'hdfs', 'dfs', '-mkdir', '-p',
                     '/user/ubuntu/spark-bench')
        utils.run_as('hdfs', 'hdfs', 'dfs', '-chown', '-R', 'ubuntu:hadoop',
                     '/user/ubuntu/spark-bench')

        # ensure user-provided Hadoop works
        hadoop_classpath = utils.run_as('hdfs', 'hadoop', 'classpath',
                                        capture_output=True)
        spark_env = self.dist_config.path('spark_conf') / 'spark-env.sh'
        utils.re_edit_in_place(spark_env, {
            r'.*SPARK_DIST_CLASSPATH.*': 'SPARK_DIST_CLASSPATH={}'.format(hadoop_classpath),
        }, append_non_matches=True)

        # update spark-defaults
        spark_conf = self.dist_config.path('spark_conf') / 'spark-defaults.conf'
        utils.re_edit_in_place(spark_conf, {
            r'.*spark.master .*': 'spark.master {}'.format(self.get_master()),
        }, append_non_matches=True)

        unitdata.kv().set('hdfs.available', True)
        unitdata.kv().flush(True)

    def disable_yarn_mode(self):
        # put the spark jar in hdfs
        with utils.environment_edit_in_place('/etc/environment') as env:
            env['SPARK_JAR'] = glob('{}/lib/spark-assembly-*.jar'.format(
                                    self.dist_config.path('spark')))[0]

        # update spark-defaults
        spark_conf = self.dist_config.path('spark_conf') / 'spark-defaults.conf'
        utils.re_edit_in_place(spark_conf, {
            r'.*spark.master .*': 'spark.master {}'.format(self.get_master()),
        }, append_non_matches=True)

        unitdata.kv().set('hdfs.available', False)
        unitdata.kv().flush(True)

    def configure_ha(self, zk_units):
        unitdata.kv().set('zookeeper.units', zk_units)
        zks = []
        for unit in zk_units:
            ip = utils.resolve_private_address(unit['host'])
            zks.append("%s:%s" % (ip, unit['port']))

        zk_connect = ",".join(zks)

        daemon_opts = ('-Dspark.deploy.recoveryMode=ZOOKEEPER '
                       '-Dspark.deploy.zookeeper.url={}'.format(zk_connect))

        spark_env = self.dist_config.path('spark_conf') / 'spark-env.sh'
        utils.re_edit_in_place(spark_env, {
            r'.*SPARK_DAEMON_JAVA_OPTS.*': 'SPARK_DAEMON_JAVA_OPTS=\"{}\"'.format(daemon_opts),
            r'.*SPARK_MASTER_IP.*': '# SPARK_MASTER_IP',
        })
        unitdata.kv().set('zookeepers.available', True)
        unitdata.kv().flush(True)

    def disable_ha(self):
        spark_env = self.dist_config.path('spark_conf') / 'spark-env.sh'
        utils.re_edit_in_place(spark_env, {
            r'.*SPARK_DAEMON_JAVA_OPTS.*': '# SPARK_DAEMON_JAVA_OPTS',
        })
        unitdata.kv().set('zookeepers.available', False)
        unitdata.kv().flush(True)

    def setup_spark_config(self):
        '''
        copy the default configuration files to spark_conf property
        defined in dist.yaml
        '''
        default_conf = self.dist_config.path('spark') / 'conf.orig'
        spark_conf = self.dist_config.path('spark_conf')
        spark_conf.rmtree_p()
        default_conf.copytree(spark_conf)
        # Now remove the conf included in the tarball and symlink our real conf
        target_conf = self.dist_config.path('spark') / 'conf'
        if target_conf.islink():
            target_conf.unlink()
        else:
            target_conf.rmtree_p()
        spark_conf.symlink(target_conf)
        spark_env = self.dist_config.path('spark_conf') / 'spark-env.sh'
        if not spark_env.exists():
            (self.dist_config.path('spark_conf') / 'spark-env.sh.template').copy(spark_env)
        spark_default = self.dist_config.path('spark_conf') / 'spark-defaults.conf'
        if not spark_default.exists():
            (self.dist_config.path('spark_conf') / 'spark-defaults.conf.template').copy(spark_default)
        spark_log4j = self.dist_config.path('spark_conf') / 'log4j.properties'
        if not spark_log4j.exists():
            spark_log4j.write_lines([
                "log4j.rootLogger=INFO, rolling",
                "",
                "log4j.appender.rolling=org.apache.log4j.RollingFileAppender",
                "log4j.appender.rolling.layout=org.apache.log4j.PatternLayout",
                "log4j.appender.rolling.layout.conversionPattern=[%d] %p %m (%c)%n",
                "log4j.appender.rolling.maxFileSize=50MB",
                "log4j.appender.rolling.maxBackupIndex=5",
                "log4j.appender.rolling.file=/var/log/spark/spark.log",
                "log4j.appender.rolling.encoding=UTF-8",
                "",
                "log4j.logger.org.apache.spark=WARN",
                "log4j.logger.org.eclipse.jetty=WARN",
            ])

    def setup_init_scripts(self):
        templates_list = ['history', 'master', 'slave']
        for template in templates_list:
            if host.init_is_systemd():
                template_path = '/etc/systemd/system/spark-{}.service'.format(template)
            else:
                template_path = '/etc/init/spark-{}.conf'.format(template)
            if os.path.exists(template_path):
                os.remove(template_path)

        self.stop()

        mode = hookenv.config()['spark_execution_mode']
        templates_list = ['history']
        if mode == 'standalone':
            templates_list.append('master')
            templates_list.append('slave')

        for template in templates_list:
            template_name = '{}-upstart.conf'.format(template)
            template_path = '/etc/init/spark-{}.conf'.format(template)
            if host.init_is_systemd():
                template_name = '{}-systemd.conf'.format(template)
                template_path = '/etc/systemd/system/spark-{}.service'.format(template)

            render(
                template_name,
                template_path,
                context={
                    'spark_bin': self.dist_config.path('spark'),
                    'master': self.get_master()
                },
            )
            if host.init_is_systemd():
                utils.run_as('root', 'systemctl', 'enable', 'spark-{}.service'.format(template))

        if host.init_is_systemd():
            utils.run_as('root', 'systemctl', 'daemon-reload')

    def install_demo(self):
        '''
        Install sparkpi.sh to /home/ubuntu (executes SparkPI example app)
        '''
        demo_source = 'scripts/sparkpi.sh'
        demo_target = '/home/ubuntu/sparkpi.sh'
        Path(demo_source).copy(demo_target)
        Path(demo_target).chmod(0o755)
        Path(demo_target).chown('ubuntu', 'hadoop')

    def is_spark_local(self):
        # spark is local if our execution mode is 'local*' or 'standalone'
        mode = hookenv.config()['spark_execution_mode']
        if mode.startswith('local'):
            return True
        else:
            return False

    def update_peers(self, node_list):
        '''
        This method wtill return True if the master peer was updated.
        False otherwise.
        '''
        old_master = unitdata.kv().get('spark_master.ip', 'not_set')
        master_ip = ''
        if not node_list:
            hookenv.log("No peers yet. Acting as master.")
            master_ip = utils.resolve_private_address(hookenv.unit_private_ip())
            nodes = [(hookenv.local_unit(), master_ip)]
            unitdata.kv().set('spark_all_master.ips', nodes)
            unitdata.kv().set('spark_master.ip', master_ip)
        else:
            # Use as master the node with minimum Id
            # Any ordering is fine here. Lexicografical ordering too.
            node_list.sort()
            master_ip = utils.resolve_private_address(node_list[0][1])
            unitdata.kv().set('spark_master.ip', master_ip)
            unitdata.kv().set('spark_all_master.ips', node_list)
            hookenv.log("Updating master ip to {}.".format(master_ip))

        unitdata.kv().set('spark_master.is_set', True)
        unitdata.kv().flush(True)
        # Incase of an HA setup adding peers must be treated as a potential
        # mastr change
        if (old_master != master_ip) or unitdata.kv().get('zookeepers.available', False):
            return True
        else:
            return False

    def get_master_ip(self):
        if not unitdata.kv().get('spark_master.is_set', False):
            self.update_peers([])

        return unitdata.kv().get('spark_master.ip')

    def is_master(self):
        unit_ip = utils.resolve_private_address(hookenv.unit_private_ip())
        master_ip = self.get_master_ip()
        return unit_ip == master_ip

    def get_all_master_ips(self):
        if not unitdata.kv().get('spark_master.is_set', False):
            self.update_peers([])

        return [p[1] for p in unitdata.kv().get('spark_all_master.ips')]

    # translate our execution_mode into the appropriate --master value
    def get_master(self):
        mode = hookenv.config()['spark_execution_mode']
        zks = unitdata.kv().get('zookeepers.available', False)
        master = None
        if mode.startswith('local') or mode == 'yarn-cluster':
            master = mode
        elif mode == 'standalone' and zks:
            master_ips = self.get_all_master_ips()
            nodes = []
            for ip in master_ips:
                nodes.append('{}:7077'.format(ip))
            nodes_str = ','.join(nodes)
            master = 'spark://{}'.format(nodes_str)
        elif mode == 'standalone' and (not zks):
            master_ip = self.get_master_ip()
            master = 'spark://{}:7077'.format(master_ip)
        elif mode.startswith('yarn'):
            master = 'yarn-client'
        return master

    def configure_classpaths(self, client_classpaths=None):
        etc_env = utils.read_etc_env()
        extra_classpath = etc_env.get('HADOOP_EXTRA_CLASSPATH', '')
        if client_classpaths:
            extra_classpath += ':' + ':'.join(client_classpaths)
        spark_conf = self.dist_config.path('spark_conf') / 'spark-defaults.conf'
        utils.re_edit_in_place(spark_conf, {
            r'.*spark.driver.extraClassPath .*': 'spark.driver.extraClassPath {}'.format(extra_classpath),
            r'.*spark.executor.extraClassPath .*': 'spark.executor.extraClassPath {}'.format(extra_classpath),
        }, append_non_matches=True)

    def configure(self):
        '''
        Configure spark environment for all users
        '''
        dc = self.dist_config
        spark_home = self.dist_config.path('spark')
        spark_bin = spark_home / 'bin'

        # handle tuning options that may be set as percentages
        driver_mem = '1g'
        req_driver_mem = hookenv.config()['driver_memory']
        executor_mem = '1g'
        req_executor_mem = hookenv.config()['executor_memory']
        if req_driver_mem.endswith('%'):
            if self.is_spark_local():
                mem_mb = host.get_total_ram() / 1024 / 1024
                req_percentage = float(req_driver_mem.strip('%')) / 100
                driver_mem = str(int(mem_mb * req_percentage)) + 'm'
            else:
                hookenv.log("driver_memory percentage in non-local mode. Using 1g default.",
                            level=None)
        else:
            driver_mem = req_driver_mem

        if req_executor_mem.endswith('%'):
            if self.is_spark_local():
                mem_mb = host.get_total_ram() / 1024 / 1024
                req_percentage = float(req_executor_mem.strip('%')) / 100
                executor_mem = str(int(mem_mb * req_percentage)) + 'm'
            else:
                hookenv.log("executor_memory percentage in non-local mode. Using 1g default.",
                            level=None)
        else:
            executor_mem = req_executor_mem

        # update environment variables
        with utils.environment_edit_in_place('/etc/environment') as env:
            if spark_bin not in env['PATH']:
                env['PATH'] = ':'.join([env['PATH'], spark_bin])
            env['MASTER'] = self.get_master()
            env['PYSPARK_DRIVER_PYTHON'] = "ipython"
            env['SPARK_CONF_DIR'] = self.dist_config.path('spark_conf')
            env['SPARK_DRIVER_MEMORY'] = driver_mem
            env['SPARK_EXECUTOR_MEMORY'] = executor_mem
            env['SPARK_HOME'] = spark_home

        events_dir = 'file://{}'.format(dc.path('spark_events'))
        if unitdata.kv().get('hdfs.available', False):
            prefix = dc.path('log_prefix')
            events_dir = dc.path('spark_events')
            events_dir = 'hdfs:///{}'.format(events_dir.replace(prefix, ''))

        # update spark-defaults
        spark_conf = self.dist_config.path('spark_conf') / 'spark-defaults.conf'
        utils.re_edit_in_place(spark_conf, {
            r'.*spark.master .*': 'spark.master {}'.format(self.get_master()),
            r'.*spark.eventLog.enabled .*': 'spark.eventLog.enabled true',
            r'.*spark.history.fs.logDirectory .*': 'spark.history.fs.logDirectory {}'.format(
                events_dir),
            r'.*spark.eventLog.dir .*': 'spark.eventLog.dir {}'.format(events_dir),
        }, append_non_matches=True)

        # update spark-env
        spark_env = self.dist_config.path('spark_conf') / 'spark-env.sh'
        utils.re_edit_in_place(spark_env, {
            r'.*SPARK_DRIVER_MEMORY.*': 'SPARK_DRIVER_MEMORY={}'.format(driver_mem),
            r'.*SPARK_EXECUTOR_MEMORY.*': 'SPARK_EXECUTOR_MEMORY={}'.format(executor_mem),
            r'.*SPARK_LOG_DIR.*': 'SPARK_LOG_DIR={}'.format(self.dist_config.path('spark_logs')),
            r'.*SPARK_WORKER_DIR.*': 'SPARK_WORKER_DIR={}'.format(self.dist_config.path('spark_work')),
        })

        # If zookeeper is available we should be in HA mode so we should not set the MASTER_IP
        if not unitdata.kv().get('zookeepers.available', False):
            master_ip = self.get_master_ip()
            utils.re_edit_in_place(spark_env, {
                r'.*SPARK_MASTER_IP.*': 'SPARK_MASTER_IP={}'.format(master_ip),
            })

        # manage SparkBench
        install_sb = hookenv.config()['spark_bench_enabled']
        sb_dir = '/home/ubuntu/spark-bench'
        if install_sb:
            if not unitdata.kv().get('spark_bench.installed', False):
                if utils.cpu_arch() == 'ppc64le':
                    sb_url = hookenv.config()['spark_bench_ppc64le']
                else:
                    # TODO: may need more arch cases (go with x86 sb for now)
                    sb_url = hookenv.config()['spark_bench_x86_64']

                Path(sb_dir).rmtree_p()
                au = ArchiveUrlFetchHandler()
                au.install(sb_url, '/home/ubuntu')

                # #####
                # Handle glob if we use a .tgz that doesn't expand to sb_dir
                # sb_archive_dir = glob('/home/ubuntu/spark-bench-*')[0]
                # SparkBench expects to live in ~/spark-bench, so put it there
                # Path(sb_archive_dir).rename(sb_dir)
                # #####

                # comment out mem tunings (let them come from /etc/environment)
                sb_env = Path(sb_dir) / 'conf/env.sh'
                utils.re_edit_in_place(sb_env, {
                    r'^SPARK_DRIVER_MEMORY.*': '# SPARK_DRIVER_MEMORY (use value from environment)',
                    r'^SPARK_EXECUTOR_MEMORY.*': '# SPARK_EXECUTOR_MEMORY (use value from environment)',
                })

                unitdata.kv().set('spark_bench.installed', True)
                unitdata.kv().flush(True)
        else:
            Path(sb_dir).rmtree_p()
            unitdata.kv().set('spark_bench.installed', False)
            unitdata.kv().flush(True)

        self.setup_init_scripts()

    def open_ports(self):
        for port in self.dist_config.exposed_ports('spark'):
            hookenv.open_port(port)

    def close_ports(self):
        for port in self.dist_config.exposed_ports('spark'):
            hookenv.close_port(port)

    def start(self):
        if unitdata.kv().get('spark.uprading', False):
            return

        # stop services (if they're running) to pick up any config change
        self.stop()
        # always start the history server, start master/worker if we're standalone
        host.service_start('spark-history')
        if hookenv.config()['spark_execution_mode'] == 'standalone':
            host.service_start('spark-master')
            host.service_start('spark-slave')

    def stop(self):
        if not unitdata.kv().get('spark.installed', False):
            return
        # Only stop services if they're running
        if utils.jps("HistoryServer"):
            host.service_stop('spark-history')
        if utils.jps("Master"):
            host.service_stop('spark-master')
        if utils.jps("Worker"):
            host.service_stop('spark-slave')
