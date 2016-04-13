# pylint: disable=unused-argument
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state, is_state
from charmhelpers.core import hookenv
from charms.layer.apache_spark import Spark
from charms.layer.hadoop_client import get_dist_config
from charms.reactive.helpers import data_changed

# This file contains the reactive handlers for this charm.  These handlers
# determine when the various relations and other conditions are met so that
# Spark can be deployed.  The states used by this charm to determine this are:
#
#   * spark.installed - This is set by this charm in the code below.
#   * yarn.configured - This is set when spark is configured to use hadoop-yarn.
#   * zookeeper.configured - This is set when spark is related to zk. Implies HA.
#
#   * hadoop.ready - This is set by the hadoop-plugin interface layer once
#                    Yarn & HDFS have reported that ready.  The prefix "hadoop"
#                    in this state is determined by the name of the relation
#                    to the plugin charm in metadata.yaml.
#   * zookeeper.ready - This is set by the zookeeper interface layer.
#


@when_not('spark.installed')
def install_spark():
    dist = get_dist_config()
    spark = Spark(dist)
    hookenv.status_set('maintenance', 'Installing Apache Spark')
    spark.install()
    spark.setup_spark_config()
    spark.install_demo()
    set_state('spark.installed')
    set_state('not.upgrading')


@when('spark.installed', 'not.upgrading')
@when_not('spark.started')
def start_spark():
    hookenv.status_set('maintenance', 'Setting up Apache Spark')
    spark = Spark(get_dist_config())
    spark.configure()
    spark.start()
    spark.open_ports()
    set_state('spark.started')
    report_status(spark)


def report_status(spark):
    if not is_state('not.upgrading'):
        hookenv.status_set('maintenance', 'Preparing for an upgrade')
        return

    mode = hookenv.config()['spark_execution_mode']
    if (not is_state('yarn.configured')) and mode.startswith('yarn'):
        hookenv.status_set('blocked',
                           'Yarn execution mode not available')
        return

    if mode == 'standalone':
        if is_state('zookeeper.configured'):
            mode = mode + " HA"
        elif spark.is_master():
            mode = mode + " - master"

    hookenv.status_set('active', 'Ready ({})'.format(mode))


@when('spark.installed', 'hadoop.ready', 'not.upgrading')
@when_not('yarn.configured')
def switch_to_yarn(hadoop):
    '''
    In case you first change the config and then connect the plugin.
    '''
    mode = hookenv.config()['spark_execution_mode']
    if mode.startswith('yarn'):
        spark = Spark(get_dist_config())
        hookenv.status_set('maintenance', 'Setting up Apache Spark for YARN')
        spark.stop()
        spark.configure_yarn_mode()
        set_state('yarn.configured')
        spark.configure()
        spark.start()
        report_status(spark)


def upgrade_spark():
    version = hookenv.config()['spark_version']
    hookenv.status_set('maintenance', 'Upgrading to {}'.format(version))
    hookenv.log("Upgrading to {}".format(version))
    spark = Spark(get_dist_config())
    spark.switch_version(version)
    hookenv.status_set('maintenance', 'Upgrade complete. You can exit maintainance mode')


@when('spark.started', 'config.changed')
def reconfigure_spark():
    maintainance = hookenv.config()['maintainance_mode']
    if maintainance:
        remove_state('not.upgrading')
        spark = Spark(get_dist_config())
        report_status(spark)
        spark.stop()
        return
    else:
        set_state('not.upgrading')

    mode = hookenv.config()['spark_execution_mode']
    hookenv.status_set('maintenance', 'Configuring Apache Spark')
    spark = Spark(get_dist_config())
    spark.stop()
    if is_state('hadoop.ready') and mode.startswith('yarn') and (not is_state('yarn.configured')):
        # was in a mode other than yarn, going to yarn
        hookenv.status_set('maintenance', 'Setting up Apache Spark for YARN')
        spark.configure_yarn_mode()
        set_state('yarn.configured')

    if is_state('hadoop.ready') and (not mode.startswith('yarn')) and is_state('yarn.configured'):
        # was in a yarn mode and going to another mode
        hookenv.status_set('maintenance', 'Disconnecting Apache Spark from YARN')
        spark.disable_yarn_mode()
        remove_state('yarn.configured')

    spark.configure()
    spark.start()
    report_status(spark)


@when('spark.started', 'yarn.configured', 'not.upgrading')
@when_not('hadoop.ready')
def disable_yarn():
    hookenv.status_set('maintenance', 'Disconnecting Apache Spark from YARN')
    spark = Spark(get_dist_config())
    spark.stop()
    spark.disable_yarn_mode()
    spark.start()
    remove_state('yarn.configured')
    report_status(spark)


@when('spark.installed', 'sparkpeers.joined', 'not.upgrading')
def peers_update(sprkpeer):
    nodes = sprkpeer.get_nodes()
    nodes.append((hookenv.local_unit(), hookenv.unit_private_ip()))
    update_peers_config(nodes)


@when('spark.installed', 'not.upgrading')
@when_not('sparkpeers.joined')
def no_peers_update():
    nodes = [(hookenv.local_unit(), hookenv.unit_private_ip())]
    update_peers_config(nodes)


def update_peers_config(nodes):
    nodes.sort()
    if data_changed('available.peers', nodes):
        spark = Spark(get_dist_config())
        # We need to reconfigure spark only if the master changes or if we
        # are in HA mode and a new potential master is added
        if spark.update_peers(nodes):
            hookenv.status_set('maintenance', 'Updating Apache Spark config')
            spark.stop()
            spark.configure()
            spark.start()
            report_status(spark)


@when('spark.installed', 'zookeeper.ready', 'not.upgrading')
def configure_zookeepers(zk):
    zks = zk.zookeepers()
    if data_changed('available.zookeepers', zks):
        spark = Spark(get_dist_config())
        hookenv.status_set('maintenance', 'Updating Apache Spark HA')
        spark.stop()
        spark.configure_ha(zks)
        spark.configure()
        spark.start()
        set_state('zookeeper.configured')
        report_status(spark)


@when('spark.installed', 'zookeeper.configured', 'not.upgrading')
@when_not('zookeeper.ready')
def disable_zookeepers():
    hookenv.status_set('maintenance', 'Disabling high availability')
    spark = Spark(get_dist_config())
    spark.stop()
    spark.disable_ha()
    spark.configure()
    spark.start()
    remove_state('zookeeper.configured')
    report_status(spark)


@when('spark.started', 'client.joined')
def client_present(client):
    client.set_spark_started()


@when('client.joined')
@when_not('spark.started')
def client_should_stop(client):
    client.clear_spark_started()


@when('benchmark.joined', 'not.upgrading')
def register_benchmarks(benchmark):
    benchmarks = ['sparkpi']
    if hookenv.config('spark_bench_enabled'):
        benchmarks.extend(['logisticregression',
                           'matrixfactorization',
                           'pagerank',
                           'sql',
                           'streaming',
                           'svdplusplus',
                           'svm',
                           'trianglecount'])
    benchmark.register(benchmarks)
