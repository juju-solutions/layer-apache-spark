from charms.reactive import when, when_not
from charms.reactive import set_state
from charmhelpers.core import hookenv


@when('bootstrapped')
@when_not('spark.installed')
def install_spark():
    from charms.spark import Spark  # installed during bootstrap

    spark = Spark()
    if spark.verify_resources():
        hookenv.status_set('maintenance', 'Installing Apache Spark')
        spark.install()
        set_state('spark.installed')


@when('spark.installed')
@when_not('hadoop.connected')
def blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Hadoop')


@when('spark.installed', 'hadoop.connected')
@when_not('hadoop.yarn.ready', 'hadoop.hdfs.ready')
@when_not('spark.started')
def waiting(*args):
    hookenv.status_set('waiting', 'Waiting for Hadoop to become ready')


@when('spark.installed', 'hadoop.yarn.ready', 'hadoop.hdfs.ready')
def start_spark(*args):
    from charms.spark import Spark  # installed during bootstrap

    hookenv.status_set('maintenance', 'Setting up Apache Spark')
    spark = Spark()
    spark.configure()
    spark.start()
    spark.open_ports()
    set_state('spark.started')
    hookenv.status_set('active', 'Ready')


@when('spark.started')
@when_not('hadoop.yarn.ready', 'hadoop.hdfs.ready')
def stop_spark():
    from charms.spark import Spark  # installed during bootstrap

    hookenv.status_set('maintenance', 'Stopping Apache Spark')
    spark = Spark()
    spark.close_ports()
    spark.stop()
