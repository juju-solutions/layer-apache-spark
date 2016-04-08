#!/usr/bin/env python3

import unittest
import amulet


class TestDeploy(unittest.TestCase):
    """
    Trivial deployment test for Apache Spark.
    """
    def setUp(self):
        self.d = amulet.Deployment(series='trusty')
        self.d.add('spark', 'apache-spark')
        self.d.setup(timeout=900)
        self.d.sentry.wait(timeout=1800)

    def test_deploy(self):
        self.d.sentry.wait_for_messages({"spark": "Ready (standalone - master)"})
        spark_unit = self.d.sentry["spark"][0]
        ip = spark_unit.info['public-address']
        (stdout, errcode) = spark_unit.run('su ubuntu -c /home/ubuntu/sparkpi.sh')
        # ensure we comuted pi
        assert '3.14' in stdout
        # we have only one unit that must be the master
        assert 'spark://{}'.format(ip) in stdout


if __name__ == '__main__':
    unittest.main()
