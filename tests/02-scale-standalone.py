#!/usr/bin/env python3

import unittest
import amulet
import time


class TestScaleStandalone(unittest.TestCase):
    """
    Test scaling of Apache Spark in standalone mode.
    """
    def setUp(self):
        self.d = amulet.Deployment(series='trusty')
        self.d.add('sparkscale', 'apache-spark', units=3)
        self.d.setup(timeout=1800)
        self.d.sentry.wait(timeout=1800)

    def test_scaleup(self):
        """
        Wait for all three spark units to agree on a master.
        Remove the master.
        Check that all units agree on the same new master.
        """
        print("Waiting for units to become ready.")
        self.d.sentry.wait_for_messages({"sparkscale": ["Ready (standalone - master)",
                                                        "Ready (standalone)",
                                                        "Ready (standalone)"]}, timeout=900)

        print("Waiting for units to agree on master.")
        time.sleep(60)

        spark0_unit = self.d.sentry['sparkscale'][0]
        spark1_unit = self.d.sentry['sparkscale'][1]
        spark2_unit = self.d.sentry['sparkscale'][2]
        ip1 = spark1_unit.info['public-address']
        ip2 = spark2_unit.info['public-address']
        (stdout0, errcode0) = spark0_unit.run('grep MASTER /etc/environment')
        (stdout1, errcode1) = spark1_unit.run('grep MASTER /etc/environment')
        (stdout2, errcode2) = spark2_unit.run('grep MASTER /etc/environment')
        # ensure units agree on the master
        assert stdout0 == stdout2
        assert stdout1 == stdout2

        master_name = ''
        for unit in self.d.sentry['sparkscale']:
            if unit.info['public-address'] in stdout0:
                master_name = unit.info['unit_name']
                print("Killin master {}".format(master_name))
                self.d.remove_unit(master_name)
                break

        print("Waiting for the cluster to select a new master.")
        time.sleep(60)
        self.d.sentry.wait_for_messages({"sparkscale": ["Ready (standalone - master)",
                                                        "Ready (standalone)"]}, timeout=900)

        spark1_unit = self.d.sentry['sparkscale'][0]
        spark2_unit = self.d.sentry['sparkscale'][1]
        ip1 = spark1_unit.info['public-address']
        ip2 = spark2_unit.info['public-address']
        (stdout1, errcode1) = spark1_unit.run('grep MASTER /etc/environment')
        (stdout2, errcode2) = spark2_unit.run('grep MASTER /etc/environment')
        # ensure units agree on the master
        assert stdout1 == stdout2
        assert ip1 in stdout1 or ip2 in stdout2


if __name__ == '__main__':
    unittest.main()
