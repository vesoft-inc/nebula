# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


from tests.common.nebula_test_suite import NebulaTestSuite


class TestZone(NebulaTestSuite):
    def test_zone(self):
        resp = self.client.execute('SHOW HOSTS')
        self.check_resp_succeeded(resp)
        assert not resp.is_empty()
        storage_port = resp.row_values(0)[1].as_int()
        # Add Zone
        resp = self.client.execute('ADD ZONE zone_0 127.0.0.1:' + str(storage_port))
        self.check_resp_succeeded(resp)

        # Get Zone
        resp = self.client.execute('DESC ZONE zone_0')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESCRIBE ZONE zone_0')
        self.check_resp_succeeded(resp)

        # Get Zone which is not exist
        resp = self.client.execute('DESC ZONE zone_not_exist')
        self.check_resp_failed(resp)

        resp = self.client.execute('DESCRIBE ZONE zone_not_exist')
        self.check_resp_failed(resp)

        # SHOW Zones
        resp = self.client.execute('SHOW ZONES')
        self.check_resp_succeeded(resp)

        # Add Group
        resp = self.client.execute('ADD GROUP group_0 zone_0')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('ADD GROUP default zone_0')
        self.check_resp_failed(resp)

        # Get Group
        resp = self.client.execute('DESC GROUP group_0')
        self.check_resp_succeeded(resp)

        resp = self.client.execute('DESCRIBE GROUP group_0')
        self.check_resp_succeeded(resp)

        # Get Group which is not exist
        resp = self.client.execute('DESC GROUP group_not_exist')
        self.check_resp_failed(resp)

        resp = self.client.execute('DESCRIBE GROUP group_not_exist')
        self.check_resp_failed(resp)

        # SHOW Groups
        resp = self.client.execute('SHOW GROUPS')
        self.check_resp_succeeded(resp)

        # Drop Group
        resp = self.client.execute('DROP GROUP group_0')
        self.check_resp_succeeded(resp)

        # Drop Group which is not exist
        resp = self.client.execute('DROP GROUP group_0')
        self.check_resp_failed(resp)

        # Drop Zone
        resp = self.client.execute('DROP ZONE zone_0')
        self.check_resp_succeeded(resp)

        # Drop Zone which is not exist
        resp = self.client.execute('DROP ZONE zone_0')
        self.check_resp_failed(resp)
