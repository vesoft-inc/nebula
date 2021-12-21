# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from tests.common.nebula_test_suite import NebulaTestSuite


class TestZone(NebulaTestSuite):
    def test_zone(self):
        pass
        # resp = self.client.execute('SHOW HOSTS')
        # self.check_resp_succeeded(resp)
        # assert not resp.is_empty()
        # storage_port = resp.row_values(0)[1].as_int()

        # # Get Zone
        # resp = self.client.execute('DESC ZONE default_zone_127.0.0.1_' + str(storage_port))
        # self.check_resp_succeeded(resp)

        # resp = self.client.execute('DESCRIBE ZONE zone_0')
        # self.check_resp_succeeded(resp)

        # # Get Zone which is not exist
        # resp = self.client.execute('DESC ZONE zone_not_exist')
        # self.check_resp_failed(resp)

        # resp = self.client.execute('DESCRIBE ZONE zone_not_exist')
        # self.check_resp_failed(resp)

        # # SHOW Zones
        # resp = self.client.execute('SHOW ZONES')
        # self.check_resp_succeeded(resp)

        # # Drop Zone
        # resp = self.client.execute('DROP ZONE zone_0')
        # self.check_resp_succeeded(resp)

        # # Drop Zone which is not exist
        # resp = self.client.execute('DROP ZONE zone_0')
        # self.check_resp_failed(resp)
