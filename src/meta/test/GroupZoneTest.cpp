/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/zoneMan/AddGroupProcessor.h"
#include "meta/processors/zoneMan/DropGroupProcessor.h"
#include "meta/processors/zoneMan/GetGroupProcessor.h"
#include "meta/processors/zoneMan/ListGroupsProcessor.h"
#include "meta/processors/zoneMan/UpdateGroupProcessor.h"
#include "meta/processors/zoneMan/AddZoneProcessor.h"
#include "meta/processors/zoneMan/DropZoneProcessor.h"
#include "meta/processors/zoneMan/GetZoneProcessor.h"
#include "meta/processors/zoneMan/ListZonesProcessor.h"
#include "meta/processors/zoneMan/UpdateZoneProcessor.h"

namespace nebula {
namespace meta {

TEST(GroupAndZoneTest, GroupAndZoneTest) {
    fs::TempDir rootPath("/tmp/GroupZoneTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

    // Add Zone
    {
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 0; i < 3; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_0");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 3; i < 6; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_1");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 6; i < 9; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_2");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        // Host have overlap
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 8; i < 11; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_3");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.code);
        }
    }
    // Add Zone with empty node list
    {
        std::vector<HostAddr> nodes;
        cpp2::AddZoneReq req;
        req.set_zone_name("zone_0");
        req.set_nodes(std::move(nodes));
        auto* processor = AddZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.code);
    }
    // Add Zone with duplicate node
    {
        std::vector<HostAddr> nodes;
        nodes.emplace_back(std::to_string(1), 1);
        nodes.emplace_back(std::to_string(1), 1);

        cpp2::AddZoneReq req;
        req.set_zone_name("zone_0");
        req.set_nodes(std::move(nodes));
        auto* processor = AddZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.code);
    }
    // Add Zone already existed
    {
        std::vector<HostAddr> nodes;
        for (int32_t i = 0; i < 3; i++) {
            nodes.emplace_back(std::to_string(i), i);
        }

        cpp2::AddZoneReq req;
        req.set_zone_name("zone_0");
        req.set_nodes(std::move(nodes));
        auto* processor = AddZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.code);
    }
    // Get Zone
    {
        cpp2::GetZoneReq req;
        req.set_zone_name("zone_0");
        auto* processor = GetZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}, {"2", 2}};
        ASSERT_EQ(nodes, resp.hosts);
    }
    // Get Zone which is not exist
    {
        cpp2::GetZoneReq req;
        req.set_zone_name("zone_not_exist");
        auto* processor = GetZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // List Zones
    {
        cpp2::ListZonesReq req;
        auto* processor = ListZonesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(3, resp.zones.size());
        ASSERT_EQ("zone_0", resp.zones[0].zone_name);
        ASSERT_EQ("zone_1", resp.zones[1].zone_name);
        ASSERT_EQ("zone_2", resp.zones[2].zone_name);
    }
    // Add host into zone
    {
        cpp2::AddHostIntoZoneReq req;
        req.set_zone_name("zone_0");
        HostAddr node{"3", 3};
        req.set_node(std::move(node));
        auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Add host into zone which zone is not exist
    {
        cpp2::AddHostIntoZoneReq req;
        req.set_zone_name("zone_not_exist");
        HostAddr node{"4", 4};
        req.set_node(std::move(node));
        auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Add host into zone which the node have existed
    {
        cpp2::AddHostIntoZoneReq req;
        req.set_zone_name("zone_0");
        HostAddr node{"3", 3};
        req.set_node(std::move(node));
        auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.code);
    }
    // Drop host from zone
    {
        cpp2::DropHostFromZoneReq req;
        req.set_zone_name("zone_0");
        HostAddr node{"3", 3};
        req.set_node(std::move(node));
        auto* processor = DropHostFromZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Drop host from zone which zone is not exist
    {
        cpp2::DropHostFromZoneReq req;
        req.set_zone_name("zone_not_exist");
        HostAddr node{"3", 3};
        req.set_node(std::move(node));
        auto* processor = DropHostFromZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Drop host from zone which the node not exist
    {
        cpp2::DropHostFromZoneReq req;
        req.set_zone_name("zone_0");
        HostAddr node{"3", 3};
        req.set_node(std::move(node));
        auto* processor = DropHostFromZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Add Group
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_0");
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Group already existed
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_1");
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.code);
    }
    // Group already existed although the order is different
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_1");
        std::vector<std::string> zones = {"zone_2", "zone_1", "zone_0"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.code);
    }
    // Add Group with empty zone name list
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_0");
        std::vector<std::string> zones = {};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.code);
    }
    // Add Group with duplicate zone name
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_0");
        std::vector<std::string> zones = {"zone_0", "zone_0", "zone_2"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.code);
    }
    // Add Group name already existed
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_0");
        std::vector<std::string> zones = {"zone_0", "zone_1"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.code);
    }
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_1");
        std::vector<std::string> zones = {"zone_0", "zone_1"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Get Group
    {
        cpp2::GetGroupReq req;
        req.set_group_name("group_0");
        auto* processor = GetGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(3, resp.zone_names.size());
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
        ASSERT_EQ(zones, resp.zone_names);
    }
    // Get Group which is not exist
    {
        cpp2::GetGroupReq req;
        req.set_group_name("group_not_exist");
        auto* processor = GetGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // List Groups
    {
        cpp2::ListGroupsReq req;
        auto* processor = ListGroupsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.groups.size());
        ASSERT_EQ("group_0", resp.groups[0].group_name);
        ASSERT_EQ("group_1", resp.groups[1].group_name);
    }
    // Add zone into group
    {
        cpp2::AddZoneIntoGroupReq req;
        req.set_group_name("group_0");
        req.set_zone_name("zone_3");
        auto* processor = AddZoneIntoGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Add zone into group which group not exist
    {
        cpp2::AddZoneIntoGroupReq req;
        req.set_group_name("group_not_exist");
        req.set_zone_name("zone_0");
        auto* processor = AddZoneIntoGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Add zone into group which zone already exist
    {
        cpp2::AddZoneIntoGroupReq req;
        req.set_group_name("group_0");
        req.set_zone_name("zone_0");
        auto* processor = AddZoneIntoGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.code);
    }
    // Drop zone from group
    {
        cpp2::DropZoneFromGroupReq req;
        req.set_group_name("group_0");
        req.set_zone_name("zone_3");
        auto* processor = DropZoneFromGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Drop zone from group which group not exist
    {
        cpp2::DropZoneFromGroupReq req;
        req.set_group_name("group_not_exist");
        req.set_zone_name("zone_0");
        auto* processor = DropZoneFromGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Drop zone from group which zone not exist
    {
        cpp2::DropZoneFromGroupReq req;
        req.set_group_name("group_0");
        req.set_zone_name("zone_not_exist");
        auto* processor = DropZoneFromGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Drop Group
    {
        cpp2::DropGroupReq req;
        req.set_group_name("group_0");
        auto* processor = DropGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Drop Group which is not exist
    {
        cpp2::DropGroupReq req;
        req.set_group_name("group_0");
        auto* processor = DropGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    // Drop Zone belong to a group
    {
        cpp2::DropZoneReq req;
        req.set_zone_name("zone_0");
        auto* processor = DropZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_DROP, resp.code);
    }
    {
        cpp2::DropGroupReq req;
        req.set_group_name("group_1");
        auto* processor = DropGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Drop Zone
    {
        cpp2::DropZoneReq req;
        req.set_zone_name("zone_0");
        auto* processor = DropZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    // Drop Zone which is not exist
    {
        cpp2::DropZoneReq req;
        req.set_zone_name("zone_0");
        auto* processor = DropZoneProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
