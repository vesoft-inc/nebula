/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/parts/DropSpaceProcessor.h"
#include "meta/processors/zone/AddZoneProcessor.h"
#include "meta/processors/zone/DropZoneProcessor.h"
#include "meta/processors/zone/GetZoneProcessor.h"
#include "meta/processors/zone/ListZonesProcessor.h"
#include "meta/processors/zone/UpdateZoneProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(GroupAndZoneTest, GroupAndZoneTest) {
  fs::TempDir rootPath("/tmp/GroupZoneTest.XXXXXX");

  // Prepare
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  std::vector<HostAddr> addresses;
  for (int32_t i = 0; i < 13; i++) {
    addresses.emplace_back(std::to_string(i), i);
  }
  TestUtils::registerHB(kv.get(), addresses);
  {
    cpp2::ListHostsReq req;
    req.type_ref() = cpp2::ListHostType::STORAGE;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(13, (*resp.hosts_ref()).size());
    for (auto i = 0; i < 13; i++) {
      ASSERT_EQ(std::to_string(i), (*resp.hosts_ref())[i].get_hostAddr().host);
      ASSERT_EQ(i, (*resp.hosts_ref())[i].get_hostAddr().port);
      ASSERT_EQ(cpp2::HostStatus::ALIVE, (*resp.hosts_ref())[i].get_status());
    }
  }
  // Add Zone
  {{std::vector<HostAddr> nodes;
  for (int32_t i = 0; i < 3; i++) {
    nodes.emplace_back(std::to_string(i), i);
  }
  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_0";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
}
{
  std::vector<HostAddr> nodes;
  for (int32_t i = 3; i < 6; i++) {
    nodes.emplace_back(std::to_string(i), i);
  }
  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_1";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
}
{
  std::vector<HostAddr> nodes;
  for (int32_t i = 6; i < 9; i++) {
    nodes.emplace_back(std::to_string(i), i);
  }
  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_2";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
}
// Host have overlap
{
  std::vector<HostAddr> nodes;
  for (int32_t i = 8; i < 11; i++) {
    nodes.emplace_back(std::to_string(i), i);
  }
  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_3";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
}
}  // namespace meta
// Add Zone with empty node list
{
  std::vector<HostAddr> nodes;
  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_0";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
}
// Add Zone with duplicate node
{
  std::vector<HostAddr> nodes;
  nodes.emplace_back(std::to_string(1), 1);
  nodes.emplace_back(std::to_string(1), 1);

  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_0";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
}
// Add Zone which node not exist
{
  std::vector<HostAddr> nodes = {{"zone_not_exist", 0}};
  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_4";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
}
// Add Zone already existed
{
  std::vector<HostAddr> nodes;
  for (int32_t i = 0; i < 3; i++) {
    nodes.emplace_back(std::to_string(i), i);
  }

  cpp2::AddZoneReq req;
  req.zone_name_ref() = "zone_0";
  req.nodes_ref() = std::move(nodes);
  auto* processor = AddZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
}
// Get Zone
{
  cpp2::GetZoneReq req;
  req.zone_name_ref() = "zone_0";
  auto* processor = GetZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}, {"2", 2}};
  ASSERT_EQ(nodes, *resp.hosts_ref());
}
// Get Zone which is not exist
{
  cpp2::GetZoneReq req;
  req.zone_name_ref() = "zone_not_exist";
  auto* processor = GetZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND, resp.get_code());
}
// List Zones
{
  cpp2::ListZonesReq req;
  auto* processor = ListZonesProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  ASSERT_EQ(3, (*resp.zones_ref()).size());
  ASSERT_EQ("zone_0", (*resp.zones_ref())[0].get_zone_name());
  ASSERT_EQ("zone_1", (*resp.zones_ref())[1].get_zone_name());
  ASSERT_EQ("zone_2", (*resp.zones_ref())[2].get_zone_name());
}
// Add host into zone
{
  cpp2::AddHostIntoZoneReq req;
  req.zone_name_ref() = "zone_0";
  HostAddr node{"12", 12};
  req.node_ref() = std::move(node);
  auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
}
// Add host into zone overlap with another zone
{
  cpp2::AddHostIntoZoneReq req;
  req.zone_name_ref() = "zone_0";
  HostAddr node{"3", 3};
  req.node_ref() = std::move(node);
  auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
}
// Add host into zone which zone is not exist
{
  cpp2::AddHostIntoZoneReq req;
  req.zone_name_ref() = "zone_not_exist";
  HostAddr node{"4", 4};
  req.node_ref() = std::move(node);
  auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND, resp.get_code());
}
// Add host into zone which the node have existed
{
  cpp2::AddHostIntoZoneReq req;
  req.zone_name_ref() = "zone_0";
  HostAddr node{"0", 0};
  req.node_ref() = std::move(node);
  auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
}
// Add host into zone which the node not existed
{
  cpp2::AddHostIntoZoneReq req;
  req.zone_name_ref() = "zone_0";
  HostAddr node{"99", 99};
  req.node_ref() = std::move(node);
  auto* processor = AddHostIntoZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
}
// Drop host from zone which zone is not exist
{
  cpp2::DropHostFromZoneReq req;
  req.zone_name_ref() = "zone_not_exist";
  HostAddr node{"3", 3};
  req.node_ref() = std::move(node);
  auto* processor = DropHostFromZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND, resp.get_code());
}
// Drop host from zone which the node not exist
{
  cpp2::DropHostFromZoneReq req;
  req.zone_name_ref() = "zone_0";
  HostAddr node{"3", 3};
  req.node_ref() = std::move(node);
  auto* processor = DropHostFromZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
}
{
  cpp2::DropHostFromZoneReq req;
  req.zone_name_ref() = "zone_0";
  HostAddr node{"12", 12};
  req.node_ref() = std::move(node);
  auto* processor = DropHostFromZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
}
// Drop Zone
{
  cpp2::DropZoneReq req;
  req.zone_name_ref() = "zone_0";
  auto* processor = DropZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
}
// Drop Zone which is not exist
{
  cpp2::DropZoneReq req;
  req.zone_name_ref() = "zone_0";
  auto* processor = DropZoneProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND, resp.get_code());
}
}  // namespace nebula

TEST(GroupAndZoneTest, DropHostAndZoneTest) {
  fs::TempDir rootPath("/tmp/DropHostAndZoneTest.XXXXXX");

  // Prepare
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  std::vector<HostAddr> addresses;
  for (int32_t i = 0; i < 1; i++) {
    addresses.emplace_back(std::to_string(i), i);
  }
  TestUtils::registerHB(kv.get(), addresses);
  {
    cpp2::ListHostsReq req;
    req.type_ref() = cpp2::ListHostType::STORAGE;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(1, (*resp.hosts_ref()).size());
    for (auto i = 0; i < 1; i++) {
      ASSERT_EQ(std::to_string(i), (*resp.hosts_ref())[i].get_hostAddr().host);
      ASSERT_EQ(i, (*resp.hosts_ref())[i].get_hostAddr().port);
      ASSERT_EQ(cpp2::HostStatus::ALIVE, (*resp.hosts_ref())[i].get_status());
    }
  }

  // Add Zone
  {
    std::vector<HostAddr> nodes;
    nodes.emplace_back("0", 0);
    cpp2::AddZoneReq req;
    req.zone_name_ref() = "zone_0";
    req.nodes_ref() = std::move(nodes);
    auto* processor = AddZoneProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Drop host from zone
  {
    cpp2::DropHostFromZoneReq req;
    req.zone_name_ref() = "zone_0";
    HostAddr node{"0", 0};
    req.node_ref() = std::move(node);
    auto* processor = DropHostFromZoneProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // List Zones
  {
    cpp2::ListZonesReq req;
    auto* processor = ListZonesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, (*resp.zones_ref()).size());
    ASSERT_EQ("zone_0", (*resp.zones_ref())[0].get_zone_name());
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
