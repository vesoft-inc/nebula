/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "MockSchemaManager.h"
#include "common/base/Base.h"

namespace nebula {
namespace graph {
class MockSchemaManagerTest : public ::testing::Test {
 public:
  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(MockSchemaManagerTest, all) {
  MockSchemaManager mock;
  mock.init();
  auto spaceRet = mock.toGraphSpaceID("test_space");
  ASSERT_TRUE(spaceRet.ok());
  auto spaceId = spaceRet.value();
  ASSERT_EQ(1, spaceId);

  auto tagRet = mock.toTagID(spaceId, "person");
  ASSERT_TRUE(tagRet.ok()) << tagRet.status();
  auto tagId = tagRet.value();
  ASSERT_EQ(2, tagRet.value());

  auto edgeRet = mock.toEdgeType(spaceId, "like");
  ASSERT_TRUE(edgeRet.ok());
  auto edgeType = edgeRet.value();
  ASSERT_EQ(3, edgeType);

  auto tagNameRet = mock.toTagName(spaceId, tagId);
  ASSERT_TRUE(tagNameRet.ok());
  ASSERT_EQ("person", tagNameRet.value());

  auto edgeNameRet = mock.toEdgeName(spaceId, edgeType);
  ASSERT_TRUE(edgeNameRet.ok());
  ASSERT_EQ("like", edgeNameRet.value());

  auto tagSchema = mock.getTagSchema(spaceId, tagId);
  ASSERT_TRUE(tagSchema != nullptr);

  auto edgeSchema = mock.getEdgeSchema(spaceId, edgeType);
  ASSERT_TRUE(edgeSchema != nullptr);
}
}  // namespace graph
}  // namespace nebula
