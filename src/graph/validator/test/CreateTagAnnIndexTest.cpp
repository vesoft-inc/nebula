/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/Admin.h"
#include "graph/planner/plan/Maintain.h"
#include "graph/validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;

class CreateTagAnnIndexTest : public ValidatorTestBase {};

TEST_F(CreateTagAnnIndexTest, BasicCreateTagAnnIndex) {
  // SubmitJob -> CreateTagAnnIndex -> Start
  std::vector<PlanNode::Kind> expected = {PK::kSubmitJob, PK::kCreateTagAnnIndex, PK::kStart};

  ASSERT_TRUE(checkResult(
      "CREATE TAG ANNINDEX person_ann_idx ON person::(embedding) {ANNINDEX_TYPE:\"HNSW\", DIM:128, "
      "METRIC_TYPE:\"l2\", MAXDEGREE:16, EFCONSTRUCTION:200, MAXELEMENTS:1000}; "
      "",
      expected));
  expected = {PK::kSubmitJob, PK::kCreateTagAnnIndex, PK::kStart};

  ASSERT_TRUE(
      checkResult("CREATE TAG ANNINDEX IF NOT EXISTS teacher_student_idx ON "
                  "teacher&student::(embedding) {ANNINDEX_TYPE:\"HNSW\", DIM:128, "
                  "METRIC_TYPE:\"l2\", MAXDEGREE:16, EFCONSTRUCTION:200, MAXELEMENTS:1000};",
                  expected));
  expected = {PK::kSubmitJob, PK::kCreateTagAnnIndex, PK::kStart};

  auto status = validate(
      "CREATE TAG ANNINDEX person_ann_idx ON person::(embedding) "
      "{ANNINDEX_TYPE:\"IVF\", DIM:128, METRIC_TYPE:\"l2\", NLIST:8, TRAINSIZE:3};");

  ASSERT_TRUE(status.ok());
  auto qctx = status.value();
  ASSERT_NE(qctx, nullptr);

  auto plan = qctx->plan();
  ASSERT_NE(plan, nullptr);

  auto root = plan->root();
  ASSERT_NE(root, nullptr);

  ASSERT_EQ(root->kind(), PK::kSubmitJob);

  auto submitJobNode = static_cast<const SubmitJob*>(root);
  ASSERT_NE(submitJobNode->dep(), nullptr);
  ASSERT_EQ(submitJobNode->dep()->kind(), PK::kCreateTagAnnIndex);

  auto jobType = submitJobNode->jobType();
  ASSERT_EQ(jobType, meta::cpp2::JobType::BUILD_TAG_VECTOR_INDEX);

  auto createNode = static_cast<const CreateTagAnnIndex*>(submitJobNode->dep());
  ASSERT_NE(createNode->dep(), nullptr);
  ASSERT_EQ(createNode->dep()->kind(), PK::kStart);

  ASSERT_EQ(createNode->getIndexName(), "person_ann_idx");
  auto tagNames = createNode->getSchemaNames();
  ASSERT_EQ(tagNames.size(), 1);
  ASSERT_EQ(tagNames[0], "person");
}

}  // namespace graph
}  // namespace nebula
