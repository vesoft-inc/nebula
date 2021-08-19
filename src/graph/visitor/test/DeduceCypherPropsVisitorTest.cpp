/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include <memory>
#include <unordered_set>

#include "graph/visitor/DeduceCypherPropsVisitor.h"
#include "graph/visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {

class DeduceCypherPropsVisitorTest : public VisitorTestBase {};

TEST_F(DeduceCypherPropsVisitorTest, Basic) {
  std::unordered_map<std::string, AliasType> aliases{{"v", AliasType::kNode},
                                                     {"e", AliasType::kEdge}};
  {
    // v.prop
    auto expr = laExpr("v", "prop");
    CypherProps props;
    DeduceCypherPropsVisitor visitor(props, aliases);
    expr->accept(&visitor);
    EXPECT_EQ(
        props,
        CypherProps({}, CypherProps::AliasProps{{"v", std::set<std::string>{"prop"}}}, {}, {}));
  }
  {
    // (v.prop + 3) > e.prop1
    auto expr = gtExpr(addExpr(laExpr("v", "prop"), constantExpr(3)), laExpr("e", "prop1"));
    CypherProps props;
    DeduceCypherPropsVisitor visitor(props, aliases);
    expr->accept(&visitor);
    EXPECT_EQ(props,
              CypherProps({},
                          CypherProps::AliasProps{{"v", std::set<std::string>{"prop"}}},
                          CypherProps::AliasProps{{"e", std::set<std::string>{"prop1"}}},
                          {}));
  }
  {
    // id(v) > v.prop
    auto expr = gtExpr(fnExpr("id", {labelExpr("v")}), laExpr("v", "prop"));
    CypherProps props;
    DeduceCypherPropsVisitor visitor(props, aliases);
    expr->accept(&visitor);
    EXPECT_EQ(
        props,
        CypherProps({}, CypherProps::AliasProps{{"v", std::set<std::string>{"prop"}}}, {}, {}));
  }
  {
    // v + e + v.prop
    auto expr = addExpr(addExpr(labelExpr("v"), labelExpr("e")), laExpr("v", "prop"));
    CypherProps props;
    DeduceCypherPropsVisitor visitor(props, aliases);
    expr->accept(&visitor);
    EXPECT_EQ(props,
              CypherProps({},
                          CypherProps::AliasProps{{"v", CypherProps::AllProps()}},
                          CypherProps::AliasProps{{"e", CypherProps::AllProps()}},
                          {}));
  }
}

}  // namespace graph
}  // namespace nebula
