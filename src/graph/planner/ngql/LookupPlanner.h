/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_NGQL_LOOKUPPLANNER_H_
#define PLANNER_NGQL_LOOKUPPLANNER_H_

#include <memory>
#include <string>

#include "planner/Planner.h"

namespace nebula {

class Expression;
class YieldColumns;

namespace graph {

struct LookupContext;
struct AstContext;

class LookupPlanner final : public Planner {
public:
    static std::unique_ptr<Planner> make();
    static bool match(AstContext* astCtx);

    StatusOr<SubPlan> transform(AstContext* astCtx) override;

private:
    YieldColumns* prepareReturnCols(LookupContext* lookupCtx);
    void appendColumns(LookupContext* lookupCtx, YieldColumns* columns);
    void extractUsedColumns(Expression* filter);
    void addLookupColumns(const std::string& retCol, const std::string& outCol);

    std::vector<std::string> returnCols_;
    std::vector<std::string> colNames_;
};

}   // namespace graph
}   // namespace nebula

#endif   // PLANNER_NGQL_LOOKUPPLANNER_H_
