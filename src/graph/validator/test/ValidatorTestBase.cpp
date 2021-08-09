/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"

#include "common/base/Base.h"
#include "context/QueryContext.h"
#include "context/ValidateContext.h"
#include "parser/GQLParser.h"
#include "planner/plan/Admin.h"
#include "planner/plan/ExecutionPlan.h"
#include "planner/plan/Maintain.h"
#include "planner/plan/Mutate.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"
#include "util/Utils.h"

namespace nebula {
namespace graph {

// static
void ValidatorTestBase::bfsTraverse(const PlanNode *root, std::vector<PlanNode::Kind> &result) {
    std::queue<const PlanNode *> queue;
    std::unordered_set<int64_t> visited;
    queue.emplace(root);

    while (!queue.empty()) {
        auto node = queue.front();
        VLOG(1) << "node kind: " << node->kind();
        queue.pop();
        if (visited.find(node->id()) != visited.end()) {
            continue;
        }
        visited.emplace(node->id());
        result.emplace_back(node->kind());

        if (node->kind() == PlanNode::Kind::kUnknown) {
            ASSERT_TRUE(false) << "Unknown Plan Node.";
        }

        for (size_t i = 0; i < node->numDeps(); ++i) {
            queue.emplace(node->dep(i));
        }

        if (node->kind() == PlanNode::Kind::kSelect) {
            auto *current = static_cast<const Select *>(node);
            queue.emplace(current->then());
            if (current->otherwise() != nullptr) {
                queue.emplace(current->otherwise());
            }
        } else if (node->kind() == PlanNode::Kind::kLoop) {
            auto *current = static_cast<const Loop *>(node);
            queue.emplace(current->body());
        }
    }
}

// Compare the node itself's field in this function
// static
Status ValidatorTestBase::EqSelf(const PlanNode *l, const PlanNode *r) {
    if (l != nullptr && r != nullptr) {
        // continue
    } else if (l == nullptr && r == nullptr) {
        return Status::OK();
    } else {
        return Status::Error("%s.this != %s.this", l->outputVar().c_str(), r->outputVar().c_str());
    }
    if (l->kind() != r->kind()) {
        return Status::Error(
            "%s.kind_ != %s.kind_", l->outputVar().c_str(), r->outputVar().c_str());
    }
    // TODO(shylock) col names in GetVertices generate by unordered container
    // So can't check now
    if ((l->colNames() != r->colNames()) && l->kind() != PlanNode::Kind::kGetVertices) {
        return Status::Error(
            "%s.colNames_ != %s.colNames_", l->outputVar().c_str(), r->outputVar().c_str());
    }
    switch (l->kind()) {
        case PlanNode::Kind::kUnknown:
            return Status::Error("Unknown node");
        case PlanNode::Kind::kStart:
        case PlanNode::Kind::kDedup:
            return Status::OK();
        case PlanNode::Kind::kDataCollect: {
            const auto *lDC = static_cast<const DataCollect*>(l);
            const auto *rDC = static_cast<const DataCollect*>(r);
            if (lDC->kind() != rDC->kind()) {
                return Status::Error(
                    "%s.collectKind_ != %s.collectKind_",
                    l->outputVar().c_str(), r->outputVar().c_str());
            }
            return Status::OK();
        }
        case PlanNode::Kind::kGetVertices: {
            const auto *lGV = static_cast<const GetVertices *>(l);
            const auto *rGV = static_cast<const GetVertices *>(r);
            // src
            if (lGV->src() != nullptr && rGV->src() != nullptr) {
                // TODO(shylock) check more about the anno variable
                if (lGV->src()->kind() != rGV->src()->kind()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->outputVar().c_str(), r->outputVar().c_str());
                }
            } else if (lGV->src() != rGV->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->outputVar().c_str(), r->outputVar().c_str());
            }
            // props
            // TODO(shylock) props in GetVertices collect from yield generate by unordered container
            // So can't check now
            return Status::OK();
        }
        case PlanNode::Kind::kGetEdges: {
            const auto *lGE = static_cast<const GetEdges *>(l);
            const auto *rGE = static_cast<const GetEdges *>(r);
            // src
            if (lGE->src() != nullptr && rGE->src() != nullptr) {
                // TODO(shylock) check more about the anno variable
                if (lGE->src()->kind() != rGE->src()->kind()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->outputVar().c_str(), r->outputVar().c_str());
                }
            } else if (lGE->src() != rGE->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->outputVar().c_str(), r->outputVar().c_str());
            }
            // dst
            if (lGE->dst() != nullptr && rGE->dst() != nullptr) {
                if (lGE->dst()->kind() != rGE->dst()->kind()) {
                    return Status::Error(
                        "%s.dst_ != %s.dst_", l->outputVar().c_str(), r->outputVar().c_str());
                }
            } else if (lGE->dst() != rGE->dst()) {
                    return Status::Error(
                        "%s.dst_ != %s.dst_", l->outputVar().c_str(), r->outputVar().c_str());
            }
            // ranking
            if (lGE->ranking() != nullptr && rGE->ranking() != nullptr) {
                if (lGE->ranking()->kind() != lGE->ranking()->kind()) {
                    return Status::Error(
                        "%s.ranking_ != %s.ranking_",
                        l->outputVar().c_str(), r->outputVar().c_str());
                }
            } else if (lGE->ranking() != rGE->ranking()) {
                    return Status::Error(
                        "%s.ranking_ != %s.ranking_",
                        l->outputVar().c_str(), r->outputVar().c_str());
            }
            // props
            if (*lGE->props() != *rGE->props()) {
                return Status::Error(
                    "%s.props_ != %s.props_", l->outputVar().c_str(), r->outputVar().c_str());
            }
            return Status::OK();
        }
        case PlanNode::Kind::kProject: {
            const auto *lp = static_cast<const Project *>(l);
            const auto *rp = static_cast<const Project *>(r);
            if (lp->columns() == nullptr && rp->columns() == nullptr) {
            } else if (lp->columns() != nullptr && rp->columns() != nullptr) {
                auto lCols = lp->columns()->columns();
                auto rCols = rp->columns()->columns();
                if (lCols.size() != rCols.size()) {
                    return Status::Error("%s.columns_ != %s.columns_",
                                         l->outputVar().c_str(),
                                         r->outputVar().c_str());
                }
                for (std::size_t i = 0; i < lCols.size(); ++i) {
                    if (*lCols[i] != *rCols[i]) {
                        return Status::Error("%s.columns_ != %s.columns_",
                                             l->outputVar().c_str(),
                                             r->outputVar().c_str());
                    }
                }
            } else {
                return Status::Error(
                    "%s.columns_ != %s.columns_", l->outputVar().c_str(), r->outputVar().c_str());
            }
            return Status::OK();
        }
        case PlanNode::Kind::kFilter: {
            // TODO
            return Status::OK();
        }
        default:
            LOG(FATAL) << "Unknow plan node kind " << static_cast<int>(l->kind());
    }
}

// only traversal the plan by inputs, only `select` or `loop` make ring
// So it's a DAG
// There are some node visited multiple times but it doesn't matter
// For Example:
//      A
//    |  |
//   V   V
//   B   C
//   |  |
//   V V
//    D
//    |
//   V
//   E
// this will traversal sub-tree [D->E] twice but not matter the Equal result
// TODO(shylock) maybe need check the toplogy of `Select` and `Loop`
/*static*/ Status ValidatorTestBase::Eq(const PlanNode *l, const PlanNode *r) {
    auto result = EqSelf(l, r);
    if (!result.ok()) {
        return result;
    }

    const auto *lSingle = dynamic_cast<const SingleDependencyNode *>(l);
    const auto *rSingle = dynamic_cast<const SingleDependencyNode *>(r);
    if (lSingle != nullptr) {
        const auto *lDep = lSingle->dep();
        const auto *rDep = CHECK_NOTNULL(rSingle)->dep();
        return Eq(lDep, rDep);
    }

    const auto *lBi = dynamic_cast<const BinaryInputNode *>(l);
    const auto *rBi = dynamic_cast<const BinaryInputNode *>(r);
    if (lBi != nullptr) {
        const auto *llInput = lBi->left();
        const auto *rlInput = CHECK_NOTNULL(rBi)->left();
        result = Eq(llInput, rlInput);
        if (!result.ok()) {
            return result;
        }

        const auto *lrInput = lBi->right();
        const auto *rrInput = rBi->right();
        result = Eq(lrInput, rrInput);
        return result;
    }

    return result;
}

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan) {
    auto printPNKind = [](auto k) { return PlanNode::toString(k); };
    os << "[" << util::join(plan, printPNKind, ", ") << "]";
    return os;
}

}   // namespace graph
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
