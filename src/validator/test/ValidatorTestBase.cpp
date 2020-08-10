/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "context/QueryContext.h"
#include "context/ValidateContext.h"
#include "parser/GQLParser.h"
#include "planner/Admin.h"
#include "planner/ExecutionPlan.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"
#include "validator/ASTValidator.h"
#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

// Compare the node itself's field in this function
/*static*/ Status ValidatorTestBase::EqSelf(const PlanNode *l, const PlanNode *r) {
    if (l != nullptr && r != nullptr) {
        // continue
    } else if (l == nullptr && r == nullptr) {
        return Status::OK();
    } else {
        return Status::Error("%s.this != %s.this", l->nodeLabel().c_str(), r->nodeLabel().c_str());
    }
    if (l->kind() != r->kind()) {
        return Status::Error(
            "%s.kind_ != %s.kind_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
    }
    if (l->colNamesRef() != r->colNamesRef()) {
        return Status::Error(
            "%s.colNames_ != %s.colNames_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
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
            if (lDC->collectKind() != rDC->collectKind()) {
                return Status::Error(
                    "%s.collectKind_ != %s.collectKind_",
                    l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            return Status::OK();
        }
        case PlanNode::Kind::kGetVertices: {
            const auto *lGV = static_cast<const GetVertices *>(l);
            const auto *rGV = static_cast<const GetVertices *>(r);
            // vertices
            if (lGV->vertices() != rGV->vertices()) {
                return Status::Error(
                    "%s.vertices_ != %s.vertices_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // src
            if (lGV->src() != nullptr && rGV->src() != nullptr) {
                if (*lGV->src() != *rGV->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
                }
            } else if (lGV->src() != rGV->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // props
            if (lGV->props() != rGV->props()) {
                return Status::Error(
                    "%s.props_ != %s.props_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            return Status::OK();
        }
        case PlanNode::Kind::kGetEdges: {
            const auto *lGE = static_cast<const GetEdges *>(l);
            const auto *rGE = static_cast<const GetEdges *>(r);
            // vertices
            if (lGE->edges() != rGE->edges()) {
                return Status::Error(
                    "%s.vertices_ != %s.vertices_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // src
            if (lGE->src() != nullptr && rGE->src() != nullptr) {
                if (*lGE->src() != *rGE->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
                }
            } else if (lGE->src() != rGE->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // dst
            if (lGE->dst() != nullptr && rGE->dst() != nullptr) {
                if (*lGE->dst() != *rGE->dst()) {
                    return Status::Error(
                        "%s.dst_ != %s.dst_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
                }
            } else if (lGE->dst() != rGE->dst()) {
                    return Status::Error(
                        "%s.dst_ != %s.dst_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // ranking
            if (lGE->ranking() != nullptr && rGE->ranking() != nullptr) {
                if (*lGE->ranking() != *lGE->ranking()) {
                    return Status::Error(
                        "%s.ranking_ != %s.ranking_",
                        l->nodeLabel().c_str(), r->nodeLabel().c_str());
                }
            } else if (lGE->ranking() != rGE->ranking()) {
                    return Status::Error(
                        "%s.ranking_ != %s.ranking_",
                        l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // props
            if (lGE->props() != rGE->props()) {
                return Status::Error(
                    "%s.props_ != %s.props_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
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
                                         l->nodeLabel().c_str(),
                                         r->nodeLabel().c_str());
                }
                for (std::size_t i = 0; i < lCols.size(); ++i) {
                    if (*lCols[i] != *rCols[i]) {
                        return Status::Error("%s.columns_ != %s.columns_",
                                             l->nodeLabel().c_str(),
                                             r->nodeLabel().c_str());
                    }
                }
            } else {
                return Status::Error(
                    "%s.columns_ != %s.columns_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
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

    const auto *lBi = dynamic_cast<const BiInputNode *>(l);
    const auto *rBi = dynamic_cast<const BiInputNode *>(r);
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
    std::vector<const char*> kinds(plan.size());
    std::transform(plan.cbegin(), plan.cend(), kinds.begin(), PlanNode::toString);
    os << "[" << folly::join(", ", kinds) << "]";
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

