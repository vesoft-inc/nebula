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
#include "planner/Admin.h"
#include "planner/ExecutionPlan.h"
#include "planner/Maintain.h"
#include "planner/Mutate.h"
#include "planner/PlanNode.h"
#include "planner/Query.h"

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

        switch (node->kind()) {
            case PlanNode::Kind::kUnknown:
                ASSERT_TRUE(false) << "Unknown Plan Node.";
            case PlanNode::Kind::kStart: {
                break;
            }
            case PlanNode::Kind::kCreateUser:
            case PlanNode::Kind::kDropUser:
            case PlanNode::Kind::kUpdateUser:
            case PlanNode::Kind::kGrantRole:
            case PlanNode::Kind::kRevokeRole:
            case PlanNode::Kind::kChangePassword:
            case PlanNode::Kind::kListUserRoles:
            case PlanNode::Kind::kListUsers:
            case PlanNode::Kind::kListRoles:
            case PlanNode::Kind::kShowHosts:
            case PlanNode::Kind::kGetNeighbors:
            case PlanNode::Kind::kGetVertices:
            case PlanNode::Kind::kGetEdges:
            case PlanNode::Kind::kIndexScan:
            case PlanNode::Kind::kFilter:
            case PlanNode::Kind::kProject:
            case PlanNode::Kind::kSort:
            case PlanNode::Kind::kLimit:
            case PlanNode::Kind::kAggregate:
            case PlanNode::Kind::kSwitchSpace:
            case PlanNode::Kind::kPassThrough:
            case PlanNode::Kind::kDedup:
            case PlanNode::Kind::kDataCollect:
            case PlanNode::Kind::kCreateSpace:
            case PlanNode::Kind::kCreateTag:
            case PlanNode::Kind::kCreateEdge:
            case PlanNode::Kind::kDescSpace:
            case PlanNode::Kind::kDescTag:
            case PlanNode::Kind::kDescEdge:
            case PlanNode::Kind::kInsertVertices:
            case PlanNode::Kind::kInsertEdges:
            case PlanNode::Kind::kShowCreateSpace:
            case PlanNode::Kind::kShowCreateTag:
            case PlanNode::Kind::kShowCreateEdge:
            case PlanNode::Kind::kDropSpace:
            case PlanNode::Kind::kDropTag:
            case PlanNode::Kind::kDropEdge:
            case PlanNode::Kind::kShowSpaces:
            case PlanNode::Kind::kShowTags:
            case PlanNode::Kind::kShowEdges:
            case PlanNode::Kind::kCreateSnapshot:
            case PlanNode::Kind::kDropSnapshot:
            case PlanNode::Kind::kShowSnapshots:
            case PlanNode::Kind::kDataJoin:
            case PlanNode::Kind::kDeleteVertices:
            case PlanNode::Kind::kDeleteEdges:
            case PlanNode::Kind::kUpdateVertex:
            case PlanNode::Kind::kUpdateEdge: {
                auto *current = static_cast<const SingleDependencyNode *>(node);
                queue.emplace(current->dep());
                break;
            }
            case PlanNode::Kind::kUnion:
            case PlanNode::Kind::kIntersect:
            case PlanNode::Kind::kMinus: {
                auto *current = static_cast<const BiInputNode *>(node);
                queue.emplace(current->left());
                queue.emplace(current->right());
                break;
            }
            case PlanNode::Kind::kSelect: {
                auto *current = static_cast<const Select *>(node);
                queue.emplace(current->dep());
                queue.emplace(current->then());
                if (current->otherwise() != nullptr) {
                    queue.emplace(current->otherwise());
                }
                break;
            }
            case PlanNode::Kind::kLoop: {
                auto *current = static_cast<const Loop *>(node);
                queue.emplace(current->dep());
                queue.emplace(current->body());
                break;
            }
            default:
                LOG(FATAL) << "Unknown PlanNode: " << static_cast<int64_t>(node->kind());
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
            // src
            if (lGV->src() != nullptr && rGV->src() != nullptr) {
                // TODO(shylock) check more about the anno variable
                if (lGV->src()->kind() != rGV->src()->kind()) {
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
            // src
            if (lGE->src() != nullptr && rGE->src() != nullptr) {
                // TODO(shylock) check more about the anno variable
                if (lGE->src()->kind() != rGE->src()->kind()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
                }
            } else if (lGE->src() != rGE->src()) {
                    return Status::Error(
                        "%s.src_ != %s.src_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // dst
            if (lGE->dst() != nullptr && rGE->dst() != nullptr) {
                if (lGE->dst()->kind() != rGE->dst()->kind()) {
                    return Status::Error(
                        "%s.dst_ != %s.dst_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
                }
            } else if (lGE->dst() != rGE->dst()) {
                    return Status::Error(
                        "%s.dst_ != %s.dst_", l->nodeLabel().c_str(), r->nodeLabel().c_str());
            }
            // ranking
            if (lGE->ranking() != nullptr && rGE->ranking() != nullptr) {
                if (lGE->ranking()->kind() != lGE->ranking()->kind()) {
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
