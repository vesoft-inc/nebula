/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "executor/algo/ConjunctPathExecutor.h"

#include "planner/plan/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> ConjunctPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* conjunct = asNode<ConjunctPath>(node());
    switch (conjunct->pathKind()) {
        case ConjunctPath::PathKind::kBiBFS:
            return bfsShortestPath();
        case ConjunctPath::PathKind::kAllPaths:
            return allPaths();
        case ConjunctPath::PathKind::kFloyd:
            return floydShortestPath();
        default:
            LOG(FATAL) << "Not implement.";
    }
}

folly::Future<Status> ConjunctPathExecutor::bfsShortestPath() {
    auto* conjunct = asNode<ConjunctPath>(node());
    auto lIter = ectx_->getResult(conjunct->leftInputVar()).iter();
    const auto& rHist = ectx_->getHistory(conjunct->rightInputVar());
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "left input: " << conjunct->leftInputVar()
            << " right input: " << conjunct->rightInputVar();
    DCHECK(!!lIter);

    auto steps = conjunct->steps();
    count_++;

    DataSet ds;
    ds.colNames = conjunct->colNames();

    VLOG(1) << "forward, size: " << forward_.size();
    VLOG(1) << "backward, size: " << backward_.size();
    forward_.emplace_back();
    for (; lIter->valid(); lIter->next()) {
        auto& dst = lIter->getColumn(kVid);
        auto& edge = lIter->getColumn(kEdgeStr);
        VLOG(1) << "dst: " << dst << " edge: " << edge;
        if (!edge.isEdge()) {
            forward_.back().emplace(Value(dst), nullptr);
        } else {
            forward_.back().emplace(Value(dst), &edge.getEdge());
        }
    }

    bool isLatest = false;
    if (rHist.size() >= 2) {
        auto previous = rHist[rHist.size() - 2].iter();
        VLOG(1) << "Find odd length path.";
        auto rows = findBfsShortestPath(previous.get(), isLatest, forward_.back());
        if (!rows.empty()) {
            VLOG(1) << "Meet odd length path.";
            ds.rows = std::move(rows);
            return finish(ResultBuilder().value(Value(std::move(ds))).finish());
        }
    }

    if (count_ * 2 <= steps) {
        auto latest = rHist.back().iter();
        isLatest = true;
        backward_.emplace_back();
        VLOG(1) << "Find even length path.";
        auto rows = findBfsShortestPath(latest.get(), isLatest, forward_.back());
        if (!rows.empty()) {
            VLOG(1) << "Meet even length path.";
            ds.rows = std::move(rows);
        }
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

std::vector<Row> ConjunctPathExecutor::findBfsShortestPath(
    Iterator* iter,
    bool isLatest,
    std::unordered_multimap<Value, const Edge*>& table) {
    std::unordered_set<Value> meets;
    for (; iter->valid(); iter->next()) {
        auto& dst = iter->getColumn(kVid);
        if (isLatest) {
            auto& edge = iter->getColumn(kEdgeStr);
            VLOG(1) << "dst: " << dst << " edge: " << edge;
            if (!edge.isEdge()) {
                backward_.back().emplace(dst, nullptr);
            } else {
                backward_.back().emplace(dst, &edge.getEdge());
            }
        }
        if (table.find(dst) != table.end()) {
            meets.emplace(dst);
        }
    }

    std::vector<Row> rows;
    if (!meets.empty()) {
        VLOG(1) << "Build forward, size: " << forward_.size();
        auto forwardPath = buildBfsInterimPath(meets, forward_);
        VLOG(1) << "Build backward, size: " << backward_.size();
        auto backwardPath = buildBfsInterimPath(meets, backward_);
        for (auto& p : forwardPath) {
            auto range = backwardPath.equal_range(p.first);
            for (auto& i = range.first; i != range.second; ++i) {
                Path result = p.second;
                result.reverse();
                VLOG(1) << "Forward path: " << result;
                VLOG(1) << "Backward path: " << i->second;
                result.append(i->second);
                Row row;
                row.emplace_back(std::move(result));
                rows.emplace_back(std::move(row));
            }
        }
    }
    return rows;
}

std::unordered_multimap<Value, Path> ConjunctPathExecutor::buildBfsInterimPath(
    std::unordered_set<Value>& meets,
    std::vector<std::unordered_multimap<Value, const Edge*>>& hists) {
    std::unordered_multimap<Value, Path> results;
    for (auto& v : meets) {
        VLOG(1) << "Meet at: " << v;
        Path start;
        start.src = Vertex(v, {});
        if (hists.empty()) {
            // Happens at one step path situation when meet at starts
            VLOG(1) << "Start: " << start;
            results.emplace(v, std::move(start));
            continue;
        }
        std::vector<Path> interimPaths = {std::move(start)};
        for (auto hist = hists.rbegin(); hist < hists.rend(); ++hist) {
            std::vector<Path> tmp;
            for (auto& interimPath : interimPaths) {
                Value id;
                if (interimPath.steps.empty()) {
                    id = interimPath.src.vid;
                } else {
                    id = interimPath.steps.back().dst.vid;
                }
                auto edges = hist->equal_range(id);
                for (auto i = edges.first; i != edges.second; ++i) {
                    Path p = interimPath;
                    if (i->second != nullptr) {
                        auto& edge = *(i->second);
                        VLOG(1) << "Edge: " << edge;
                        VLOG(1) << "Interim path: " << interimPath;
                        p.steps.emplace_back(
                            Step(Vertex(edge.src, {}), -edge.type, edge.name, edge.ranking, {}));
                        VLOG(1) << "New semi path: " << p;
                    }
                    if (hist == (hists.rend() - 1)) {
                        VLOG(1) << "emplace result: " << p.src.vid;
                        results.emplace(p.src.vid, std::move(p));
                    } else {
                        tmp.emplace_back(std::move(p));
                    }
                }   // `edge'
            }       // `interimPath'
            if (hist != (hists.rend() - 1)) {
                interimPaths = std::move(tmp);
            }
        }   // `hist'
    }       // `v'
    return results;
}


folly::Future<Status> ConjunctPathExecutor::floydShortestPath() {
    auto* conjunct = asNode<ConjunctPath>(node());
    conditionalVar_ = conjunct->conditionalVar();
    auto lIter = ectx_->getResult(conjunct->leftInputVar()).iter();
    const auto& rHist = ectx_->getHistory(conjunct->rightInputVar());
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "left input: " << conjunct->leftInputVar()
            << " right input: " << conjunct->rightInputVar();
    DCHECK(!!lIter);
    auto steps = conjunct->steps();
    count_++;

    DataSet ds;
    ds.colNames = conjunct->colNames();

    CostPathsValMap forwardCostPathMap;
    for (; lIter->valid(); lIter->next()) {
        auto& dst = lIter->getColumn(kDst);
        auto& src = lIter->getColumn(kSrc);
        auto cost = lIter->getColumn(kCostStr);

        auto& pathList = lIter->getColumn(kPathStr);
        if (!pathList.isList()) {
            continue;
        }
        auto& srcPaths = forwardCostPathMap[dst];
        srcPaths.emplace(src, CostPaths(cost, pathList.getList()));
    }

    if (rHist.size() >= 2) {
        auto previous = rHist[rHist.size() - 2].iter();
        VLOG(1) << "Find odd length path.";
        findPath(previous.get(), forwardCostPathMap, ds);
    }

    if (count_ * 2 <= steps) {
        VLOG(1) << "Find even length path.";
        auto latest = rHist.back().iter();
        findPath(latest.get(), forwardCostPathMap, ds);
    }

    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

Status ConjunctPathExecutor::conjunctPath(const List& forwardPaths,
                                          const List& backwardPaths,
                                          Value& cost,
                                          DataSet& ds) {
    for (auto& i : forwardPaths.values) {
        if (!i.isPath()) {
            return Status::Error("Forward Path Type Error");
        }
        for (auto& j : backwardPaths.values) {
            if (!j.isPath()) {
                return Status::Error("Backward Path Type Error");
            }
            Row row;
            auto forward = i.getPath();
            auto backward = j.getPath();
            VLOG(1) << "Forward path:" << forward;
            VLOG(1) << "Backward path:" << backward;
            backward.reverse();
            VLOG(1) << "Backward reverse path:" << backward;
            forward.append(std::move(backward));
            VLOG(1) << "Found path: " << forward;
            row.values.emplace_back(std::move(forward));
            row.values.emplace_back(cost);
            ds.rows.emplace_back(std::move(row));
        }
    }
    return Status::OK();
}

bool ConjunctPathExecutor::findPath(Iterator* backwardPathIter,
                                    CostPathsValMap& forwardPathTable,
                                    DataSet& ds) {
    bool found = false;
    for (; backwardPathIter->valid(); backwardPathIter->next()) {
        auto& dst = backwardPathIter->getColumn(kDst);
        auto& endVid = backwardPathIter->getColumn(kSrc);
        auto cost = backwardPathIter->getColumn(kCostStr);
        VLOG(1) << "Backward dst: " << dst;
        auto& pathList = backwardPathIter->getColumn(kPathStr);
        if (!pathList.isList()) {
            continue;
        }
        auto forwardPaths = forwardPathTable.find(dst);
        if (forwardPaths == forwardPathTable.end()) {
            continue;
        }
        for (auto& srcPaths : forwardPaths->second) {
            auto& startVid = srcPaths.first;
            if (startVid == endVid) {
                delPathFromConditionalVar(startVid, endVid);
                continue;
            }
            auto totalCost = cost + srcPaths.second.cost_;
            if (historyCostMap_.find(startVid) != historyCostMap_.end() &&
                historyCostMap_[startVid].find(endVid) != historyCostMap_[startVid].end() &&
                historyCostMap_[startVid][endVid] < totalCost) {
                continue;
            }
            // update history cost
            auto& hist = historyCostMap_[startVid];
            hist[endVid] = totalCost;
            delPathFromConditionalVar(startVid, endVid);
            conjunctPath(srcPaths.second.paths_, pathList.getList(), totalCost, ds);
            found = true;
        }
    }
    return found;
}

void ConjunctPathExecutor::delPathFromConditionalVar(const Value& start, const Value& end) {
    auto iter = qctx_->ectx()->getResult(conditionalVar_).iter();

    while (iter->valid()) {
        auto startVid = iter->getColumn(0);
        auto endVid = iter->getColumn(4);
        if (startVid == endVid || (startVid == start && endVid == end)) {
            iter->unstableErase();
            VLOG(1) << "Path src: " << start << " dst: " << end;
            VLOG(1) << "Delete CartesianProduct src: "<< startVid << " dstVid: " << endVid;
        } else {
            iter->next();
        }
    }
}

folly::Future<Status> ConjunctPathExecutor::allPaths() {
    auto* conjunct = asNode<ConjunctPath>(node());
    noLoop_ = conjunct->noLoop();
    auto lIter = ectx_->getResult(conjunct->leftInputVar()).iter();
    const auto& rHist = ectx_->getHistory(conjunct->rightInputVar());
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "left input: " << conjunct->leftInputVar()
            << " right input: " << conjunct->rightInputVar();
    VLOG(1) << "right hist size: " << rHist.size();
    DCHECK(!!lIter);
    auto steps = conjunct->steps();
    count_++;

    DataSet ds;
    ds.colNames = conjunct->colNames();

    std::unordered_map<Value, const List&> table;
    for (; lIter->valid(); lIter->next()) {
        auto& dst = lIter->getColumn(kVid);
        auto& path = lIter->getColumn(kPathStr);
        if (path.isList()) {
            VLOG(1) << "Forward dst: " << dst;
            table.emplace(dst, path.getList());
        }
    }

    if (rHist.size() >= 2) {
        VLOG(1) << "Find odd length path.";
        auto previous = rHist[rHist.size() - 2].iter();
        findAllPaths(previous.get(), table, ds);
    }

    if (count_ * 2 <= steps) {
        VLOG(1) << "Find even length path.";
        auto latest = rHist.back().iter();
        findAllPaths(latest.get(), table, ds);
    }

    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

bool ConjunctPathExecutor::findAllPaths(Iterator* backwardPathsIter,
                                        std::unordered_map<Value, const List&>& forwardPathsTable,
                                        DataSet& ds) {
    bool found = false;
    for (; backwardPathsIter->valid(); backwardPathsIter->next()) {
        auto& dst = backwardPathsIter->getColumn(kVid);
        VLOG(1) << "Backward dst: " << dst;
        auto& pathList = backwardPathsIter->getColumn(kPathStr);
        if (!pathList.isList()) {
            continue;
        }
        for (const auto& path : pathList.getList().values) {
            if (!path.isPath()) {
                continue;
            }
            auto forwardPaths = forwardPathsTable.find(dst);
            if (forwardPaths == forwardPathsTable.end()) {
                continue;
            }

            for (const auto& i : forwardPaths->second.values) {
                if (!i.isPath()) {
                    continue;
                }
                Row row;
                auto forward = i.getPath();
                Path backward = path.getPath();
                if (forward.src == backward.src) {
                    continue;
                }
                VLOG(1) << "Forward path:" << forward;
                VLOG(1) << "Backward path:" << backward;
                backward.reverse();
                VLOG(1) << "Backward reverse path:" << backward;
                forward.append(std::move(backward));
                if (forward.hasDuplicateEdges()) {
                    continue;
                }
                if (noLoop_ && forward.hasDuplicateVertices()) {
                    continue;
                }
                VLOG(1) << "Found path: " << forward;
                row.values.emplace_back(std::move(forward));
                ds.rows.emplace_back(std::move(row));
            }  // `i'
            found = true;
        }  // `path'
    }  // `backwardPathsIter'
    return found;
}

}  // namespace graph
}  // namespace nebula
