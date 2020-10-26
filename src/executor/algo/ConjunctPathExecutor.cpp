/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ConjunctPathExecutor.h"

#include "planner/Algo.h"

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

    DataSet ds;
    ds.colNames = conjunct->colNames();

    VLOG(1) << "forward, size: " << forward_.size();
    VLOG(1) << "backward, size: " << backward_.size();
    forward_.emplace_back();
    for (; lIter->valid(); lIter->next()) {
        auto& dst = lIter->getColumn(kVid);
        auto& edge = lIter->getColumn("edge");
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

    auto latest = rHist.back().iter();
    isLatest = true;
    backward_.emplace_back();
    VLOG(1) << "Find even length path.";
    auto rows = findBfsShortestPath(latest.get(), isLatest, forward_.back());
    if (!rows.empty()) {
        VLOG(1) << "Meet even length path.";
        ds.rows = std::move(rows);
    }
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

std::vector<Row> ConjunctPathExecutor::findBfsShortestPath(
    Iterator* iter,
    bool isLatest,
    std::multimap<Value, const Edge*>& table) {
    std::unordered_set<Value> meets;
    for (; iter->valid(); iter->next()) {
        auto& dst = iter->getColumn(kVid);
        if (isLatest) {
            auto& edge = iter->getColumn("edge");
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

std::multimap<Value, Path> ConjunctPathExecutor::buildBfsInterimPath(
    std::unordered_set<Value>& meets,
    std::vector<std::multimap<Value, const Edge*>>& hists) {
    std::multimap<Value, Path> results;
    for (auto& v : meets) {
        VLOG(1) << "Meet at: " << v;
        Path start;
        start.src = Vertex(v.getStr(), {});
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

folly::Future<Status> ConjunctPathExecutor::conjunctPath() {
    auto* conjunct = asNode<ConjunctPath>(node());
    auto lIter = ectx_->getResult(conjunct->leftInputVar()).iter();
    const auto& rHist = ectx_->getHistory(conjunct->rightInputVar());
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "left input: " << conjunct->leftInputVar()
            << " right input: " << conjunct->rightInputVar();
    DCHECK(!!lIter);

    DataSet ds;
    ds.colNames = conjunct->colNames();

    std::multimap<Value, const Path*> table;
    for (; lIter->valid(); lIter->next()) {
        auto& dst = lIter->getColumn(kVid);
        auto& path = lIter->getColumn("path");
        if (path.isPath() && !path.getPath().steps.empty()) {
            VLOG(1) << "Forward dst: " << dst;
            table.emplace(dst, &path.getPath());
        }
    }

    if (rHist.size() >= 2) {
        auto previous = rHist[rHist.size() - 2].iter();
        if (findPath(previous.get(), table, ds)) {
            VLOG(1) << "Meet odd length path.";
            return finish(ResultBuilder().value(Value(std::move(ds))).finish());
        }
    }

    auto latest = rHist.back().iter();
    findPath(latest.get(), table, ds);
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

bool ConjunctPathExecutor::findPath(Iterator* iter,
                                    std::multimap<Value, const Path*>& table,
                                    DataSet& ds) {
    bool found = false;
    for (; iter->valid(); iter->next()) {
        auto& dst = iter->getColumn(kVid);
        VLOG(1) << "Backward dst: " << dst;
        auto& path = iter->getColumn("path");
        if (path.isPath()) {
            auto paths = table.equal_range(dst);
            if (paths.first != paths.second) {
                for (auto i = paths.first; i != paths.second; ++i) {
                    Row row;
                    auto forward = *i->second;
                    Path backward = path.getPath();
                    VLOG(1) << "Forward path:" << forward;
                    VLOG(1) << "Backward path:" << backward;
                    backward.reverse();
                    VLOG(1) << "Backward reverse path:" << backward;
                    forward.append(std::move(backward));
                    VLOG(1) << "Found path: " << forward;
                    row.values.emplace_back(std::move(forward));
                    ds.rows.emplace_back(std::move(row));
                }
                found = true;
            }
        }
    }
    return found;
}

folly::Future<Status> ConjunctPathExecutor::allPaths() {
    auto* conjunct = asNode<ConjunctPath>(node());
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
        auto& path = lIter->getColumn("path");
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
        auto& pathList = backwardPathsIter->getColumn("path");
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
                VLOG(1) << "Forward path:" << forward;
                VLOG(1) << "Backward path:" << backward;
                backward.reverse();
                VLOG(1) << "Backward reverse path:" << backward;
                forward.append(std::move(backward));
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
