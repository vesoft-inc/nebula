/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/ProduceSemiShortestPathExecutor.h"

#include "planner/plan/Algo.h"

namespace nebula {
namespace graph {

std::vector<Path> ProduceSemiShortestPathExecutor::createPaths(
    const std::vector<const Path*>& paths,
    const Edge& edge) {
    std::vector<Path> newPaths;
    newPaths.reserve(paths.size());
    for (auto p : paths) {
        Path path = *p;
        path.steps.emplace_back(Step(Vertex(edge.dst, {}), edge.type, edge.name, edge.ranking, {}));
        newPaths.emplace_back(std::move(path));
    }
    return newPaths;
}

void ProduceSemiShortestPathExecutor::dstInCurrent(const Edge& edge,
                                                   CostPathMapType& currentCostPathMap) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    auto weight = 1;   // weight = weight_->getWeight();
    auto& srcPaths = historyCostPathMap_[src];

    for (auto& srcPath : srcPaths) {
        if (currentCostPathMap[dst].find(srcPath.first) != currentCostPathMap[dst].end()) {
            // startVid in currentCostPathMap[dst]
            auto newCost = srcPath.second.cost_ + weight;
            auto oldCost = currentCostPathMap[dst][srcPath.first].cost_;
            if (newCost > oldCost) {
                continue;
            } else if (newCost < oldCost) {
                // update (dst, startVid)'s path
                std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                currentCostPathMap[dst][srcPath.first].cost_ = newCost;
                currentCostPathMap[dst][srcPath.first].paths_.swap(newPaths);
            } else {
                // add (dst, startVid)'s path
                std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                for (auto& p : newPaths) {
                    currentCostPathMap[dst][srcPath.first].paths_.emplace_back(std::move(p));
                }
            }
        } else {
            // startVid not in currentCostPathMap[dst]
            auto newCost = srcPath.second.cost_ + weight;
            std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
            // dst in history
            if (historyCostPathMap_.find(dst) != historyCostPathMap_.end()) {
                if (historyCostPathMap_[dst].find(srcPath.first) !=
                    historyCostPathMap_[dst].end()) {
                    auto historyCost = historyCostPathMap_[dst][srcPath.first].cost_;
                    if (newCost > historyCost) {
                        continue;
                    } else {
                        removeSamePath(newPaths, historyCostPathMap_[dst][srcPath.first].paths_);
                    }
                }
            }
            if (!newPaths.empty()) {
                CostPaths temp(newCost, newPaths);
                currentCostPathMap[dst].emplace(srcPath.first, std::move(temp));
            }
        }
    }
}

void ProduceSemiShortestPathExecutor::dstNotInHistory(const Edge& edge,
                                                      CostPathMapType& currentCostPathMap) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    auto weight = 1;   // weight = weight_->getWeight();
    auto& srcPaths = historyCostPathMap_[src];
    if (currentCostPathMap.find(dst) == currentCostPathMap.end()) {
        //  dst not in history and not in current
        for (auto& srcPath : srcPaths) {
            auto cost = srcPath.second.cost_ + weight;

            std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
            currentCostPathMap[dst].emplace(srcPath.first, CostPaths(cost, std::move(newPaths)));
        }
    } else {
        // dst in current
        dstInCurrent(edge, currentCostPathMap);
    }
}

void ProduceSemiShortestPathExecutor::removeSamePath(std::vector<Path>& paths,
                                                     std::vector<const Path*>& historyPathsPtr) {
    for (auto ptr : historyPathsPtr) {
        auto iter = paths.begin();
        while (iter != paths.end()) {
            if (*iter == *ptr) {
                VLOG(2) << "Erese Path :" << *iter;
                iter = paths.erase(iter);
            } else {
                ++iter;
            }
        }
    }
}

void ProduceSemiShortestPathExecutor::dstInHistory(const Edge& edge,
                                                   CostPathMapType& currentCostPathMap) {
    auto& src = edge.src;
    auto& dst = edge.dst;
    auto weight = 1;   // weight = weight_->getWeight();
    auto& srcPaths = historyCostPathMap_[src];

    if (currentCostPathMap.find(dst) == currentCostPathMap.end()) {
        // dst not in current but in history
        for (auto& srcPath : srcPaths) {
            if (historyCostPathMap_[dst].find(srcPath.first) == historyCostPathMap_[dst].end()) {
                //  (dst, startVid)'s path not in history
                auto newCost = srcPath.second.cost_ + weight;
                std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                currentCostPathMap[dst].emplace(srcPath.first,
                                                CostPaths(newCost, std::move(newPaths)));
            } else {
                //  (dst, startVid)'s path in history, compare cost
                auto newCost = srcPath.second.cost_ + weight;
                auto historyCost = historyCostPathMap_[dst][srcPath.first].cost_;
                if (newCost > historyCost) {
                    continue;
                } else if (newCost < historyCost) {
                    // update (dst, startVid)'s path
                    std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                    currentCostPathMap[dst].emplace(srcPath.first,
                                                    CostPaths(newCost, std::move(newPaths)));
                } else {
                    std::vector<Path> newPaths = createPaths(srcPath.second.paths_, edge);
                    // if same path in history, remove it
                    removeSamePath(newPaths, historyCostPathMap_[dst][srcPath.first].paths_);
                    if (newPaths.empty()) {
                        continue;
                    }
                    currentCostPathMap[dst].emplace(srcPath.first,
                                                    CostPaths(newCost, std::move(newPaths)));
                }
            }
        }
    } else {
        // dst in current
        dstInCurrent(edge, currentCostPathMap);
    }
}

void ProduceSemiShortestPathExecutor::updateHistory(const Value& dst,
                                                    const Value& src,
                                                    double cost,
                                                    Value& paths) {
    const List& pathList = paths.getList();
    std::vector<const Path*> tempPathsPtr;
    tempPathsPtr.reserve(pathList.size());
    for (auto& p : pathList.values) {
        tempPathsPtr.emplace_back(&p.getPath());
    }

    if (historyCostPathMap_.find(dst) == historyCostPathMap_.end()) {
        // insert path to history
        std::unordered_map<Value, CostPathsPtr> temp = {{src, CostPathsPtr(cost, tempPathsPtr)}};
        historyCostPathMap_.emplace(dst, std::move(temp));
    } else {
        if (historyCostPathMap_[dst].find(src) == historyCostPathMap_[dst].end()) {
            // startVid not in history ; insert it
            historyCostPathMap_[dst].emplace(src, CostPathsPtr(cost, tempPathsPtr));
        } else {
            // startVid in history; compare cost
            auto historyCost = historyCostPathMap_[dst][src].cost_;
            if (cost < historyCost) {
                historyCostPathMap_[dst][src].cost_ = cost;
                historyCostPathMap_[dst][src].paths_.swap(tempPathsPtr);
            } else if (cost == historyCost) {
                for (auto p : tempPathsPtr) {
                    historyCostPathMap_[dst][src].paths_.emplace_back(p);
                }
            } else {
                LOG(FATAL) << "current Cost : " << cost << " history Cost : " << historyCost;
            }
        }
    }
}

folly::Future<Status> ProduceSemiShortestPathExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* pssp = asNode<ProduceSemiShortestPath>(node());
    auto iter = ectx_->getResult(pssp->inputVar()).iter();
    VLOG(1) << "current: " << node()->outputVar();
    VLOG(1) << "input: " << pssp->inputVar();
    DCHECK(!!iter);

    CostPathMapType currentCostPathMap;

    for (; iter->valid(); iter->next()) {
        auto edgeVal = iter->getEdge();
        if (!edgeVal.isEdge()) {
            continue;
        }
        auto& edge = edgeVal.getEdge();
        auto& src = edge.src;
        auto& dst = edge.dst;
        auto weight = 1;

        if (historyCostPathMap_.find(src) == historyCostPathMap_.end()) {
            // src not in history, now src must be startVid
            Path path;
            path.src = Vertex(src, {});
            path.steps.emplace_back(Step(Vertex(dst, {}), edge.type, edge.name, edge.ranking, {}));
            if (currentCostPathMap.find(dst) != currentCostPathMap.end()) {
                if (currentCostPathMap[dst].find(src) == currentCostPathMap[dst].end()) {
                    CostPaths costPaths(weight, {std::move(path)});
                    currentCostPathMap[dst].emplace(src, std::move(costPaths));
                } else {
                    // same (src, dst), diffrent edge type or rank
                    auto currentCost = currentCostPathMap[dst][src].cost_;
                    if (weight == currentCost) {
                        currentCostPathMap[dst][src].paths_.emplace_back(std::move(path));
                    } else if (weight < currentCost) {
                        std::vector<Path> tempPaths ={std::move(path)};
                        currentCostPathMap[dst][src].paths_.swap(tempPaths);
                    } else {
                        continue;
                    }
                }
            } else {
                CostPaths costPaths(weight, {std::move(path)});
                currentCostPathMap[dst].emplace(src, std::move(costPaths));
            }
        } else {
            if (historyCostPathMap_.find(dst) == historyCostPathMap_.end()) {
                dstNotInHistory(edge, currentCostPathMap);
            } else {
                dstInHistory(edge, currentCostPathMap);
            }
        }
    }

    DataSet ds;
    ds.colNames = node()->colNames();
    for (auto& dstPath : currentCostPathMap) {
        auto& dst = dstPath.first;
        for (auto& srcPath : dstPath.second) {
            auto& src = srcPath.first;
            auto cost = srcPath.second.cost_;
            List paths;
            paths.values.reserve(srcPath.second.paths_.size());
            for (auto & path : srcPath.second.paths_) {
                paths.values.emplace_back(std::move(path));
            }
            Row row;
            row.values.emplace_back(std::move(dst));
            row.values.emplace_back(std::move(src));
            row.values.emplace_back(std::move(cost));
            row.values.emplace_back(std::move(paths));
            ds.rows.emplace_back(std::move(row));

            // update (dst, startVid)'s paths to history
            updateHistory(dst, srcPath.first, cost, ds.rows.back().values.back());
        }
    }
    VLOG(2) << "SemiShortPath : " << ds;
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}


}   // namespace graph
}   // namespace nebula
