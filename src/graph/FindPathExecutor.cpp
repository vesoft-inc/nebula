/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "FindPathExecutor.h"

namespace nebula {
namespace graph {

FindPathExecutor::FindPathExecutor(Sentence *sentence, ExecutionContext *exct)
    : TraverseExecutor(exct, "find_path") {
    sentence_ = static_cast<FindPathSentence*>(sentence);
}

Status FindPathExecutor::prepare() {
    return Status::OK();
}

Status FindPathExecutor::prepareClauses() {
    spaceId_ = ectx()->rctx()->session()->space();
    Status status;
    expCtx_ = std::make_unique<ExpressionContext>();
    expCtx_->setStorageClient(ectx()->getStorageClient());
    expCtx_->setSpace(spaceId_);
    do {
        shortest_ = sentence_->isShortest();
        singleShortest_ = sentence_->isSingleShortest();
        noLoop_ = sentence_->isNoLoop();
        oneWayTraverse_ = sentence_->isOneWayTraverse();

        if (sentence_->from() != nullptr) {
            sentence_->from()->setContext(expCtx_.get());
            status = sentence_->from()->prepare(from_);
            if (!status.ok()) {
                break;
            }
        }

        if (!oneWayTraverse_ && sentence_->to() != nullptr) {
            sentence_->to()->setContext(expCtx_.get());
            status = sentence_->to()->prepare(to_);
            if (!status.ok()) {
                break;
            }
        }

        if (sentence_->over() != nullptr) {
            direction_ = sentence_->over()->direction();
            status = sentence_->over()->prepare(over_);
            if (!status.ok()) {
                break;
            }
        }

        if (sentence_->step() != nullptr) {
            status = sentence_->step()->prepare(step_);
            if (!status.ok()) {
                break;
            }
        }

        whereWrapper_ = std::make_unique<WhereWrapper>(sentence_->where());
        status = whereWrapper_->prepare(expCtx_.get());
        if (!status.ok()) {
            break;
        }

        if (step_.recordFrom_ > 0 && shortest_) {
            status = Status::Error("SHORTEST does not compatible with WITHIN");
        }
    } while (false);

    if (!status.ok()) {
        stats::Stats::addStatsValue(stats_.get(), false, duration().elapsedInUSec());
    }

    return status;
}

Status FindPathExecutor::beforeExecute() {
    Status status;
    do {
        status = checkIfGraphSpaceChosen();
        if (!status.ok()) {
            break;
        }

        status = prepareClauses();
        if (!status.ok()) {
            break;
        }

        status = prepareOver();
        if (!status.ok()) {
            break;
        }

        status = setupVids();
        if (!status.ok()) {
            break;
        }

        status = prepareNeededProps();
        if (!status.ok()) {
            break;
        }

        status = checkNeededProps();
        if (!status.ok()) {
            break;
        }
    } while (false);
    return status;
}

Status FindPathExecutor::prepareOverAll() {
    auto edgeAllStatus = ectx()->schemaManager()->getAllEdge(spaceId_);

    if (!edgeAllStatus.ok()) {
        return edgeAllStatus.status();
    }

    auto allEdge = edgeAllStatus.value();
    for (auto &e : allEdge) {
        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId_, e);
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        Status status = addToEdgeTypes(v);
        if (!status.ok()) {
            return status;
        }

        if (!expCtx_->addEdge(e, v)) {
            return Status::Error(folly::sformat("edge alias({}) was dup", e));
        }
        edgeTypeNameMap_.emplace(v, e);
    }

    return Status::OK();
}

Status FindPathExecutor::prepareOver() {
    Status status = Status::OK();

    for (auto e : over_.edges_) {
        if (e->isOverAll()) {
            expCtx_->setOverAllEdge();
            return prepareOverAll();
        }

        auto edgeStatus = ectx()->schemaManager()->toEdgeType(spaceId_, *e->edge());
        if (!edgeStatus.ok()) {
            return edgeStatus.status();
        }

        auto v = edgeStatus.value();
        status = addToEdgeTypes(v);
        if (!status.ok()) {
            return status;
        }

        if (e->alias() != nullptr) {
            if (!expCtx_->addEdge(*e->alias(), v)) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->alias()));
            }
        } else {
            if (!expCtx_->addEdge(*e->edge(), v)) {
                return Status::Error(folly::sformat("edge alias({}) was dup", *e->edge()));
            }
        }

        edgeTypeNameMap_.emplace(v, *e->edge());
    }

    return status;
}

Status FindPathExecutor::addToEdgeTypes(EdgeType type) {
    switch (direction_) {
        case OverClause::Direction::kForward: {
            fEdgeTypes_.push_back(type);
            tEdgeTypes_.push_back(-type);
            break;
        }
        case OverClause::Direction::kBackward: {
            fEdgeTypes_.push_back(-type);
            tEdgeTypes_.push_back(type);
            break;
        }
        case OverClause::Direction::kBidirect: {
            fEdgeTypes_.push_back(type);
            fEdgeTypes_.push_back(-type);
            tEdgeTypes_.push_back(type);
            tEdgeTypes_.push_back(-type);
            break;
        }
        default: {
            return Status::Error("Unknown direction type: %ld", static_cast<int64_t>(direction_));
        }
    }
    return Status::OK();
}

Status FindPathExecutor::prepareNeededProps() {
    auto status = Status::OK();
    do {
        auto &tagMap = expCtx_->getTagMap();
        auto spaceId = ectx()->rctx()->session()->space();
        for (auto &entry : tagMap) {
            auto tagId = ectx()->schemaManager()->toTagID(spaceId, entry.first);
            if (!tagId.ok()) {
                status = Status::Error("Tag `%s' not found.", entry.first.c_str());
                break;
            }
            entry.second = tagId.value();
        }
    } while (false);

    return status;
}

Status FindPathExecutor::checkNeededProps() {
    auto space = ectx()->rctx()->session()->space();
    TagID tagId;
    auto srcTagProps = expCtx_->srcTagProps();
    auto dstTagProps = expCtx_->dstTagProps();
    srcTagProps.insert(srcTagProps.begin(),
                       std::make_move_iterator(dstTagProps.begin()),
                       std::make_move_iterator(dstTagProps.end()));
    for (auto &pair : srcTagProps) {
        auto found = expCtx_->getTagId(pair.first, tagId);
        if (!found) {
            return Status::Error("Tag `%s' not found.", pair.first.c_str());
        }
        auto ts = ectx()->schemaManager()->getTagSchema(space, tagId);
        if (ts == nullptr) {
            return Status::Error("No tag schema for %s", pair.first.c_str());
        }
        if (ts->getFieldIndex(pair.second) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                                 pair.second.c_str(), pair.first.c_str());
        }
    }

    EdgeType edgeType;
    auto edgeProps = expCtx_->aliasProps();
    for (auto &pair : edgeProps) {
        auto found = expCtx_->getEdgeType(pair.first, edgeType);
        if (!found) {
            return Status::Error("Edge `%s' not found.", pair.first.c_str());
        }
        auto propName = pair.second;
        if (propName == _SRC || propName == _DST
            || propName == _RANK || propName == _TYPE) {
            continue;
        }
        auto es = ectx()->schemaManager()->getEdgeSchema(space, std::abs(edgeType));
        if (es == nullptr) {
            return Status::Error("No edge schema for %s", pair.first.c_str());
        }
        if (es->getFieldIndex(propName) == -1) {
            return Status::Error("`%s' is not a prop of `%s'",
                                 propName.c_str(), pair.first.c_str());
        }
    }

    return Status::OK();
}

void FindPathExecutor::execute() {
    auto status = beforeExecute();
    if (!status.ok()) {
        doError(std::move(status));
        return;
    }

    fromVids_ = from_.vids_;
    visitedFrom_.insert(fromVids_.begin(), fromVids_.end());
    for (auto &v : fromVids_) {
        Path path;
        pathFrom_.emplace(v, std::move(path));
    }

    if (oneWayTraverse_) {
        // oneWayTraverse_ : FROM --------> (all reachable tags)
        steps_ = step_.recordTo_;
        toVids_ = from_.vids_;
        visitedTo_.insert(fromVids_.begin(), fromVids_.end());
    } else {
        // !oneWayTraverse_ : FROM ---->  <---- TO
        steps_ = step_.recordTo_ / 2 + step_.recordTo_ % 2;
        toVids_ = to_.vids_;
        visitedTo_.insert(toVids_.begin(), toVids_.end());
        targetNotFound_.insert(toVids_.begin(), toVids_.end());
        for (auto &v : toVids_) {
            Path path;
            pathTo_.emplace(v, std::move(path));
        }
    }

    getNeighborsAndFindPath();
}

void FindPathExecutor::getNeighborsAndFindPath() {
    // We meet the dead end.
    if (fromVids_.empty() || toVids_.empty()) {
        onFinish_(Executor::ProcessControl::kNext);
        return;
    }

    fPro_ = std::make_unique<folly::Promise<folly::Unit>>();
    tPro_ = std::make_unique<folly::Promise<folly::Unit>>();
    std::vector<folly::Future<folly::Unit>> futures;

    futures.emplace_back(fPro_->getFuture());
    auto props = getStepOutProps(false);
    if (!props.ok()) {
        doError(std::move(props).status());
        return;
    }
    getFromFrontiers(std::move(props).value());

    if (!oneWayTraverse_) {
        futures.emplace_back(tPro_->getFuture());
        props = getStepOutProps(true);
        if (!props.ok()) {
            doError(std::move(props).status());
            return;
        }
        getToFrontiers(std::move(props).value());
    }

    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        UNUSED(result);
        if (!fStatus_.ok() || !tStatus_.ok()) {
            std::string msg = fStatus_.toString() + " " + tStatus_.toString();
            doError(Status::Error(std::move(msg)));
            return;
        }

        findPath();
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        doError(Status::Error("Find path exception: %s", e.what().c_str()));
    };
    folly::collectAll(futures).via(runner).thenValue(cb).thenError(error);
}

void FindPathExecutor::findPath() {
    VLOG(1) << "Find Path at Step: " << currentStep_;
    VLOG(1) << "Visited froms: " << visitedFrom_.size();
    VLOG(1) << "Visited tos: " << visitedTo_.size();
    visitedFrom_.clear();
    pathFromShortestLength_.clear();
    pathToShortestLength_.clear();
    std::multimap<VertexID, Path> pathF;
    VLOG(1) << "Frontier froms: " << fromFrontiers_.second.size();
    VLOG(1) << "Path from size: " << pathFrom_.size();

    if (!oneWayTraverse_) {
        VLOG(1) << "Frontier tos: " << toFrontiers_.second.size();
        VLOG(1) << "Path to size: " << pathTo_.size();
    }

    int64_t frontierUpdateCount = 0L;
    for (auto &frontier : fromFrontiers_.second) {
        // Notice: we treat edges with different ranking
        // between two vertices as different path
        for (auto &neighbor : frontier.second) {
            frontierUpdateCount++;
            auto dstId = std::get<0>(neighbor);
            VLOG(2) << "src vertex:" << frontier.first;
            VLOG(2) << "dst vertex:" << dstId;
            // if current neighbor is acceptable
            // update the path to frontiers
            if (updatePath(frontier.first, pathFrom_, neighbor, pathF, VisitedBy::FROM)) {
                // only emplace dstId when at least one path was updated
                visitedFrom_.emplace(dstId);
                if (oneWayTraverse_) {
                    // one way traverse, save every reachable tag's path
                    saveSinglePath(frontier.first, neighbor);
                } else if (visitedTo_.count(dstId) == 1) {
                    // if frontiers of F are neighbors of visitedByT,
                    // we found an odd path
                    meetOddPath(frontier.first, dstId, neighbor);
                }
            }
        }  // for `neighbor'
    }  // for `frontier'

    VLOG(1) << "Frontier froms scanned: " << frontierUpdateCount;
    pathFrom_ = std::move(pathF);
    fromVids_.clear();
    fromVids_.reserve(visitedFrom_.size());
    std::copy(visitedFrom_.begin(),
              visitedFrom_.end(), std::back_inserter(fromVids_));
    VLOG(1) << "Next froms vids: " << fromVids_.size();

    if (!oneWayTraverse_) {
        visitedTo_.clear();
        std::multimap<VertexID, Path> pathT;
        frontierUpdateCount = 0L;
        for (auto &frontier : toFrontiers_.second) {
            for (auto &neighbor : frontier.second) {
                frontierUpdateCount++;
                auto dstId = std::get<0>(neighbor);
                // update the path to frontiers
                // only emplace dstId when at least one path was updated
                if (updatePath(frontier.first, pathTo_, neighbor, pathT, VisitedBy::TO)) {
                    visitedTo_.emplace(dstId);
                }
            }   // for `neighbor'
        }       // for `frontier'

        VLOG(1) << "Frontier tos scanned: " << frontierUpdateCount;
        pathTo_ = std::move(pathT);
        toVids_.clear();
        toVids_.reserve(visitedTo_.size());
        std::copy(visitedTo_.begin(), visitedTo_.end(), std::back_inserter(toVids_));
        VLOG(1) << "Next to vids: " << toVids_.size();

        std::sort(fromVids_.begin(), fromVids_.end());
        std::sort(toVids_.begin(), toVids_.end());
        std::set<VertexID> intersect;
        std::set_intersection(fromVids_.begin(),
                              fromVids_.end(),
                              toVids_.begin(),
                              toVids_.end(),
                              std::inserter(intersect, intersect.end()));

        // if frontiersF meets frontiersT, we found an even path
        if (!intersect.empty()) {
            if (shortest_ && targetNotFound_.empty()) {
                doFinish(Executor::ProcessControl::kNext);
                return;
            }

            for (auto intersectId : intersect) {
                meetEvenPath(intersectId);
            }   // `intersectId'
        }
    }

    VLOG(1) << "Current step done: " << currentStep_;
    if (isFinalStep() ||
        (shortest_ && targetNotFound_.empty() && !oneWayTraverse_)) {
        doFinish(Executor::ProcessControl::kNext);
        return;
    } else {
        ++currentStep_;
    }
    getNeighborsAndFindPath();
}

inline void FindPathExecutor::saveSinglePath(VertexID src, Neighbor &neighbor) {
    VLOG(2) << "Save Single Path.";
    auto rangeF = pathFrom_.equal_range(src);
    for (auto i = rangeF.first; i != rangeF.second; ++i) {
        // Build single path:
        // i->second + (src,type,ranking) + (dst, 0, 0)
        Path path = i->second;
        auto s0 = std::make_unique<StepOut>(neighbor);
        std::get<0>(*s0) = src;
        path.emplace_back(s0.get());
        stepOutHolder_.emplace(std::move(s0));
        VLOG(2) << "PathF: " << buildPathString(path);

        auto s1 = std::make_unique<StepOut>(neighbor);
        std::get<1>(*s1) = 0;
        std::get<2>(*s1) = 0L;
        path.emplace_back(s1.get());
        stepOutHolder_.emplace(std::move(s1));

        auto target = std::get<0>(*(path.back()));
        if (shortest_) {
            auto pathFound = finalPath_.find(target);
            if (pathFound != finalPath_.end()
                && pathFound->second.size() < path.size()) {
                // already found a shorter path
                continue;
            } else if (pathFound != finalPath_.end()
                       && pathFound->second.size() == path.size() && singleShortest_) {
                // single shortest, drop path which length equals to found
                continue;
            } else {
                VLOG(2) << "Found path: " << buildPathString(path);
                finalPath_.emplace(std::move(target), std::move(path));
            }
        } else {
            if (step_.recordFrom_ > 0 && path.size() <= step_.recordFrom_) {
                // ignore path **REAL** length < step from
                continue;
            }
            VLOG(2) << "Found path: " << buildPathString(path);
            finalPath_.emplace(std::move(target), std::move(path));
        }
    }  // for `i'
}

inline void FindPathExecutor::meetOddPath(VertexID src, VertexID dst, Neighbor &neighbor) {
    VLOG(2) << "Meet Odd Path.";
    auto rangeF = pathFrom_.equal_range(src);
    for (auto i = rangeF.first; i != rangeF.second; ++i) {
        auto rangeT = pathTo_.equal_range(dst);
        for (auto j = rangeT.first; j != rangeT.second; ++j) {
            if (j->second.size() + i->second.size() > step_.recordTo_) {
                continue;
            }
            // avoid ABA loop when meetOddPath
            auto n0 = std::make_unique<Neighbor>(neighbor);
            std::get<0>(*n0) = src;
            std::get<1>(*n0) = - std::get<1>(neighbor);
            if (!isPathAcceptable(j->second, 0L, *n0, VisitedBy::TO, false)) {
                continue;
            }
            // Build path:
            // i->second + (src,type,ranking) + (dst, -type, ranking) + j->second
            Path path = i->second;
            auto s0 = std::make_unique<StepOut>(neighbor);
            std::get<0>(*s0) = src;
            path.emplace_back(s0.get());
            stepOutHolder_.emplace(std::move(s0));
            VLOG(2) << "PathF: " << buildPathString(path);

            auto s1 = std::make_unique<StepOut>(neighbor);
            std::get<1>(*s1) = - std::get<1>(neighbor);
            path.emplace_back(s1.get());
            stepOutHolder_.emplace(std::move(s1));
            path.insert(path.end(), j->second.begin(), j->second.end());
            VLOG(2) << "PathT: " << buildPathString(j->second);

            auto target = std::get<0>(*(path.back()));
            if (shortest_) {
                targetNotFound_.erase(target);
                auto pathFound = finalPath_.find(target);
                if (pathFound != finalPath_.end()
                    && pathFound->second.size() < path.size()) {
                    // already found a shorter path
                    continue;
                } else if (pathFound != finalPath_.end()
                           && pathFound->second.size() == path.size() && singleShortest_) {
                    // single shortest, drop path which length equals to found
                    continue;
                } else {
                    VLOG(2) << "Found path: " << buildPathString(path);
                    finalPath_.emplace(std::move(target), std::move(path));
                }
            } else {
                if (step_.recordFrom_ > 0 && path.size() <= step_.recordFrom_) {
                    // ignore path **REAL** length < step from
                    continue;
                }
                VLOG(2) << "Found path: " << buildPathString(path);
                finalPath_.emplace(std::move(target), std::move(path));
            }
        }  // for `j'
    }  // for `i'
}

inline void FindPathExecutor::meetEvenPath(VertexID intersectId) {
    VLOG(2) << "Meet Even Path.";
    auto rangeF = pathFrom_.equal_range(intersectId);
    auto rangeT = pathTo_.equal_range(intersectId);
    for (auto i = rangeF.first; i != rangeF.second; ++i) {
        for (auto j = rangeT.first; j != rangeT.second; ++j) {
            if (j->second.size() + i->second.size() > step_.recordTo_) {
                continue;
            }
            // Build path:
            // i->second + (src,type,ranking) + j->second
            Path path = i->second;
            VLOG(2) << "PathF: " << buildPathString(path);
            if (j->second.size() > 0) {
                StepOut *s = j->second.front();
                // avoid ABA loop when meetEvenPath
                auto n0 = std::make_unique<Neighbor>(*s);
                std::get<1>(*n0) = - std::get<1>(*n0);
                if (!isPathAcceptable(i->second, 0L, *n0, VisitedBy::FROM, false)) {
                    continue;
                }
                auto s0 = std::make_unique<StepOut>(*s);
                std::get<0>(*s0) = intersectId;
                std::get<1>(*s0) = - std::get<1>(*s0);
                path.emplace_back(s0.get());
                stepOutHolder_.emplace(std::move(s0));
                VLOG(2) << "Joiner: " << buildPathString(path);
            } else if (i->second.size() > 0) {
                StepOut *s = i->second.back();
                // avoid ABA loop when meetEvenPath
                auto n0 = std::make_unique<Neighbor>(*s);
                std::get<1>(*n0) = - std::get<1>(*n0);
                if (!isPathAcceptable(j->second, 0L, *n0, VisitedBy::TO, false)) {
                    continue;
                }
                auto s0 = std::make_unique<StepOut>(*s);
                std::get<0>(*s0) = intersectId;
                path.emplace_back(s0.get());
                stepOutHolder_.emplace(std::move(s0));
                VLOG(2) << "Joiner: " << buildPathString(path);
            }
            VLOG(2) << "PathT: " << buildPathString(j->second);
            path.insert(path.end(), j->second.begin(), j->second.end());

            auto target = std::get<0>(*(path.back()));
            if (shortest_) {
                auto pathFound = finalPath_.find(target);
                if (pathFound != finalPath_.end()
                    && pathFound->second.size() < path.size()) {
                    // already found a shorter path
                    continue;
                } else if (pathFound != finalPath_.end()
                    && pathFound->second.size() == path.size() && singleShortest_) {
                    // single shortest, drop path which length equals to found
                    continue;
                } else {
                    targetNotFound_.erase(target);
                    VLOG(2) << "Found path: " << buildPathString(path);
                    finalPath_.emplace(std::move(target), std::move(path));
                }
            } else {
                if (step_.recordFrom_ > 0 && path.size() <= step_.recordFrom_) {
                    // ignore path **REAL** length < step from
                    continue;
                }
                VLOG(2) << "Found path: " << buildPathString(path);
                finalPath_.emplace(std::move(target), std::move(path));
            }
        }
    }
}

inline bool FindPathExecutor::updatePath(
    VertexID &src,
    std::multimap<VertexID, Path> &pathToSrc,
    Neighbor &neighbor,
    std::multimap<VertexID, Path> &pathToNeighbor,
    VisitedBy visitedBy) {
    VLOG(2) << "Update Path.";
    auto range = pathToSrc.equal_range(src);
    bool atLeastOnePathUpdated = false;
    for (auto i = range.first; i != range.second; ++i) {
        // Build path:
        // i->second + (src,type,ranking)
        Path path = i->second;
        VLOG(2) << "Interim path before :" << buildPathString(path);
        VLOG(2) << "Interim path length before:" << path.size();
        auto s = std::make_unique<StepOut>(neighbor);
        std::get<0>(*s) = src;
        if (!isPathAcceptable(path, src, neighbor, visitedBy, shortest_)) {
            continue;
        }
        if (visitedBy == VisitedBy::FROM) {
            path.emplace_back(s.get());
        } else {
            path.emplace(path.begin(), s.get());
        }
        VLOG(2) << "Neighbor: " << std::get<0>(neighbor);
        VLOG(2) << "Interim path:" << buildPathString(path);
        VLOG(2) << "Interim path length:" << path.size();
        stepOutHolder_.emplace(std::move(s));
        pathToNeighbor.emplace(std::get<0>(neighbor), std::move(path));
        atLeastOnePathUpdated = true;
    }  // for `i'
    return atLeastOnePathUpdated;
}

inline bool FindPathExecutor::isPathAcceptable(Path path, VertexID stepOutSrcId, Neighbor &neighbor,
                                               VisitedBy visitedBy, bool dstShortest) {
    auto thisId = std::get<0>(neighbor);

    // drop non-shortest path
    if (dstShortest) {
        auto thisPathLength = path.size();
        auto pathShortestLengthMap =
            visitedBy == VisitedBy::FROM ? &pathFromShortestLength_ : &pathToShortestLength_;
        VertexID originVertexId;
        if (path.size() == 0) {
            originVertexId = stepOutSrcId;
        } else {
            originVertexId = visitedBy == VisitedBy::FROM ?
                                std::get<0>(*(path.front())) : std::get<0>(*(path.back()));
        }
        // from originVertex to currentNeighbor, only accept the shortest path we know **CURRENTLY**
        // to avoid **MOST** unnecessary updatePath() call in next step
        auto keyPair = std::make_pair(originVertexId, thisId);
        auto currentShortest = pathShortestLengthMap->find(keyPair);
        if (currentShortest == pathShortestLengthMap->end()) {
            pathShortestLengthMap->insert(std::make_pair(keyPair, thisPathLength));
            VLOG(2) << "accept_by_null originVertexId: " << originVertexId
                    << "thisId: " << thisId << " thisPathLength: " << thisPathLength;
        } else if (thisPathLength < currentShortest->second) {
            pathShortestLengthMap->erase(keyPair);
            pathShortestLengthMap->insert(std::make_pair(keyPair, thisPathLength));
            VLOG(2) << "accept_by_shorter originVertexId: " << originVertexId
                    << "thisId: " << thisId << " thisPathLength: " << thisPathLength
                    << " currentLength: " << currentShortest->second;
        } else if (thisPathLength == currentShortest->second) {
            if (singleShortest_) {
                VLOG(2) << "deny_by_equal originVertexId: " << originVertexId
                        << "thisId: " << thisId << " thisPathLength: " << thisPathLength
                        << " currentLength: " << currentShortest->second;
                return false;
            }
        } else {
            VLOG(2) << "deny_by_longer originVertexId: " << originVertexId
                        << "thisId: " << thisId << " thisPathLength: " << thisPathLength
                        << " currentLength: " << currentShortest->second;
            return false;
        }
    }

    if (path.size() > 0) {
        // avoid one-step loop when BIDIRECT
        if (direction_ == OverClause::Direction::kBidirect) {
            StepOut lastNode;
            if (visitedBy == VisitedBy::FROM) {
                lastNode = *path.back();
            } else {
                lastNode = *path.front();
            }
            auto lastId = std::get<0>(lastNode);
            auto lastEdge = std::get<1>(lastNode);
            auto lastRank = std::get<2>(lastNode);
            auto thisEdge = std::get<1>(neighbor);
            auto thisRank = std::get<2>(neighbor);
            if (lastId == thisId && lastEdge == -thisEdge && lastRank == thisRank) {
                return false;
            }
        }
        // avoid path loop when NOLOOP
        if (noLoop_) {
            auto iter = path.begin();
            for (; iter != path.end(); ++iter) {
                auto *step = *iter;
                auto id = std::get<0>(*step);
                if (id == thisId) {
                    return false;
                }
            }
        }
    }

    return true;
}

Status FindPathExecutor::setupVids() {
    Status status = Status::OK();
    do {
        if (sentence_->from()->isRef()) {
            status = setupVidsFromRef(from_);
            if (!status.ok()) {
                break;
            }
        }

        if (!oneWayTraverse_ && sentence_->to()->isRef()) {
            status = setupVidsFromRef(to_);
            if (!status.ok()) {
                break;
            }
        }
    } while (false);

    return status;
}

Status FindPathExecutor::setupVidsFromRef(Clause::Vertices &vertices) {
    const InterimResult *inputs;
    if (vertices.varname_ == nullptr) {
        inputs = inputs_.get();
        if (inputs == nullptr) {
            return Status::OK();
        }
    } else {
        inputs = ectx()->variableHolder()->get(*(vertices.varname_));
        if (inputs == nullptr) {
            return Status::Error("Variable `%s' not defined", vertices.varname_->c_str());
        }
    }

    auto status = checkIfDuplicateColumn();
    if (!status.ok()) {
        return status;
    }
    auto result = inputs->getDistinctVIDs(*(vertices.colname_));
    if (!result.ok()) {
        return std::move(result).status();
    }
    vertices.vids_ = std::move(result).value();
    return Status::OK();
}

void FindPathExecutor::getFromFrontiers(
    std::vector<storage::cpp2::PropDef> props) {
    auto future = ectx()->getStorageClient()->getNeighbors(spaceId_,
                                                           std::move(fromVids_),
                                                           fEdgeTypes_,
                                                           "",
                                                           std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        Frontiers frontiers;
        auto completeness = result.completeness();
        if (completeness == 0) {
            fStatus_ = Status::Error("Get neighbors failed.");
            fPro_->setValue();
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        auto requireNeighborProps = expCtx_->hasDstTagProp();
        if (requireNeighborProps) {
            std::vector<VertexID> neighborIds = getNeighborIdsFromResps(result);
            if (neighborIds.size() > 0) {
                fetchVertexPropsAndFilter(std::move(result), std::move(neighborIds), false);
                return;
            }
        }
        doWithFilter(std::move(result), false);
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        fStatus_ = Status::Error("Get neighbors exception: %s.", e.what().c_str());
    };
    std::move(future).via(runner, folly::Executor::HI_PRI).thenValue(cb).thenError(error);
}

void FindPathExecutor::getToFrontiers(
    std::vector<storage::cpp2::PropDef> props) {
    auto future = ectx()->getStorageClient()->getNeighbors(spaceId_,
                                                           std::move(toVids_),
                                                           tEdgeTypes_,
                                                           "",
                                                           std::move(props));
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this] (auto &&result) {
        Frontiers frontiers;
        auto completeness = result.completeness();
        if (completeness == 0) {
            tStatus_ = Status::Error("Get neighbors failed.");
            tPro_->setValue();
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
            ectx()->addWarningMsg("Find path executor was partially performed");
        }
        auto requireNeighborProps = expCtx_->hasSrcTagProp();
        if (requireNeighborProps) {
            std::vector<VertexID> neighborIds = getNeighborIdsFromResps(result);
            if (neighborIds.size() > 0) {
                fetchVertexPropsAndFilter(std::move(result), std::move(neighborIds), true);
                return;
            }
        }
        doWithFilter(std::move(result), true);
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        tStatus_ = Status::Error("Get neighbors exception: %s.", e.what().c_str());
    };
    std::move(future).via(runner, folly::Executor::HI_PRI).thenValue(cb).thenError(error);
}

std::vector<VertexID> FindPathExecutor::getNeighborIdsFromResps(
    storage::StorageRpcResponse<storage::cpp2::QueryResponse> &result) const {
    size_t num = 0;
    auto &resps = result.responses();
    for (const auto &resp : resps) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        num += resp.get_vertices()->size();
    }

    std::unordered_set<VertexID> set;
    set.reserve(num);

    for (const auto &resp : resps) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        for (const auto &vdata : resp.vertices) {
            for (const auto &edata : vdata.edge_data) {
                for (const auto& edge : edata.get_edges()) {
                    auto dst = edge.get_dst();
                    set.emplace(dst);
                }
            }
        }
    }
    return std::vector<VertexID>(set.begin(), set.end());
}

void FindPathExecutor::doWithFilter(
    storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&result,
    bool isToSide) {
    Frontiers frontiers;
    auto status = doFilter(std::move(result), frontiers, isToSide);
    if (!isToSide) {
        if (!status.ok()) {
            fStatus_ = std::move(status);
            fPro_->setValue();
            return;
        }
        fromFrontiers_ = std::make_pair(VisitedBy::FROM, std::move(frontiers));
        fPro_->setValue();
    } else {
        if (!status.ok()) {
            tStatus_ = std::move(status);
            tPro_->setValue();
            return;
        }
        toFrontiers_ = std::make_pair(VisitedBy::TO, std::move(frontiers));
        tPro_->setValue();
    }
}

Status FindPathExecutor::doFilter(
    storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&result,
    Frontiers &frontiers,
    bool isToSide) {

    auto spaceId = ectx()->rctx()->session()->space();
    VertexID srcId = 0;
    VertexID dstId = 0;
    EdgeType edgeType = 0;
    RowReader reader = RowReader::getEmptyRowReader();
    std::unordered_map<TagID, std::shared_ptr<ResultSchemaProvider>> tagSchema;
    std::unordered_map<EdgeType, std::shared_ptr<ResultSchemaProvider>> edgeSchema;
    const std::vector< ::nebula::storage::cpp2::TagData>* tagData = nullptr;

    std::function<OptVariantType(const std::string&, const std::string&)> getSrcTagProp =
        [&spaceId, &tagData, &tagSchema, this] (
            const std::string &tag, const std::string &prop) -> OptVariantType {
        TagID tagId;
        auto found = expCtx_->getTagId(tag, tagId);
        if (!found) {
            return Status::Error(
                "Get tag id for `%s' failed in getters.", tag.c_str());
        }

        auto it2 = std::find_if(tagData->cbegin(),
        tagData->cend(),
        [&tagId] (auto &td) {
            if (td.tag_id == tagId) {
                return true;
            }
            return false;
        });
        if (it2 == tagData->cend()) {
            auto ts = ectx()->schemaManager()->getTagSchema(spaceId, tagId);
            if (ts == nullptr) {
                return Status::Error("No tag schema for %s", tag.c_str());
            }
            return RowReader::getDefaultProp(ts.get(), prop);
        }
        DCHECK(it2->__isset.data);
        auto vreader = RowReader::getRowReader(it2->data, tagSchema[tagId]);
        auto res = RowReader::getPropByName(vreader.get(), prop);
        if (!ok(res)) {
            return Status::Error("get prop(%s.%s) failed",
                                 tag.c_str(),
                                 prop.c_str());
        }
        return value(res);
    };

    std::function<OptVariantType(const std::string&, const std::string&)> getDstTagProp =
        [&spaceId, &dstId, isToSide, this] (
            const std::string &tag, const std::string &prop) -> OptVariantType {
        TagID tagId;
        auto found = expCtx_->getTagId(tag, tagId);
        if (!found) {
            return Status::Error(
                "Get tag id for `%s' failed in getters.", tag.c_str());
        }
        OptVariantType ret;
        if (!isToSide) {
            ret = fromVertexHolder_->get(dstId, tagId, prop);
        } else {
            ret = toVertexHolder_->get(dstId, tagId, prop);
        }
        if (!ret.ok()) {
            auto ts = ectx()->schemaManager()->getTagSchema(spaceId, tagId);
            if (ts == nullptr) {
                return Status::Error("No tag schema for %s", tag.c_str());
            }
            return RowReader::getDefaultProp(ts.get(), prop);
        }
        return ret;
    };

    Getters getters;
    // In isToSide mode, it is used to get the dst props.
    getters.getSrcTagProp = isToSide ? getDstTagProp : getSrcTagProp;
    // In isToSide mode, it is used to get the src props.
    getters.getDstTagProp = isToSide ? getSrcTagProp : getDstTagProp;
    // In isToSide mode, we should handle _src
    getters.getAliasProp = [&reader, &srcId, &edgeType, &edgeSchema, this] (
        const std::string &edgeName, const std::string &prop) mutable -> OptVariantType {
        EdgeType type;
        auto found = expCtx_->getEdgeType(edgeName, type);
        if (!found) {
            return Status::Error(
                "Get edge type for `%s' failed in getters.",
                edgeName.c_str());
        }
        VLOG(2) << "getAliasProp edgeName: " << edgeName << "prop: " << prop
                << " edgeType: " << edgeType << " type: " << type;
        if (std::abs(edgeType) != type) {
            auto sit = edgeSchema.find(type);
            if (sit == edgeSchema.end()) {
                sit = edgeSchema.find(-type);
            }
            if (sit == edgeSchema.end()) {
                std::string errMsg = folly::stringPrintf(
                    "Can't find shcema for %s when get default.",
                    edgeName.c_str());
                LOG(ERROR) << errMsg;
                return Status::Error(errMsg);
            }
            return RowReader::getDefaultProp(sit->second.get(), prop);
        }

        if (prop == _SRC) {
            return srcId;
        }
        DCHECK(reader != nullptr);
        auto res = RowReader::getPropByName(reader.get(), prop);
        if (!ok(res)) {
            LOG(ERROR) << "Can't get prop for " << prop
                       << ", edge " << edgeName;
            return Status::Error(
                folly::sformat("get prop({}.{}) failed",
                               edgeName,
                               prop));
        }
        return value(std::move(res));
    };  // getAliasProp

    auto &resps = result.responses();
    for (auto &resp : resps) {
        if (resp.get_vertices() == nullptr) {
            continue;
        }
        auto *vschema = resp.get_vertex_schema();
        if (vschema != nullptr) {
            std::transform(vschema->cbegin(), vschema->cend(),
                           std::inserter(tagSchema, tagSchema.begin()), [](auto &schema) {
                    return std::make_pair(
                        schema.first,
                        std::make_shared<ResultSchemaProvider>(schema.second));
                });
        }
        auto *eschema = resp.get_edge_schema();
        if (eschema != nullptr) {
            std::transform(eschema->cbegin(), eschema->cend(),
                           std::inserter(edgeSchema, edgeSchema.begin()), [](auto &schema) {
                    return std::make_pair(
                        schema.first,
                        std::make_shared<ResultSchemaProvider>(schema.second));
                });
        }

        if (edgeSchema.empty()) {
            continue;
        }

        std::vector<VariantType> temps;
        temps.reserve(kReserveProps_.size());
        for (auto &vdata : resp.vertices) {
            DCHECK(vdata.__isset.edge_data);
            tagData = &vdata.get_tag_data();
            srcId = vdata.get_vertex_id();
            for (auto &edata : vdata.edge_data) {
                edgeType = edata.type;
                auto it = edgeSchema.find(edgeType);
                DCHECK(it != edgeSchema.end());
                Neighbors neighbors;
                for (auto& edge : edata.get_edges()) {
                    dstId = edge.get_dst();
                    reader = RowReader::getRowReader(edge.props, it->second);
                    if (reader == nullptr) {
                        return Status::Error("Can't get row reader!");
                    }
                    temps.clear();
                    for (auto &prop : kReserveProps_) {
                        auto res = RowReader::getPropByName(reader.get(), prop);
                        if (ok(res)) {
                            temps.emplace_back(std::move(value(res)));
                        } else {
                            return Status::Error("get edge prop failed %s", prop.c_str());
                        }
                    }
                    // Evaluate filter
                    if (whereWrapper_->getFilter() != nullptr) {
                        auto value = whereWrapper_->getFilter()->eval(getters);
                        if (!value.ok()) {
                            return std::move(value).status();
                        }
                        if (!Expression::asBool(value.value())) {
                            VLOG(2) << "whereWrapper_ filtered neighbor dst: " << dstId
                                << "eType: " << temps[0] << " rank: " << temps[1];
                            continue;
                        }
                    }
                    Neighbor neighbor(
                        dstId,
                        boost::get<int64_t>(temps[0]),
                        boost::get<int64_t>(temps[1]));
                    neighbors.emplace_back(std::move(neighbor));
                }
                auto frontier = std::make_pair(vdata.get_vertex_id(), std::move(neighbors));
                frontiers.emplace_back(std::move(frontier));
            }  // `edata'
        }  // for `vdata'
    }  // for `resp'
    return Status::OK();
}

StatusOr<std::vector<storage::cpp2::PropDef>>
FindPathExecutor::getStepOutProps(bool isToSide) {
    auto edges = isToSide ? tEdgeTypes_ : fEdgeTypes_;
    std::vector<storage::cpp2::PropDef> props;
    for (auto &e : edges) {
        {
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::EDGE;
            pd.name = "_dst";
            pd.id.set_edge_type(e);
            props.emplace_back(std::move(pd));
        }
        for (auto &prop : kReserveProps_) {
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::EDGE;
            pd.name = prop;
            pd.id.set_edge_type(e);
            props.emplace_back(std::move(pd));
        }
        auto spaceId = ectx()->rctx()->session()->space();
        auto tagProps = isToSide ? expCtx_->dstTagProps() : expCtx_->srcTagProps();
        for (auto &tagProp : tagProps) {
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::SOURCE;
            pd.name = tagProp.second;
            auto status = ectx()->schemaManager()->toTagID(spaceId, tagProp.first);
            if (!status.ok()) {
                return Status::Error("No schema found for '%s'", tagProp.first.c_str());
            }
            auto tagId = status.value();
            pd.id.set_tag_id(tagId);
            props.emplace_back(std::move(pd));
            VLOG(3) << "Need tag props: " << tagProp.first << ", " << tagProp.second;
        }
        auto edgeProps = expCtx_->aliasProps();
        for (auto &pair : edgeProps) {
            auto propName = pair.second;
            if (propName == _SRC || propName == _DST
                || propName == _RANK || propName == _TYPE) {
                continue;
            }
            storage::cpp2::PropDef pd;
            pd.owner = storage::cpp2::PropOwner::EDGE;
            pd.name  = propName;

            EdgeType edgeType;
            if (!expCtx_->getEdgeType(pair.first, edgeType)) {
                continue;
            }
            if (std::abs(edgeType) != std::abs(e)) {
                continue;
            }
            pd.id.set_edge_type(e);
            props.emplace_back(std::move(pd));
        }
    }
    return props;
}

void FindPathExecutor::fetchVertexPropsAndFilter(
    storage::StorageRpcResponse<storage::cpp2::QueryResponse> &&response,
    std::vector<VertexID> ids,
    bool isToSide) {
    auto spaceId = ectx()->rctx()->session()->space();
    auto status = getNeighborProps(isToSide);
    if (!status.ok()) {
        doError(std::move(status).status());
        return;
    }
    auto returns = status.value();
    auto future = ectx()->getStorageClient()->getVertexProps(spaceId, ids, returns);
    auto *runner = ectx()->rctx()->runner();
    auto cb = [this, ectx = ectx(), response, isToSide] (auto &&result) mutable {
        auto completeness = result.completeness();
        if (completeness == 0) {
            doError(Status::Error("Get dest props failed"));
            return;
        } else if (completeness != 100) {
            LOG(INFO) << "Get neighbors partially failed: "  << completeness << "%";
            for (auto &error : result.failedParts()) {
                LOG(ERROR) << "part: " << error.first
                           << "error code: " << static_cast<int>(error.second);
            }
        }
        if (!isToSide) {
            fromVertexHolder_ = std::make_unique<VertexHolder>(ectx);
            fromVertexHolder_->add(result.responses());
        } else {
            toVertexHolder_ = std::make_unique<VertexHolder>(ectx);
            toVertexHolder_->add(result.responses());
        }
        doWithFilter(std::move(response), isToSide);
        return;
    };
    auto error = [this] (auto &&e) {
        LOG(ERROR) << "Exception when get vertex in findPath: " << e.what();
        doError(Status::Error("Exception when get vertex in findPath: %s.",
                              e.what().c_str()));
    };
    std::move(future).via(runner).thenValue(cb).thenError(error);
}

StatusOr<std::vector<storage::cpp2::PropDef>> FindPathExecutor::getNeighborProps(bool isToSide) {
    std::vector<storage::cpp2::PropDef> props;
    auto spaceId = ectx()->rctx()->session()->space();
    auto tagProps = isToSide ? expCtx_->srcTagProps() : expCtx_->dstTagProps();
    for (auto &tagProp : tagProps) {
        storage::cpp2::PropDef pd;
        pd.owner = storage::cpp2::PropOwner::DEST;
        pd.name = tagProp.second;
        auto status = ectx()->schemaManager()->toTagID(spaceId, tagProp.first);
        if (!status.ok()) {
            return Status::Error("No schema found for '%s'", tagProp.first.c_str());
        }
        auto tagId = status.value();
        pd.id.set_tag_id(tagId);
        props.emplace_back(std::move(pd));
        VLOG(3) << "Need dst tag props: " << tagProp.first << ", " << tagProp.second;
    }
    return props;
}

OptVariantType FindPathExecutor::VertexHolder::getDefaultProp(
    TagID tagId, const std::string &prop) const {
    auto iter = tagSchemaMap_.find(tagId);
    if (iter == tagSchemaMap_.end()) {
        auto space = ectx_->rctx()->session()->space();
        auto schema = ectx_->schemaManager()->getTagSchema(space, tagId);
        if (schema == nullptr) {
            return Status::Error("No tag schema for tagId %d", tagId);
        }
        tagSchemaMap_.emplace(tagId, schema);
        return RowReader::getDefaultProp(schema.get(), prop);
    }
    return RowReader::getDefaultProp(iter->second.get(), prop);
}

OptVariantType FindPathExecutor::VertexHolder::get(
    VertexID id, TagID tid, const std::string &prop) const {
    auto iter = data_.find(std::make_pair(id, tid));
    if (iter == data_.end()) {
        return getDefaultProp(tid, prop);
    }
    auto &reader = iter->second;
    auto res = RowReader::getPropByName(reader.get(), prop);
    if (!ok(res)) {
        return Status::Error(folly::sformat("get prop({}) failed", prop));
    }
    return value(std::move(res));
}

void FindPathExecutor::VertexHolder::add(
    const std::vector<storage::cpp2::QueryResponse> &responses) {
    size_t num = 0;
    for (auto& resp : responses) {
        if (!resp.__isset.vertices || !resp.__isset.vertex_schema) {
            continue;
        }
        for (auto &vdata : resp.vertices) {
            num += vdata.tag_data.size();
        }
    }
    data_.reserve(num);

    for (auto& resp : responses) {
        if (!resp.__isset.vertices || !resp.__isset.vertex_schema) {
            continue;
        }

        std::transform(
            resp.vertex_schema.cbegin(),
            resp.vertex_schema.cend(),
            std::inserter(
                tagSchemaMap_,
                tagSchemaMap_.begin()),
            [] (auto &s) {
                return std::make_pair(
                    s.first,
                    std::make_shared<ResultSchemaProvider>(s.second));
            });

        for (auto &vdata : resp.vertices) {
            auto vid = vdata.get_vertex_id();
            for (auto &td : vdata.tag_data) {
                DCHECK(td.__isset.data);
                auto tagId = td.tag_id;
                auto it = tagSchemaMap_.find(tagId);
                DCHECK(it != tagSchemaMap_.end());
                auto &vschema = it->second;
                auto vreader = RowReader::getRowReader(td.data, vschema);
                data_.emplace(std::make_pair(vid, tagId), std::move(vreader));
            }
        }
    }
}

std::string FindPathExecutor::buildPathString(const Path &path) {
    std::string pathStr;
    auto iter = path.begin();
    for (; iter != path.end(); ++iter) {
        auto *step = *iter;
        auto id = std::get<0>(*step);
        auto type = std::get<1>(*step);
        auto ranking = std::get<2>(*step);
        pathStr += folly::stringPrintf("%ld<%d,%ld>", id, type, ranking);
    }
    return pathStr;
}

cpp2::RowValue FindPathExecutor::buildPathRow(const Path &path) {
    cpp2::RowValue rowValue;
    std::vector<cpp2::ColumnValue> row;
    cpp2::Path pathValue;
    auto entryList = pathValue.get_entry_list();
    auto iter = path.begin();
    auto turningPointIndex = path.size() / 2 + path.size() % 2;
    std::unordered_set<VertexID> pathVertexIDs;  // stores VertexIDs of current path
    uint64_t iterIndex = 0;
    for (; iter != path.end(); ++iter) {
        auto *step = *iter;
        auto id = std::get<0>(*step);
        auto type = std::get<1>(*step);
        auto ranking = std::get<2>(*step);
        // avoid path loop when NOLOOP
        if (noLoop_) {
            // if id is already exists in pathVertexSet, we found a loop path
            if (pathVertexIDs.count(id) == 1) {
                return rowValue;
            }
            pathVertexIDs.emplace(id);
        }

        entryList.emplace_back();
        cpp2::Vertex vertex;
        vertex.set_id(id);
        entryList.back().set_vertex(std::move(vertex));
        if (oneWayTraverse_) {
            if (type == 0 && ranking == 0L) {
                ++iter;
                break;
            }
        } else if (++iterIndex == turningPointIndex) {
            ++iter;
            break;
        }

        entryList.emplace_back();
        cpp2::Edge edge;
        auto typeName = edgeTypeNameMap_.find(type >= 0 ? type : -type);
        DCHECK(typeName != edgeTypeNameMap_.end()) << type;
        edge.set_type(type >= 0 ? typeName->second : (NEGATIVE_STR + typeName->second));
        edge.set_ranking(ranking);
        entryList.back().set_edge(std::move(edge));
    }

    // when oneWayTraverse_ is TRUE, iter must be NULL
    for (; iter != path.end(); ++iter) {
        auto *step = *iter;
        auto id = std::get<0>(*step);
        auto type = std::get<1>(*step);
        auto ranking = std::get<2>(*step);
        type = -type;
        // avoid path loop when NOLOOP
        if (noLoop_) {
            // if id is already exists in pathVertexSet, we found a loop path
            if (pathVertexIDs.count(id) == 1) {
                return rowValue;
            }
            pathVertexIDs.emplace(id);
        }

        entryList.emplace_back();
        cpp2::Edge edge;
        auto typeName = edgeTypeNameMap_.find(type >= 0 ? type : -type);
        DCHECK(typeName != edgeTypeNameMap_.end()) << type;
        edge.set_type(type >= 0 ? typeName->second : (NEGATIVE_STR + typeName->second));
        edge.set_ranking(ranking);
        entryList.back().set_edge(std::move(edge));

        entryList.emplace_back();
        cpp2::Vertex vertex;
        vertex.set_id(id);
        entryList.back().set_vertex(std::move(vertex));
    }

    row.emplace_back();
    pathValue.set_entry_list(std::move(entryList));
    row.back().set_path(std::move(pathValue));
    rowValue.set_columns(std::move(row));
    return rowValue;
}

void FindPathExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    std::vector<cpp2::RowValue> rows;
    for (auto &path : finalPath_) {
        auto row = buildPathRow(path.second);
        // if we meet an empty row, skip it
        if (row.get_columns().empty()) {
            continue;
        }
        rows.emplace_back(std::move(row));
        VLOG(1) << "Path: " << buildPathString(path.second);
    }

    std::vector<std::string> colNames = {"_path_"};
    resp.set_column_names(std::move(colNames));
    resp.set_rows(std::move(rows));
}
}  // namespace graph
}  // namespace nebula
