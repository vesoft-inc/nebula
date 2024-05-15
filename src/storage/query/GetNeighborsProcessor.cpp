/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/query/GetNeighborsProcessor.h"

#include "common/memory/MemoryTracker.h"
#include "storage/StorageFlags.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/GetNeighborsNode.h"
#include "storage/exec/HashJoinNode.h"
#include "storage/exec/MultiTagNode.h"
#include "storage/exec/TagNode.h"

namespace nebula {
namespace storage {

ProcessorCounters kGetNeighborsCounters;

void GetNeighborsProcessor::process(const cpp2::GetNeighborsRequest& req) {
  if (executor_ != nullptr) {
    executor_->add(
        [this, req]() { MemoryCheckScope wrapper(this, [this, req] { this->doProcess(req); }); });
  } else {
    doProcess(req);
  }
}

void GetNeighborsProcessor::doProcess(const cpp2::GetNeighborsRequest& req) {
  spaceId_ = req.get_space_id();
  auto retCode = getSpaceVidLen(spaceId_);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto& p : req.get_parts()) {
      pushResultCode(retCode, p.first);
    }
    onFinished();
    return;
  }
  if (req.common_ref().has_value() && req.get_common()->profile_detail_ref().value_or(false)) {
    profileDetailFlag_ = true;
  }
  this->planContext_ = std::make_unique<PlanContext>(
      this->env_, spaceId_, this->spaceVidLen_, this->isIntId_, req.common_ref());

  // build TagContext and EdgeContext
  retCode = checkAndBuildContexts(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto& p : req.get_parts()) {
      pushResultCode(retCode, p.first);
    }
    onFinished();
    return;
  }

  int64_t limit = FLAGS_max_edge_returned_per_vertex;
  bool random = false;
  if ((*req.traverse_spec_ref()).limit_ref().has_value()) {
    int64_t traverseLimit = *(*req.traverse_spec_ref()).limit_ref();
    if (traverseLimit >= 0 && traverseLimit < limit) {
      limit = traverseLimit;
    }
    if ((*req.traverse_spec_ref()).random_ref().has_value()) {
      random = *(*req.traverse_spec_ref()).random_ref();
    }
  }

  // todo(doodle): specify by each query
  if (!FLAGS_query_concurrently) {
    runInSingleThread(req, limit, random);
  } else {
    runInMultipleThread(req, limit, random);
  }
}

void GetNeighborsProcessor::runInSingleThread(const cpp2::GetNeighborsRequest& req,
                                              int64_t limit,
                                              bool random) {
  memory::MemoryCheckGuard guard;
  contexts_.emplace_back(RuntimeContext(planContext_.get()));
  expCtxs_.emplace_back(StorageExpressionContext(spaceVidLen_, isIntId_));
  auto plan = buildPlan(&contexts_.front(), &expCtxs_.front(), &resultDataSet_, limit, random);
  std::unordered_set<PartitionID> failedParts;
  for (const auto& partEntry : req.get_parts()) {
    contexts_.front().resultStat_ = ResultStatus::NORMAL;
    auto partId = partEntry.first;
    for (const auto& vid : partEntry.second) {
      auto vId = vid.getStr();

      if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
        LOG(INFO) << "Space " << spaceId_ << ", vertex length invalid, "
                  << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
        pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
      }

      // the first column of each row would be the vertex id
      auto ret = plan.go(partId, vId);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (failedParts.find(partId) == failedParts.end()) {
          failedParts.emplace(partId);
          handleErrorCode(ret, spaceId_, partId);
        }
      }
    }
  }
  if (UNLIKELY(profileDetailFlag_)) {
    profilePlan(plan);
  }
  onProcessFinished();
  onFinished();
}

void GetNeighborsProcessor::runInMultipleThread(const cpp2::GetNeighborsRequest& req,
                                                int64_t limit,
                                                bool random) {
  memory::MemoryCheckOffGuard offGuard;
  for (size_t i = 0; i < req.get_parts().size(); i++) {
    nebula::DataSet result = resultDataSet_;
    results_.emplace_back(std::move(result));
    contexts_.emplace_back(RuntimeContext(planContext_.get()));
    expCtxs_.emplace_back(StorageExpressionContext(spaceVidLen_, isIntId_));
  }
  size_t i = 0;
  std::vector<folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>>> futures;
  for (const auto& [partId, vids] : req.get_parts()) {
    futures.emplace_back(
        runInExecutor(&contexts_[i], &expCtxs_[i], &results_[i], partId, vids, limit, random));
    i++;
  }

  folly::collectAll(futures)
      .via(executor_)
      .thenTry([this](auto&& t) mutable {
        memory::MemoryCheckGuard guard;
        CHECK(!t.hasException());
        const auto& tries = t.value();
        size_t sum = 0;
        for (size_t j = 0; j < tries.size(); j++) {
          if (tries[j].hasException()) {
            onError();
            return;
          }
          sum += results_[j].size();
        }
        resultDataSet_.rows.reserve(sum);
        for (size_t j = 0; j < tries.size(); j++) {
          const auto& [code, partId] = tries[j].value();
          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            handleErrorCode(code, spaceId_, partId);
          } else {
            resultDataSet_.append(std::move(results_[j]));
          }
        }
        this->onProcessFinished();
        this->onFinished();
      })
      .thenError(folly::tag_t<std::bad_alloc>{}, [this](const std::bad_alloc&) {
        memoryExceeded_ = true;
        onError();
      });
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> GetNeighborsProcessor::runInExecutor(
    RuntimeContext* context,
    StorageExpressionContext* expCtx,
    nebula::DataSet* result,
    PartitionID partId,
    const std::vector<nebula::Value>& vids,
    int64_t limit,
    bool random) {
  return folly::via(
             executor_,
             [this, context, expCtx, result, partId, input = std::move(vids), limit, random]() {
               memory::MemoryCheckGuard guard;
               if (memoryExceeded_) {
                 return std::make_pair(nebula::cpp2::ErrorCode::E_STORAGE_MEMORY_EXCEEDED, partId);
               }
               auto plan = buildPlan(context, expCtx, result, limit, random);
               for (const auto& vid : input) {
                 auto vId = vid.getStr();

                 if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
                   LOG(INFO) << "Space " << spaceId_ << ", vertex length invalid, "
                             << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
                   return std::make_pair(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
                 }

                 // the first column of each row would be the vertex id
                 auto ret = plan.go(partId, vId);
                 if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                   return std::make_pair(ret, partId);
                 }
               }
               if (UNLIKELY(this->profileDetailFlag_)) {
                 profilePlan(plan);
               }
               return std::make_pair(nebula::cpp2::ErrorCode::SUCCEEDED, partId);
             })
      .thenError(folly::tag_t<std::bad_alloc>{}, [this, partId](const std::bad_alloc&) {
        memoryExceeded_ = true;
        return std::make_pair(nebula::cpp2::ErrorCode::E_STORAGE_MEMORY_EXCEEDED, partId);
      });
}

StoragePlan<VertexID> GetNeighborsProcessor::buildPlan(RuntimeContext* context,
                                                       StorageExpressionContext* expCtx,
                                                       nebula::DataSet* result,
                                                       int64_t limit,
                                                       bool random) {
  /*
  The StoragePlan looks like this:
             +------------------+                      or, if there is no edge:
             | GetNeighborsNode |
             +--------+---------+                            +-----------------+
                      |                                      |GetNeighborsNode |
             +--------+---------+                            +--------+--------+
             |   AggregateNode  |                                     |
             +--------+---------+                              +------+------+
                      |                                        |AggregateNode|
             +--------+---------+                              +------+------+
             |    FilterNode    |                                     |
             +--------+---------+                               +-----+----+
                      |                                         |FilterNode|
             +--------+---------+                               +-----+----+
         +-->+   HashJoinNode   +<----+                               |
         |   +------------------+     |                        +------+-----+
+--------+---------+        +---------+--------+               |HashJoinNode|
|     TagNodes     |        |     EdgeNodes    |               +------+-----+
+------------------+        +------------------+                      |
                                                               +------+-----+
                                                               |MultiTagNode|
                                                               +------+-----+
                                                                      |
                                                                 +----+---+
                                                                 |TagNodes|
                                                                 +--------+

  */
  StoragePlan<VertexID> plan;
  std::vector<TagNode*> tags;
  for (const auto& tc : tagContext_.propContexts_) {
    auto tag = std::make_unique<TagNode>(context, &tagContext_, tc.first, &tc.second);
    tags.emplace_back(tag.get());
    plan.addNode(std::move(tag));
  }
  std::vector<SingleEdgeNode*> edges;
  for (const auto& ec : edgeContext_.propContexts_) {
    auto edge = std::make_unique<SingleEdgeNode>(context, &edgeContext_, ec.first, &ec.second);
    edges.emplace_back(edge.get());
    plan.addNode(std::move(edge));
  }

  IterateNode<VertexID>* upstream = nullptr;
  IterateNode<VertexID>* join = nullptr;
  if (!edges.empty()) {
    auto hashJoin =
        std::make_unique<HashJoinNode>(context, tags, edges, &tagContext_, &edgeContext_, expCtx);
    for (auto* tag : tags) {
      hashJoin->addDependency(tag);
    }
    for (auto* edge : edges) {
      hashJoin->addDependency(edge);
    }
    join = hashJoin.get();
    upstream = hashJoin.get();
    plan.addNode(std::move(hashJoin));
  } else {
    context->filterInvalidResultOut = true;
    auto groupNode = std::make_unique<MultiTagNode>(context, tags, expCtx);
    for (auto* tag : tags) {
      groupNode->addDependency(tag);
    }
    join = groupNode.get();
    upstream = groupNode.get();
    plan.addNode(std::move(groupNode));
  }

  if (filter_) {
    auto filter = std::make_unique<FilterNode<VertexID>>(
        context, upstream, expCtx, filter_->clone(), tagFilter_ ? tagFilter_->clone() : nullptr);
    filter->addDependency(upstream);
    upstream = filter.get();
    if (edges.empty()) {
      filter->setFilterMode(FilterMode::TAG_ONLY);
    }
    plan.addNode(std::move(filter));
  }

  if (edgeContext_.statCount_ > 0) {
    auto agg = std::make_unique<AggregateNode<VertexID>>(context, upstream, &edgeContext_);
    agg->addDependency(upstream);
    upstream = agg.get();
    plan.addNode(std::move(agg));
  }

  std::unique_ptr<GetNeighborsNode> output;
  if (random) {
    output = std::make_unique<GetNeighborsSampleNode>(
        context, join, upstream, &edgeContext_, result, limit);
  } else {
    output =
        std::make_unique<GetNeighborsNode>(context, join, upstream, &edgeContext_, result, limit);
  }
  output->addDependency(upstream);
  plan.addNode(std::move(output));

  return plan;
}

nebula::cpp2::ErrorCode GetNeighborsProcessor::checkAndBuildContexts(
    const cpp2::GetNeighborsRequest& req) {
  resultDataSet_.colNames.emplace_back(kVid);
  // reserve second colname for stat
  resultDataSet_.colNames.emplace_back("_stats");

  auto code = getSpaceVertexSchema();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  code = getSpaceEdgeSchema();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  code = buildTagContext(req.get_traverse_spec());
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  code = buildEdgeContext(req.get_traverse_spec());
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  code = buildYields(req);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  code =
      buildFilter(req, [](const cpp2::GetNeighborsRequest& r, bool onlyTag) -> const std::string* {
        if (onlyTag) {
          return r.get_traverse_spec().tag_filter_ref().has_value()
                     ? r.get_traverse_spec().get_tag_filter()
                     : nullptr;
        }
        return r.get_traverse_spec().filter_ref().has_value() ? r.get_traverse_spec().get_filter()
                                                              : nullptr;
      });
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode GetNeighborsProcessor::buildTagContext(const cpp2::TraverseSpec& req) {
  if (!req.vertex_props_ref().has_value()) {
    // If the list is not given, no prop will be returned.
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  auto returnProps = *req.vertex_props_ref();
  auto ret = handleVertexProps(returnProps);

  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }
  buildTagColName(std::move(returnProps));
  buildTagTTLInfo();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode GetNeighborsProcessor::buildEdgeContext(const cpp2::TraverseSpec& req) {
  edgeContext_.offset_ = tagContext_.propContexts_.size() + 2;
  if (!req.edge_props_ref().has_value()) {
    // If the list is not given, no prop will be returned.
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  auto returnProps = *req.edge_props_ref();
  auto ret = handleEdgeProps(returnProps);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }
  if (req.stat_props_ref().has_value()) {
    ret = handleEdgeStatProps(*req.stat_props_ref());
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
  }
  buildEdgeColName(std::move(returnProps));
  buildEdgeTTLInfo();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void GetNeighborsProcessor::buildTagColName(const std::vector<cpp2::VertexProp>& tagProps) {
  for (const auto& tagProp : tagProps) {
    auto tagId = tagProp.get_tag();
    auto tagName = tagContext_.tagNames_[tagId];
    std::string colName = "_tag:" + tagName;
    for (const auto& prop : *tagProp.props_ref()) {
      colName += ":" + std::move(prop);
    }
    VLOG(1) << "append col name: " << colName;
    resultDataSet_.colNames.emplace_back(std::move(colName));
  }
}

void GetNeighborsProcessor::buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps) {
  for (const auto& edgeProp : edgeProps) {
    auto edgeType = edgeProp.get_type();
    auto edgeName = edgeContext_.edgeNames_[edgeType];
    std::string colName = "_edge:";
    colName.append(edgeType > 0 ? "+" : "-").append(edgeName);
    for (const auto& prop : *edgeProp.props_ref()) {
      colName += ":" + std::move(prop);
    }
    VLOG(1) << "append col name: " << colName;
    resultDataSet_.colNames.emplace_back(std::move(colName));
  }
}

nebula::cpp2::ErrorCode GetNeighborsProcessor::handleEdgeStatProps(
    const std::vector<cpp2::StatProp>& statProps) {
  edgeContext_.statCount_ = statProps.size();
  std::string colName = "_stats";
  auto pool = &this->planContext_->objPool_;

  for (size_t statIdx = 0; statIdx < statProps.size(); statIdx++) {
    const auto& statProp = statProps[statIdx];
    auto exp = Expression::decode(pool, *statProp.prop_ref());
    if (exp == nullptr) {
      return nebula::cpp2::ErrorCode::E_INVALID_STAT_TYPE;
    }

    // we only support edge property/rank expression for now
    switch (exp->kind()) {
      case Expression::Kind::kEdgeRank:
      case Expression::Kind::kEdgeProperty: {
        auto* edgeExp = static_cast<const PropertyExpression*>(exp);
        const auto& edgeName = edgeExp->sym();
        const auto& propName = edgeExp->prop();
        auto edgeRet = this->env_->schemaMan_->toEdgeType(spaceId_, edgeName);
        if (!edgeRet.ok()) {
          VLOG(1) << "Can't find edge " << edgeName << ", in space " << spaceId_;
          return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        auto edgeType = edgeRet.value();
        auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
        if (iter == edgeContext_.schemas_.end()) {
          VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << std::abs(edgeType);
          return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& edgeSchema = iter->second.back();

        const meta::NebulaSchemaProvider::SchemaField* field = nullptr;
        if (exp->kind() == Expression::Kind::kEdgeProperty) {
          field = edgeSchema->field(propName);
          if (field == nullptr) {
            VLOG(1) << "Can't find related prop " << propName << " on edge " << edgeName;
            return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
          }
          auto ret = checkStatType(*field, statProp.get_stat());
          if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
          }
        }
        auto statInfo = std::make_pair(statIdx, statProp.get_stat());
        addPropContextIfNotExists(edgeContext_.propContexts_,
                                  edgeContext_.indexMap_,
                                  edgeContext_.edgeNames_,
                                  edgeType,
                                  edgeName,
                                  propName,
                                  field,
                                  false,
                                  false,
                                  &statInfo);
        break;
      }
      default: {
        return nebula::cpp2::ErrorCode::E_INVALID_STAT_TYPE;
      }
    }
    colName += ":" + std::move(statProp.get_alias());
  }
  resultDataSet_.colNames[1] = std::move(colName);

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void GetNeighborsProcessor::onProcessFinished() {
  resp_.vertices_ref() = std::move(resultDataSet_);
}

}  // namespace storage
}  // namespace nebula
