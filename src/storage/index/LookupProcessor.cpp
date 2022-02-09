/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/index/LookupProcessor.h"

#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "folly/Likely.h"
#include "interface/gen-cpp2/common_types.tcc"
#include "interface/gen-cpp2/meta_types.tcc"
#include "interface/gen-cpp2/storage_types.tcc"
#include "storage/exec/IndexAggregateNode.h"
#include "storage/exec/IndexDedupNode.h"
#include "storage/exec/IndexEdgeScanNode.h"
#include "storage/exec/IndexLimitNode.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexProjectionNode.h"
#include "storage/exec/IndexSelectionNode.h"
#include "storage/exec/IndexTopNNode.h"
#include "storage/exec/IndexVertexScanNode.h"
namespace nebula {
namespace storage {
ProcessorCounters kLookupCounters;
// print Plan for debug
inline void printPlan(IndexNode* node, int tab = 0);
void LookupProcessor::process(const cpp2::LookupIndexRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}

void LookupProcessor::doProcess(const cpp2::LookupIndexRequest& req) {
  if (req.common_ref().has_value() && req.get_common()->profile_detail_ref().value_or(false)) {
    profileDetailFlag_ = true;
  }
  auto code = prepare(req);
  if (UNLIKELY(code != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    for (auto& p : req.get_parts()) {
      pushResultCode(code, p);
    }
    onFinished();
    return;
  }
  auto planRet = buildPlan(req);
  if (UNLIKELY(!nebula::ok(planRet))) {
    for (auto& p : req.get_parts()) {
      pushResultCode(nebula::error(planRet), p);
    }
    onFinished();
    return;
  }

  auto plan = std::move(nebula::value(planRet));

  if (UNLIKELY(profileDetailFlag_)) {
    plan->enableProfileDetail();
  }
  InitContext ctx;
  code = plan->init(ctx);
  if (UNLIKELY(code != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    for (auto& p : req.get_parts()) {
      pushResultCode(code, p);
    }
    onFinished();
    return;
  }
  if (!FLAGS_query_concurrently) {
    runInSingleThread(req.get_parts(), std::move(plan));
  } else {
    runInMultipleThread(req.get_parts(), std::move(plan));
  }
}
::nebula::cpp2::ErrorCode LookupProcessor::prepare(const cpp2::LookupIndexRequest& req) {
  auto retCode = this->getSpaceVidLen(req.get_space_id());
  if (UNLIKELY(retCode != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return retCode;
  }
  planContext_ = std::make_unique<PlanContext>(
      this->env_, req.get_space_id(), this->spaceVidLen_, this->isIntId_, req.common_ref());
  planContext_->isEdge_ =
      req.get_indices().get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type;
  context_ = std::make_unique<RuntimeContext>(this->planContext_.get());
  std::string schemaName;
  if (planContext_->isEdge_) {
    auto edgeType = req.get_indices().get_schema_id().get_edge_type();
    auto schemaNameValue = env_->schemaMan_->toEdgeName(req.get_space_id(), edgeType);
    if (!schemaNameValue.ok()) {
      return ::nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    schemaName = schemaNameValue.value();
    context_->edgeType_ = edgeType;
    context_->edgeName_ = schemaName;
  } else {
    auto tagId = req.get_indices().get_schema_id().get_tag_id();
    auto schemaNameValue = env_->schemaMan_->toTagName(req.get_space_id(), tagId);
    if (!schemaNameValue.ok()) {
      return ::nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
    }
    schemaName = schemaNameValue.value();
    context_->tagId_ = tagId;
    context_->tagName_ = schemaName;
  }
  std::vector<std::string> colNames;
  for (auto& col : *req.get_return_columns()) {
    colNames.emplace_back(schemaName + "." + col);
  }
  resultDataSet_ = ::nebula::DataSet(colNames);
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<IndexNode>> LookupProcessor::buildPlan(
    const cpp2::LookupIndexRequest& req) {
  std::vector<std::unique_ptr<IndexNode>> nodes;
  for (auto& ctx : req.get_indices().get_contexts()) {
    auto node = buildOneContext(ctx);
    nodes.emplace_back(std::move(node));
  }
  for (size_t i = 0; i < nodes.size(); i++) {
    auto projection =
        std::make_unique<IndexProjectionNode>(context_.get(), *req.get_return_columns());
    projection->addChild(std::move(nodes[i]));
    nodes[i] = std::move(projection);
  }
  if (nodes.size() > 1) {
    std::vector<std::string> dedupColumn;
    if (context_->isEdge()) {
      dedupColumn = std::vector<std::string>{kSrc, kRank, kDst};
    } else {
      dedupColumn = std::vector<std::string>{kVid};
    }
    auto dedup = std::make_unique<IndexDedupNode>(context_.get(), dedupColumn);
    for (auto& node : nodes) {
      dedup->addChild(std::move(node));
    }
    nodes.clear();
    nodes[0] = std::move(dedup);
  }
  if (req.limit_ref().has_value()) {
    auto limit = *req.get_limit();
    if (req.order_by_ref().has_value() && req.get_order_by()->size() > 0) {
      auto node = std::make_unique<IndexTopNNode>(context_.get(), limit, req.get_order_by());
      node->addChild(std::move(nodes[0]));
      nodes[0] = std::move(node);
    } else {
      auto node = std::make_unique<IndexLimitNode>(context_.get(), limit);
      node->addChild(std::move(nodes[0]));
      nodes[0] = std::move(node);
    }
  }
  if (req.stat_columns_ref().has_value()) {
    auto statRet = handleStatProps(*req.get_stat_columns());
    if (!nebula::ok(statRet)) {
      return nebula::error(statRet);
    }
    auto node = std::make_unique<IndexAggregateNode>(
        context_.get(), nebula::value(statRet), req.get_return_columns()->size());
    node->addChild(std::move(nodes[0]));
    nodes[0] = std::move(node);
  }
  return std::move(nodes[0]);
}

std::unique_ptr<IndexNode> LookupProcessor::buildOneContext(const cpp2::IndexQueryContext& ctx) {
  std::unique_ptr<IndexNode> node;
  DLOG(INFO) << ctx.get_column_hints().size();
  DLOG(INFO) << &ctx.get_column_hints();
  DLOG(INFO) << ::apache::thrift::SimpleJSONSerializer::serialize<std::string>(ctx);
  if (context_->isEdge()) {
    node = std::make_unique<IndexEdgeScanNode>(
        context_.get(), ctx.get_index_id(), ctx.get_column_hints(), context_->env()->kvstore_);
  } else {
    node = std::make_unique<IndexVertexScanNode>(
        context_.get(), ctx.get_index_id(), ctx.get_column_hints(), context_->env()->kvstore_);
  }
  if (ctx.filter_ref().is_set() && !ctx.get_filter().empty()) {
    auto expr = Expression::decode(context_->objPool(), *ctx.filter_ref());
    auto filterNode = std::make_unique<IndexSelectionNode>(context_.get(), expr);
    filterNode->addChild(std::move(node));
    node = std::move(filterNode);
  }
  return node;
}

void LookupProcessor::runInSingleThread(const std::vector<PartitionID>& parts,
                                        std::unique_ptr<IndexNode> plan) {
  // printPlan(plan.get());
  std::vector<std::deque<Row>> datasetList;
  std::vector<::nebula::cpp2::ErrorCode> codeList;
  for (auto part : parts) {
    DLOG(INFO) << "execute part:" << part;
    plan->execute(part);
    ::nebula::cpp2::ErrorCode code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    decltype(datasetList)::value_type dataset;
    do {
      auto result = plan->next();
      if (!result.success()) {
        code = result.code();
        break;
      }
      if (result.hasData()) {
        dataset.emplace_back(std::move(result).row());
      } else {
        break;
      }
    } while (true);
    datasetList.emplace_back(std::move(dataset));
    codeList.emplace_back(code);
  }
  if (statTypes_.size() > 0) {
    auto indexAgg = dynamic_cast<IndexAggregateNode*>(plan.get());
    statsDataSet_.emplace_back(indexAgg->calculateStats());
  }
  for (size_t i = 0; i < datasetList.size(); i++) {
    if (codeList[i] == ::nebula::cpp2::ErrorCode::SUCCEEDED) {
      while (!datasetList[i].empty()) {
        resultDataSet_.emplace_back(std::move(datasetList[i].front()));
        datasetList[i].pop_front();
      }
    } else {
      DLOG(INFO) << int(codeList[i]);
      handleErrorCode(codeList[i], context_->spaceId(), parts[i]);
    }
  }
  if (UNLIKELY(profileDetailFlag_)) {
    profilePlan(plan.get());
  }
  onProcessFinished();
  onFinished();
}

void LookupProcessor::runInMultipleThread(const std::vector<PartitionID>& parts,
                                          std::unique_ptr<IndexNode> plan) {
  std::vector<std::unique_ptr<IndexNode>> planCopy = reproducePlan(plan.get(), parts.size());
  using ReturnType = std::tuple<PartitionID, ::nebula::cpp2::ErrorCode, std::deque<Row>, Row>;
  std::vector<folly::Future<ReturnType>> futures;
  for (size_t i = 0; i < parts.size(); i++) {
    futures.emplace_back(folly::via(
        executor_, [this, plan = std::move(planCopy[i]), part = parts[i]]() -> ReturnType {
          ::nebula::cpp2::ErrorCode code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
          std::deque<Row> dataset;
          plan->execute(part);
          do {
            auto result = plan->next();
            if (!result.success()) {
              code = result.code();
              break;
            }
            if (result.hasData()) {
              dataset.emplace_back(std::move(result).row());
            } else {
              break;
            }
          } while (true);
          if (UNLIKELY(profileDetailFlag_)) {
            profilePlan(plan.get());
          }
          Row statResult;
          if (code == nebula::cpp2::ErrorCode::SUCCEEDED && statTypes_.size() > 0) {
            auto indexAgg = dynamic_cast<IndexAggregateNode*>(plan.get());
            statResult = indexAgg->calculateStats();
          }
          return {part, code, dataset, statResult};
        }));
  }
  folly::collectAll(futures).via(executor_).thenTry([this](auto&& t) {
    CHECK(!t.hasException());
    const auto& tries = t.value();
    std::vector<Row> statResults;
    for (size_t j = 0; j < tries.size(); j++) {
      CHECK(!tries[j].hasException());
      auto& [partId, code, dataset, statResult] = tries[j].value();
      if (code == ::nebula::cpp2::ErrorCode::SUCCEEDED) {
        for (auto& row : dataset) {
          resultDataSet_.emplace_back(std::move(row));
        }
      } else {
        handleErrorCode(code, context_->spaceId(), partId);
      }
      statResults.emplace_back(std::move(statResult));
    }
    DLOG(INFO) << "finish";
    // IndexAggregateNode has been copyed and each part get it's own aggregate info,
    // we need to merge it
    this->mergeStatsResult(statResults);
    this->onProcessFinished();
    this->onFinished();
  });
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::pair<std::string, cpp2::StatType>>>
LookupProcessor::handleStatProps(const std::vector<cpp2::StatProp>& statProps) {
  auto pool = &this->planContext_->objPool_;
  std::vector<std::pair<std::string, cpp2::StatType>> statInfos;
  std::vector<std::string> colNames;

  for (size_t statIdx = 0; statIdx < statProps.size(); statIdx++) {
    const auto& statProp = statProps[statIdx];
    statTypes_.emplace_back(statProp.get_stat());
    auto exp = Expression::decode(pool, *statProp.prop_ref());
    if (exp == nullptr) {
      return nebula::cpp2::ErrorCode::E_INVALID_STAT_TYPE;
    }

    // only support vertex property and edge property/rank expression for now
    switch (exp->kind()) {
      case Expression::Kind::kEdgeRank:
      case Expression::Kind::kEdgeProperty: {
        auto* edgeExp = static_cast<const PropertyExpression*>(exp);
        const auto& edgeName = edgeExp->sym();
        const auto& propName = edgeExp->prop();
        if (edgeName != context_->edgeName_) {
          return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        if (exp->kind() == Expression::Kind::kEdgeProperty && propName != kSrc &&
            propName != kDst) {
          auto edgeSchema =
              env_->schemaMan_->getEdgeSchema(context_->spaceId(), context_->edgeType_);
          auto field = edgeSchema->field(propName);
          if (field == nullptr) {
            VLOG(1) << "Can't find related prop " << propName << " on edge " << edgeName;
            return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
          }
          auto ret = checkStatType(*field, statProp.get_stat());
          if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
          }
        }
        statInfos.emplace_back(propName, statProp.get_stat());
        break;
      }
      case Expression::Kind::kTagProperty: {
        auto* tagExp = static_cast<const PropertyExpression*>(exp);
        const auto& tagName = tagExp->sym();
        const auto& propName = tagExp->prop();

        if (tagName != context_->tagName_) {
          return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        if (propName != kVid && propName != kTag) {
          auto tagSchema = env_->schemaMan_->getTagSchema(context_->spaceId(), context_->tagId_);
          auto field = tagSchema->field(propName);
          if (field == nullptr) {
            VLOG(1) << "Can't find related prop " << propName << "on tag " << tagName;
            return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
          }
        }
        statInfos.emplace_back(propName, statProp.get_stat());
        break;
      }
      default: {
        return nebula::cpp2::ErrorCode::E_INVALID_STAT_TYPE;
      }
    }
    colNames.push_back(statProp.get_alias());
  }
  statsDataSet_ = nebula::DataSet(colNames);
  return statInfos;
}

void LookupProcessor::mergeStatsResult(const std::vector<Row>& statsResult) {
  if (statsResult.size() == 0 || statTypes_.size() == 0) {
    return;
  }

  Row result;
  for (size_t statIdx = 0; statIdx < statTypes_.size(); statIdx++) {
    Value value = statsResult[0].values[statIdx];
    for (size_t resIdx = 1; resIdx < statsResult.size(); resIdx++) {
      const auto& currType = statTypes_[statIdx];
      if (currType == cpp2::StatType::SUM || currType == cpp2::StatType::COUNT) {
        value = value + statsResult[resIdx].values[statIdx];
      } else if (currType == cpp2::StatType::MAX) {
        value = value > statsResult[resIdx].values[statIdx] ? value
                                                            : statsResult[resIdx].values[statIdx];
      } else if (currType == cpp2::StatType::MIN) {
        value = value < statsResult[resIdx].values[statIdx] ? value
                                                            : statsResult[resIdx].values[statIdx];
      }
    }
    result.values.emplace_back(std::move(value));
  }
  statsDataSet_.emplace_back(std::move(result));
}

std::vector<std::unique_ptr<IndexNode>> LookupProcessor::reproducePlan(IndexNode* root,
                                                                       size_t count) {
  std::vector<std::unique_ptr<IndexNode>> ret(count);
  for (size_t i = 0; i < count; i++) {
    ret[i] = root->copy();
    DLOG(INFO) << ret[i].get();
  }
  for (auto& child : root->children()) {
    auto childPerPlan = reproducePlan(child.get(), count);
    for (size_t i = 0; i < count; i++) {
      ret[i]->addChild(std::move(childPerPlan[i]));
    }
  }
  return ret;
}
void LookupProcessor::profilePlan(IndexNode* root) {
  std::queue<IndexNode*> q;
  q.push(root);
  while (!q.empty()) {
    auto node = q.front();
    q.pop();
    auto id = node->identify();
    auto iter = profileDetail_.find(id);
    if (iter == profileDetail_.end()) {
      profileDetail_[id] = node->duration().elapsedInUSec();
    } else {
      iter->second += node->duration().elapsedInUSec();
    }
    for (auto& child : node->children()) {
      q.push(child.get());
    }
  }
}
inline void printPlan(IndexNode* node, int tab) {
  for (auto& child : node->children()) {
    printPlan(child.get(), tab + 1);
  }
}
}  // namespace storage
}  // namespace nebula
