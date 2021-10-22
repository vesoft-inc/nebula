/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/index/LookupProcessor.h"

#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "folly/Likely.h"
#include "interface/gen-cpp2/common_types.tcc"
#include "interface/gen-cpp2/meta_types.tcc"
#include "interface/gen-cpp2/storage_types.tcc"
#include "storage/exec/IndexDedupNode.h"
#include "storage/exec/IndexEdgeScanNode.h"
#include "storage/exec/IndexLimitNode.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexProjectionNode.h"
#include "storage/exec/IndexSelectionNode.h"
#include "storage/exec/IndexVertexScanNode.h"
namespace nebula {
namespace storage {
ProcessorCounters kLookupCounters;
void LookupProcessor::process(const cpp2::LookupIndexRequest& req) {
  DLOG(INFO) << ::apache::thrift::SimpleJSONSerializer::serialize<std::string>(req);
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}
void printPlan(IndexNode* node, int tab = 0) {
  LOG(INFO) << std::string(tab, '\t') << node->identify();
  for (auto& child : node->children()) {
    printPlan(child.get(), tab + 1);
  }
}
void LookupProcessor::doProcess(const cpp2::LookupIndexRequest& req) {
  if (req.common_ref().has_value() && req.get_common()->profile_detail_ref().value_or(false)) {
    profileDetailFlag_ = true;
  }
  prepare(req);
  auto plan = buildPlan(req);
  printPlan(plan.get());
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
    schemaName = env_->schemaMan_->toEdgeName(req.get_space_id(), edgeType).value();
    context_->edgeType_ = edgeType;
  } else {
    auto tagId = req.get_indices().get_schema_id().get_tag_id();
    schemaName = env_->schemaMan_->toTagName(req.get_space_id(), tagId).value();
    context_->tagId_ = tagId;
  }
  std::vector<std::string> colNames;
  for (auto& col : *req.get_return_columns()) {
    colNames.emplace_back(schemaName + "." + col);
  }
  resultDataSet_ = ::nebula::DataSet(colNames);
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::unique_ptr<IndexNode> LookupProcessor::buildPlan(const cpp2::LookupIndexRequest& req) {
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
    auto node = std::make_unique<IndexLimitNode>(context_.get(), limit);
    node->addChild(std::move(nodes[0]));
    nodes[0] = std::move(node);
  }
  InitContext ctx;
  auto result = nodes[0]->init(ctx);
  if (profileDetailFlag_) {
    nodes[0]->enableProfileDetail();
  }
  // TODO(hs.zhang): check init result
  if (result == ::nebula::cpp2::ErrorCode::SUCCEEDED) {
  } else {
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
  printPlan(plan.get());
  std::vector<std::deque<Row>> datasetList;
  std::vector<::nebula::cpp2::ErrorCode> codeList;
  for (auto part : parts) {
    DLOG(INFO) << "execute part:" << part;
    plan->execute(part);
    bool hasNext = true;
    ::nebula::cpp2::ErrorCode code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
    decltype(datasetList)::value_type dataset;
    do {
      auto result = plan->next(hasNext);
      if (!::nebula::ok(result)) {
        code = ::nebula::error(result);
        break;
      }
      if (hasNext) {
        dataset.emplace_back(::nebula::value(std::move(result)));
      } else {
        break;
      }
    } while (true);
    datasetList.emplace_back(std::move(dataset));
    codeList.emplace_back(code);
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
  using ReturnType = std::tuple<PartitionID, ::nebula::cpp2::ErrorCode, std::deque<Row>>;
  std::vector<folly::Future<ReturnType>> futures;
  for (size_t i = 0; i < parts.size(); i++) {
    futures.emplace_back(folly::via(
        executor_, [this, plan = std::move(planCopy[i]), part = parts[i]]() -> ReturnType {
          ::nebula::cpp2::ErrorCode code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
          std::deque<Row> dataset;
          plan->execute(part);
          bool hasNext = true;
          do {
            auto result = plan->next(hasNext);
            if (!::nebula::ok(result)) {
              code = ::nebula::error(result);
              break;
            }
            if (hasNext) {
              dataset.emplace_back(::nebula::value(std::move(result)));
            } else {
              break;
            }
          } while (true);
          if (UNLIKELY(profileDetailFlag_)) {
            profilePlan(plan.get());
          }
          return {part, code, dataset};
        }));
  }
  DLOG(INFO) << "xxxxxxxxxxxxxxxxxxxxxxx";
  folly::collectAll(futures).via(executor_).thenTry([this](auto&& t) {
    CHECK(!t.hasException());
    const auto& tries = t.value();
    for (size_t j = 0; j < tries.size(); j++) {
      CHECK(!tries[j].hasException());
      auto& [partId, code, dataset] = tries[j].value();
      if (code == ::nebula::cpp2::ErrorCode::SUCCEEDED) {
        for (auto& row : dataset) {
          resultDataSet_.emplace_back(std::move(row));
        }
      } else {
        handleErrorCode(code, context_->spaceId(), partId);
      }
    }
    DLOG(INFO) << "finish";
    this->onProcessFinished();
    this->onFinished();
  });
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

}  // namespace storage
}  // namespace nebula
