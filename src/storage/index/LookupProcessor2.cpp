/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/index/LookupProcessor2.h"

#include "storage/exec/IndexDedupNode.h"
#include "storage/exec/IndexEdgeScanNode.h"
#include "storage/exec/IndexLimitNode.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexProjectionNode.h"
#include "storage/exec/IndexSelectionNode.h"
#include "storage/exec/IndexVertexScanNode.h"

namespace nebula {
namespace storage {

void LookupProcessor::process(const cpp2::LookupIndexRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}
void LookupProcessor::doProcess(const cpp2::LookupIndexRequest& req) {
  prepare(req);
  auto plan = buildPlan(req);
  InitContext context;
  plan->init(context);
  if (!FLAGS_query_concurrently) {
    runInSingleThread(req.get_parts(), std::move(plan));
  } else {
    runInMultipleThread(req.get_parts(), std::move(plan));
  }
}
::nebula::cpp2::ErrorCode LookupProcessor::prepare(const cpp2::LookupIndexRequest& req) {}

std::unique_ptr<IndexNode> LookupProcessor::buildPlan(const cpp2::LookupIndexRequest& req) {
  bool is_edge = req.get_indices().get_schema_id().edge_type_ref().has_value();
  std::vector<std::unique_ptr<IndexNode>> nodes;
  for (auto& ctx : req.get_indices().get_contexts()) {
    auto node = buildOneContext(ctx);
  }
  for (size_t i = 0; i < nodes.size(); i++) {
    auto projection = std::make_unique<IndexProjectionNode>();
    projection->addChild(std::move(nodes[i]));
    nodes[i] = std::move(projection);
  }
  if (nodes.size() > 1) {
    auto dedup = std::make_unique<IndexDedupNode>();
    for (auto& node : nodes) {
      node->addChild(std::move(node));
    }
    nodes.clear();
    nodes[0] = std::move(dedup);
  }
  if (req.limit_ref().has_value()) {
    auto limit = req.get_limit();
    auto node = std::make_unique<IndexLimitNode>();
    node->addChild(std::move(nodes[0]));
    nodes[0] = std::move(node);
  }
  InitContext ctx;
  auto result = nodes[0]->init(ctx);
  if (result == ::nebula::cpp2::ErrorCode::SUCCEEDED) {
  } else {
  }
  return std::move(nodes[0]);
}

std::unique_ptr<IndexNode> LookupProcessor::buildOneContext(const cpp2::IndexQueryContext& ctx) {
  std::unique_ptr<IndexNode> node;
  if (context_->isEdge()) {
    node = std::make_unique<IndexEdgeScanNode>();
  } else {
    node = std::make_unique<IndexVertexScanNode>();
  }
  if (ctx.filter_ref().has_value()) {
    auto filterNode = std::make_unique<IndexSelectionNode>(context_.get(), nullptr);
  }
  return std::move(node);
}
void LookupProcessor::runInSingleThread(const std::vector<PartitionID>& parts,
                                        std::unique_ptr<IndexNode> plan) {
  std::vector<std::deque<Row>> datasetList;
  std::vector<::nebula::cpp2::ErrorCode> codeList;
  for (auto part : parts) {
    plan->execute(part);
    bool hasNext = true;
    ::nebula::cpp2::ErrorCode code;
    decltype(datasetList)::value_type dataset;
    nebula::DataSet dataset;
    do {
      auto result = plan->next(hasNext);
      if (hasNext && ::nebula::ok(result)) {
        dataset.emplace_back(::nebula::value(std::move(result)));
      } else {
        break;
      }
    } while (true);
    datasetList.emplace_back(std::move(dataset));
    codeList.emplace_back(code);
  }
  // 处理 leader change
}
void LookupProcessor::runInMultipleThread(const std::vector<PartitionID>& parts,
                                          std::unique_ptr<IndexNode> plan) {
  std::vector<std::unique_ptr<IndexNode>> planCopy = reproducePlan(plan.get(), parts.size());
  std::vector<std::vector<std::deque<Row>>> datasetList(parts.size());
  std::vector<::nebula::cpp2::ErrorCode> codeList(parts.size());
  std::vector<std::function<size_t()>> funcs;
  std::vector<folly::Future<size_t>> futures;
  for (size_t i = 0; i < parts.size(); i++) {
    funcs.emplace_back([this,
                        &plan = planCopy[i],
                        &dataset = datasetList[i],
                        &code = codeList[i],
                        part = parts[i],
                        index = i]() {
      plan->execute(part);
      bool hasNext = true;
      do {
        auto result = plan->next(hasNext);
        if (hasNext && ::nebula::ok(result)) {
          dataset.emplace_back(::nebula::value(std::move(result)));
        } else {
          break;
        }
      } while (true);
      return index;
    });
  }
  for (size_t i = 0; i < parts.size(); i++) {
    futures.emplace_back(folly::via(executor_, std::move(funcs[i])));
  }
  folly::collectAll(futures).via(executor_).thenTry([this](auto&& t) {
    CHECK(!t.hasException());
    const auto& values = t.value();
    for (auto& value : values) {
      // for (size_t i = 0; i < t.size(); i++) {
      //   // 检查切主 合并数据
      // }
    }
  });
}
std::vector<std::unique_ptr<IndexNode>> LookupProcessor::reproducePlan(IndexNode* root,
                                                                       size_t count) {
  std::vector<std::unique_ptr<IndexNode>> ret(count);
  for (size_t i = 0; i < count; i++) {
    ret.emplace_back(root->copy());
  }
  for (auto& child : root->children()) {
    auto childPerPlan = reproducePlan(child.get(), count);
    for (size_t i = 0; i < count; i++) {
      ret[i]->addChild(std::move(childPerPlan[i]));
    }
  }
  return std::move(ret);
}

}  // namespace storage
}  // namespace nebula
