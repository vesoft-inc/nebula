/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/index/LookupAnnProcessor.h"

#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include <memory>

#include "common/memory/MemoryTracker.h"
#include "common/thrift/ThriftTypes.h"
#include "folly/Likely.h"
#include "interface/gen-cpp2/common_types.tcc"
#include "interface/gen-cpp2/meta_types.tcc"
#include "interface/gen-cpp2/storage_types.tcc"
#include "storage/CommonUtils.h"
#include "storage/exec/AnnIndexVertexScanNode.h"
#include "storage/exec/IndexAggregateNode.h"
#include "storage/exec/IndexDedupNode.h"
#include "storage/exec/IndexLimitNode.h"
#include "storage/exec/IndexNode.h"
#include "storage/exec/IndexProjectionNode.h"
#include "storage/exec/IndexSelectionNode.h"
#include "storage/exec/IndexTopNNode.h"

namespace nebula {
namespace storage {

ProcessorCounters kLookupAnnCounters;

void LookupAnnProcessor::process(const cpp2::LookupAnnIndexRequest& req) {
  if (executor_ != nullptr) {
    executor_->add(
        [this, req]() { MemoryCheckScope wrapper(this, [this, req] { this->doProcess(req); }); });
  } else {
    doProcess(req);
  }
}

void LookupAnnProcessor::doProcess(const cpp2::LookupAnnIndexRequest& req) {
  auto code = prepare(req);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto& p : req.get_parts()) {
      pushResultCode(code, p);
    }
    onFinished();
    return;
  }

  auto planRet = buildAnnPlan(req);
  if (!ok(planRet)) {
    for (auto& p : req.get_parts()) {
      pushResultCode(error(planRet), p);
    }
    onFinished();
    return;
  }
  auto plan = std::move(nebula::value(planRet));

  InitContext ctx;
  code = plan->init(ctx);
  if (UNLIKELY(code != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    for (auto& p : req.get_parts()) {
      pushResultCode(code, p);
    }
    onFinished();
    return;
  }
  if (FLAGS_query_concurrently) {
    runInMultipleThread(req.get_parts(), std::move(plan));
  } else {
    runInSingleThread(req.get_parts(), std::move(plan));
  }
}

::nebula::cpp2::ErrorCode LookupAnnProcessor::prepare(const cpp2::LookupAnnIndexRequest& req) {
  planContext_ = std::make_unique<PlanContext>(
      this->env_, req.get_space_id(), this->spaceVidLen_, this->isIntId_, req.common_ref());
  planContext_->isEdge_ = req.get_ann_indice().get_schema_ids().at(0).getType() ==
                          nebula::cpp2::SchemaID::Type::edge_type;
  globalLimit_ = req.get_limit();
  param_ = req.get_param();
  queryVector_ = req.get_query_vector();

  // Validate that all contexts have valid index IDs
  auto vectorContext = req.get_ann_indice().get_context();
  auto indexId = vectorContext.get_index_id();
  auto indexItem = env_->indexMan_->getTagAnnIndex(req.get_space_id(), indexId);
  if (!indexItem.ok()) {
    LOG(ERROR) << "ANN Index not found, indexId: " << indexId;
    return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
  }

  // Validate that this is indeed a vector index
  auto item = indexItem.value();
  if (!item->get_ann_params()) {
    LOG(ERROR) << "Index is not a vector index, indexId: " << indexId;
    return nebula::cpp2::ErrorCode::E_INVALID_PARM;
  }
  context_ = std::make_unique<RuntimeContext>(this->planContext_.get());
  std::string schemaName;
  if (planContext_->isEdge_) {
    std::vector<EdgeType> edgeTypes;
    std::vector<std::string> edgeNames;
    for (auto& schema_id : req.get_ann_indice().get_schema_ids()) {
      auto edgeType = schema_id.get_edge_type();
      auto schemaNameValue = env_->schemaMan_->toEdgeName(req.get_space_id(), edgeType);
      if (!schemaNameValue.ok()) {
        return ::nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
      }
      schemaName = schemaNameValue.value();
      edgeTypes.emplace_back(edgeType);
      edgeNames.emplace_back(std::move(schemaName));
    }
    context_->edgeTypes_ = std::move(edgeTypes);
    context_->edgeNames_ = std::move(edgeNames);
  } else {
    std::vector<TagID> tagIds;
    std::vector<std::string> tagNames;
    for (auto& schema_id : req.get_ann_indice().get_schema_ids()) {
      auto tagId = schema_id.get_tag_id();
      auto schemaNameValue = env_->schemaMan_->toTagName(req.get_space_id(), tagId);
      if (!schemaNameValue.ok()) {
        return ::nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
      }
      schemaName = schemaNameValue.value();
      tagIds.emplace_back(tagId);
      tagNames.emplace_back(std::move(schemaName));
    }
    context_->tagIds_ = std::move(tagIds);
    context_->tagNames_ = std::move(tagNames);
  }

  std::vector<std::string> colNames;
  for (auto& col : *req.get_return_columns()) {
    if (context_->isEdge()) {
      for (auto& edgeName : context_->edgeNames_) {
        colNames.emplace_back(edgeName + "." + col);
      }
    } else {
      for (auto& tagName : context_->tagNames_) {
        colNames.emplace_back(tagName + "." + col);
      }
    }
  }
  resultDataSet_ = ::nebula::DataSet(colNames);
  return ::nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<IndexNode>> LookupAnnProcessor::buildAnnPlan(
    const cpp2::LookupAnnIndexRequest& req) {
  // Build individual ANN context plans
  std::unique_ptr<IndexNode> node;
  auto context = req.get_ann_indice().get_context();
  auto ret = buildOneAnnContext(context);
  if (!ok(ret)) {
    return error(ret);
  }
  node = std::move(value(ret));

  auto projection =
      std::make_unique<IndexProjectionNode>(context_.get(), *req.get_return_columns());
  projection->addChild(std::move(node));
  node = std::move(projection);

  std::vector<std::string> dedupColumn;
  if (context_->isEdge()) {
    dedupColumn = std::vector<std::string>{kSrc, kRank, kDst};
  } else {
    dedupColumn = std::vector<std::string>{kVid, kDis};
  }
  auto dedup = std::make_unique<IndexDedupNode>(context_.get(), dedupColumn);
  dedup->addChild(std::move(node));
  std::unique_ptr<IndexNode> root = std::move(dedup);

  return root;
}

ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<IndexNode>> LookupAnnProcessor::buildOneAnnContext(
    const cpp2::IndexQueryContext& ctx) {
  std::unique_ptr<IndexNode> node;
  if (context_->isEdge()) {
    // auto idx = env_->indexMan_->getEdgeAnnIndex(context_->spaceId(), ctx.get_index_id());
    // if (!idx.ok()) {
    //   return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    // }
    // auto cols = idx.value()->get_fields();
    // bool hasNullableCol =
    //     std::any_of(cols.begin(), cols.end(), [](const meta::cpp2::ColumnDef& col) {
    //       return col.nullable_ref().value_or(false);
    //     });
    // node = std::make_unique<IndexEdgeScanNode>(context_.get(),
    //                                            ctx.get_index_id(),
    //                                            ctx.get_column_hints(),
    //                                            context_->env()->kvstore_,
    //                                            hasNullableCol);
  } else {
    auto idx = env_->indexMan_->getTagAnnIndex(planContext_->spaceId_, ctx.get_index_id());
    if (!idx.ok()) {
      return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    auto col = idx.value()->get_fields();
    bool hasNullableCol =
        col.empty() ? false
                    : std::any_of(col.begin(), col.end(), [](const meta::cpp2::ColumnDef& c) {
                        return c.nullable_ref().value_or(false) == true;
                      });

    node = std::make_unique<AnnIndexVertexScanNode>(context_.get(),
                                                    ctx.get_index_id(),
                                                    context_->env()->kvstore_,
                                                    hasNullableCol,
                                                    globalLimit_,
                                                    param_,
                                                    queryVector_);
  }
  return node;
}

void LookupAnnProcessor::runInSingleThread(const std::vector<PartitionID>& parts,
                                           std::unique_ptr<IndexNode> plan) {
  memory::MemoryCheckGuard guard;
  ::nebula::cpp2::ErrorCode code = ::nebula::cpp2::ErrorCode::SUCCEEDED;
  // Initialize results storage
  partResults_.clear();
  for (const auto& partId : parts) {
    partResults_[partId] = nebula::DataSet();
  }
  for (size_t i = 0; i < parts.size(); i++) {
    auto partId = parts[i];
    code = plan->execute(partId);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      pushResultCode(code, partId);
      continue;
    }
    do {
      auto result = plan->next();
      if (!result.success()) {
        code = result.code();
        break;
      }
      if (result.hasData()) {
        partResults_[partId].rows.emplace_back(std::move(result).row());
      } else {
        break;
      }
    } while (true);
  }
  mergeResults();
  onProcessFinished();
  onFinished();
}

void LookupAnnProcessor::runInMultipleThread(const std::vector<PartitionID>& parts,
                                             std::unique_ptr<IndexNode> plan) {
  memory::MemoryCheckOffGuard offGuard;
  // Initialize results storage
  partResults_.clear();
  for (const auto& partId : parts) {
    partResults_[partId] = nebula::DataSet();
  }
  // For multi-threaded execution, create plan copies
  std::vector<std::unique_ptr<IndexNode>> planCopy = reproducePlan(plan.get(), parts.size());
  using ReturnType = std::tuple<PartitionID, ::nebula::cpp2::ErrorCode, std::deque<Row>>;
  std::vector<folly::Future<ReturnType>> futures;

  for (size_t i = 0; i < parts.size(); i++) {
    futures.emplace_back(
        folly::via(executor_, [plan = std::move(planCopy[i]), part = parts[i]]() -> ReturnType {
          memory::MemoryCheckGuard guard;
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
          return {part, code, dataset};
        }).thenError(folly::tag_t<std::bad_alloc>{}, [this](const std::bad_alloc&) {
          memoryExceeded_ = true;
          return folly::makeFuture<
              std::tuple<PartitionID, ::nebula::cpp2::ErrorCode, std::deque<Row>>>(
              std::runtime_error("Memory Limit Exceeded, " +
                                 memory::MemoryStats::instance().toString()));
        }));
  }
  folly::collectAll(futures)
      .via(executor_)
      .thenTry([this](auto&& t) {
        memory::MemoryCheckGuard guard;
        CHECK(!t.hasException());
        const auto& tries = t.value();
        for (size_t j = 0; j < tries.size(); j++) {
          if (tries[j].hasException()) {
            LOG(ERROR) << "Exception: " << tries[j].exception().what();
            onError();
            return;
          }
          auto& [partId, code, dataset] = tries[j].value();
          if (code == ::nebula::cpp2::ErrorCode::SUCCEEDED) {
            for (auto& data : dataset) {
              partResults_[partId].rows.emplace_back(std::move(data));
            }
          } else {
            handleErrorCode(code, context_->spaceId(), partId);
          }
        }
        DLOG(INFO) << "finish";
        mergeResults();
        this->onProcessFinished();
        this->onFinished();
      })
      .thenError(folly::tag_t<std::bad_alloc>{}, [this](const std::bad_alloc&) {
        memoryExceeded_ = true;
        onError();
      });
}

void LookupAnnProcessor::mergeResults() {
  // combine all results of different parts and sort by distance
  std::vector<std::pair<Row, float>> scoredRows;
  scoredRows.reserve(globalLimit_ * partResults_.size());
  for (const auto& [partId, partResult] : partResults_) {
    for (const auto& row : partResult.rows) {
      // Extract distance score([vid, dist])
      auto dist = row.values.back();
      scoredRows.emplace_back(row,
                              dist.isFloat() ? dist.getFloat() : std::numeric_limits<float>::max());
    }
  }
  std::sort(scoredRows.begin(), scoredRows.end(), [](const auto& a, const auto& b) {
    return a.second < b.second;
  });
  for (const auto& scoredRow : scoredRows) {
    if (resultDataSet_.rows.size() >= static_cast<size_t>(globalLimit_)) {
      break;
    }
    resultDataSet_.rows.emplace_back(scoredRow.first);
  }
}

std::vector<std::unique_ptr<IndexNode>> LookupAnnProcessor::reproducePlan(IndexNode* root,
                                                                          size_t count) {
  std::vector<std::unique_ptr<IndexNode>> ret(count);
  for (size_t i = 0; i < count; i++) {
    ret[i] = root->copy();
  }
  for (auto& child : root->children()) {
    auto childPerPlan = reproducePlan(child.get(), count);
    for (size_t i = 0; i < count; i++) {
      ret[i]->addChild(std::move(childPerPlan[i]));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
