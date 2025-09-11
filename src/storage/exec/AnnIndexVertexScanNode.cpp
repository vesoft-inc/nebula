/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/AnnIndexVertexScanNode.h"

#include "codec/RowReaderWrapper.h"
#include "common/base/Status.h"
#include "common/datatypes/DataSet.h"
#include "common/thrift/ThriftTypes.h"
#include "common/utils/NebulaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/exec/QueryUtils.h"
namespace nebula {
namespace storage {

AnnIndexVertexScanNode::AnnIndexVertexScanNode(const AnnIndexVertexScanNode& node)
    : AnnIndexScanNode(node), tags_(node.tags_), getIndex(node.getIndex), getTags(node.getTags) {}

AnnIndexVertexScanNode::AnnIndexVertexScanNode(RuntimeContext* context,
                                               IndexID indexId,
                                               ::nebula::kvstore::KVStore* kvstore,
                                               bool hasNullableCol,
                                               int64_t limit,
                                               int64_t param,
                                               const Value& queryVector)
    : AnnIndexScanNode(context,
                       "AnnIndexVertexScanNode",
                       indexId,
                       kvstore,
                       hasNullableCol,
                       limit,
                       param,
                       queryVector) {
  getIndex = std::function([this](std::shared_ptr<AnnIndexItem>& index) {
    if (this->context_ == nullptr) {
      LOG(ERROR) << "RuntimeContext is null";
      return ::nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
    auto indexVal =
        this->context_->env()->indexMan_->getTagAnnIndex(this->spaceId_, this->indexId_);
    if (!indexVal.ok()) {
      return ::nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    index = indexVal.value();
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  });
  getTags = std::function([this](std::unordered_map<TagID, TagSchemas>& tags) {
    if (this->context_ == nullptr) {
      LOG(ERROR) << "RuntimeContext is null";
      return ::nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
    auto schemaMgr = this->context_->env()->schemaMan_;
    if (schemaMgr == nullptr) {
      LOG(ERROR) << "SchemaManager is null";
      return ::nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
    auto allSchema = schemaMgr->getAllVerTagSchema(this->spaceId_);
    for (auto& tagId : this->index_->get_schema_ids()) {
      auto key = tagId.get_tag_id();
      if (!allSchema.ok() || !allSchema.value().count(key)) {
        return ::nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
      }
      tags.insert({key, allSchema.value().at(key)});
    }
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  });
}

::nebula::cpp2::ErrorCode AnnIndexVertexScanNode::init(InitContext& ctx) {
  if (auto ret = getIndex(index_); UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  if (auto ret = getTags(tags_); UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  return AnnIndexScanNode::init(ctx);
}

StatusOr<Map<std::string, Value>> AnnIndexVertexScanNode::getVidByVectorId(VectorID vectorId) {
  Map<std::string, Value> values;
  std::string vid;
  auto vidIdKey = NebulaKeyUtils::vidIdTagPrefix(partId_, indexId_, vectorId);
  auto ret = kvstore_->get(spaceId_, partId_, vidIdKey, &vid);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return nebula::Status::Error("Failed to get vid by vectorId");
  }
  // Check bounds for searchResults_.distances access
  if (currentResultIndex_ > searchResults_.distances.size()) {
    return nebula::Status::Error("Invalid currentResultIndex_");
  }
  for (auto& col : requiredColumns_) {
    switch (QueryUtils::toReturnColType(col)) {
      case QueryUtils::ReturnColType::kVid: {
        values[col] = Value(vid.substr(0, vid.find_first_of('\0')));
      } break;
      case QueryUtils::ReturnColType::kDis: {
        float distance = searchResults_.distances[currentResultIndex_ - 1];
        values[col] = Value(distance);
      } break;
      default:
        LOG(FATAL) << "Unexpect column name:" << col;
    }
  }
  return values;
}

std::unique_ptr<IndexNode> AnnIndexVertexScanNode::copy() {
  return std::make_unique<AnnIndexVertexScanNode>(*this);
}

}  // namespace storage
}  // namespace nebula
