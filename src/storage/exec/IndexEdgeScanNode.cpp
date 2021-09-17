/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/exec/IndexEdgeScanNode.h"
namespace nebula {
namespace storage {
IndexNode::ErrorOr<IndexEdgeScanNode> IndexEdgeScanNode::make(
    RuntimeContext* context,
    IndexID indexId,
    const std::vector<cpp2::IndexColumnHint>& columnHint) {
  IndexEdgeScanNode node(context, indexId, columnHint);
  auto env = context->env();
  auto spaceId = context->spaceId();
  auto indexMgr = env->indexMan_;
  auto schemaMgr = env->schemaMan_;
  auto index = indexMgr->getTagIndex(spaceId, indexId).value();
  auto edgeSchema = schemaMgr->getEdgeSchema(spaceId, index->get_schema_id().get_tag_id());
  node.index_ = index;
  node.edge_ = edgeSchema;
  return ErrorOr<IndexEdgeScanNode>(std::move(node));
}
Row IndexEdgeScanNode::decodeFromIndex(folly::StringPiece key) {
  std::vector<Value> values(requiredColumns_.size());
  Map<std::string, size_t> colPosMap;
  for (size_t i = 0; i < requiredColumns_.size(); i++) {
    colPosMap[requiredColumns_[i]] = i;
  }
  if (colPosMap.count(kSrc)) {
    auto vId = IndexKeyUtils::getIndexSrcId(context_->vIdLen(), key);
    if (context_->isIntId()) {
      values[colPosMap[kSrc]] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      values[colPosMap[kSrc]] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
    }
  }
  if (colPosMap.count(kDst)) {
    auto vId = IndexKeyUtils::getIndexSrcId(context_->vIdLen(), key);
    if (context_->isIntId()) {
      values[colPosMap[kSrc]] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      values[colPosMap[kSrc]] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
    }
  }
  if (colPosMap.count(kRank)) {
    auto rank = IndexKeyUtils::getIndexRank(context_->vIdLen(), key);
  }
  key.subtract(context_->vIdLen() * 2 + sizeof(EdgeType));
  decodePropFromIndex(key, colPosMap, values);
  return Row(std::move(values));
}
nebula::cpp2::ErrorCode IndexEdgeScanNode::getBaseData(folly::StringPiece key,
                                                       std::pair<std::string, std::string>& kv) {
  auto vIdLen = context_->vIdLen();
  kv.first = NebulaKeyUtils::edgeKey(vIdLen,
                                     partId_,
                                     IndexKeyUtils::getIndexSrcId(vIdLen, key).str(),
                                     context_->edgeType_,
                                     IndexKeyUtils::getIndexRank(vIdLen, key),
                                     IndexKeyUtils::getIndexDstId(vIdLen, key).str());
  return kvstore_->get(context_->spaceId(), partId_, kv.first, &kv.second);
}
Map<std::string, Value> IndexEdgeScanNode::decodeFromBase(const std::string& key,
                                                          const std::string& value) {
  Map<std::string, Value> values;
  auto reader = RowReaderWrapper::getRowReader(edge_.get(), value);
  for (auto& col : requiredColumns_) {
    switch (QueryUtils::toReturnColType(col)) {
      case QueryUtils::ReturnColType::kSrc: {
        auto vId = NebulaKeyUtils::getSrcId(context_->vIdLen(), key);
        if (context_->isIntId()) {
          values[col] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
          values[col] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
        }
      } break;
      case QueryUtils::ReturnColType::kDst: {
        values[col] = Value(context_->tagId_);
      } break;
      case QueryUtils::ReturnColType::kOther: {
        auto retVal = QueryUtils::readValue(reader.get(), col, edge_->field(col));
        if (!retVal.ok()) {
          LOG(FATAL) << "Bad value for field" << col;
        }
        values[col] = std::move(retVal.value());
      } break;
      default:
        LOG(FATAL) << "Unexpect column name:" << col;
    }
  }
  return values;
}
const meta::SchemaProviderIf* IndexEdgeScanNode::getSchema() { return edge_.get(); }
}  // namespace storage
}  // namespace nebula
