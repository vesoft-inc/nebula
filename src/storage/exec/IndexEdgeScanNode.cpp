/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/exec/IndexEdgeScanNode.h"
namespace nebula {
namespace storage {
IndexEdgeScanNode::IndexEdgeScanNode(const IndexEdgeScanNode& node)
    : IndexScanNode(node), edge_(node.edge_) {
  getIndex = std::function([this]() {
    auto env = this->context_->env();
    auto indexMgr = env->indexMan_;
    auto index = indexMgr->getTagIndex(this->spaceId_, this->indexId_).value();
    return index;
  });
  getEdge = std::function([this]() {
    auto env = this->context_->env();
    auto schemaMgr = env->schemaMan_;
    auto schema =
        schemaMgr->getEdgeSchema(this->spaceId_, this->index_->get_schema_id().get_edge_type());
    return schema;
  });
}
IndexEdgeScanNode::IndexEdgeScanNode(RuntimeContext* context,
                                     IndexID indexId,
                                     const std::vector<cpp2::IndexColumnHint>& columnHint)
    : IndexScanNode(context, "IndexEdgeScanNode", indexId, columnHint) {
  getIndex = std::function([this]() {
    auto env = this->context_->env();
    auto indexMgr = env->indexMan_;
    auto index = indexMgr->getTagIndex(this->spaceId_, this->indexId_).value();
    return index;
  });
  getEdge = std::function([this]() {
    auto env = this->context_->env();
    auto schemaMgr = env->schemaMan_;
    auto schema =
        schemaMgr->getEdgeSchema(this->spaceId_, this->index_->get_schema_id().get_edge_type());
    return schema;
  });
}
::nebula::cpp2::ErrorCode IndexEdgeScanNode::init(InitContext& ctx) {
  index_ = getIndex().value();
  edge_ = getEdge();
  return IndexScanNode::init(ctx);
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

std::unique_ptr<IndexNode> IndexEdgeScanNode::copy() {
  return std::make_unique<IndexEdgeScanNode>(*this);
}

}  // namespace storage
}  // namespace nebula
