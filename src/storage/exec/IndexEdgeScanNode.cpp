/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/exec/IndexEdgeScanNode.h"
namespace nebula {
namespace storage {
IndexEdgeScanNode::IndexEdgeScanNode(const IndexEdgeScanNode& node)
    : IndexScanNode(node), edge_(node.edge_) {}
IndexEdgeScanNode::IndexEdgeScanNode(RuntimeContext* context,
                                     IndexID indexId,
                                     const std::vector<cpp2::IndexColumnHint>& columnHint,
                                     ::nebula::kvstore::KVStore* kvstore,
                                     bool hasNullableCol)
    : IndexScanNode(context, "IndexEdgeScanNode", indexId, columnHint, kvstore, hasNullableCol) {
  getIndex = std::function([this](std::shared_ptr<IndexItem>& index) {
    auto env = this->context_->env();
    auto indexMgr = env->indexMan_;
    auto indexVal = indexMgr->getEdgeIndex(this->spaceId_, this->indexId_);
    if (!indexVal.ok()) {
      return ::nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    index = indexVal.value();
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  });
  getEdge = std::function([this](EdgeSchemas& edge) {
    auto env = this->context_->env();
    auto schemaMgr = env->schemaMan_;
    auto allSchema = schemaMgr->getAllVerEdgeSchema(this->spaceId_);
    auto edgeType = this->index_->get_schema_id().get_edge_type();
    if (!allSchema.ok() || !allSchema.value().count(edgeType)) {
      return ::nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    edge = allSchema.value().at(edgeType);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  });
}

::nebula::cpp2::ErrorCode IndexEdgeScanNode::init(InitContext& ctx) {
  if (auto ret = getIndex(this->index_); UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  if (auto ret = getEdge(edge_); UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  return IndexScanNode::init(ctx);
}

Row IndexEdgeScanNode::decodeFromIndex(folly::StringPiece key) {
  std::vector<Value> values(requiredColumns_.size());
  if (colPosMap_.count(kSrc)) {
    auto vId = IndexKeyUtils::getIndexSrcId(context_->vIdLen(), key);
    if (context_->isIntId()) {
      values[colPosMap_[kSrc]] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      values[colPosMap_[kSrc]] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
    }
  }
  if (colPosMap_.count(kDst)) {
    auto vId = IndexKeyUtils::getIndexDstId(context_->vIdLen(), key);
    if (context_->isIntId()) {
      values[colPosMap_[kDst]] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      values[colPosMap_[kDst]] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
    }
  }
  if (colPosMap_.count(kRank)) {
    auto rank = IndexKeyUtils::getIndexRank(context_->vIdLen(), key);
    values[colPosMap_[kRank]] = Value(rank);
  }
  if (colPosMap_.count(kType)) {
    values[colPosMap_[kType]] = Value(context_->edgeType_);
  }
  // Truncate the src/rank/dst at the end to facilitate obtaining the two bytes representing the
  // nullableBit directly at the end when needed
  key.subtract(context_->vIdLen() * 2 + sizeof(EdgeRanking));
  decodePropFromIndex(key, colPosMap_, values);
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
  auto reader = RowReaderWrapper::getRowReader(edge_, value);
  for (auto& col : requiredAndHintColumns_) {
    switch (QueryUtils::toReturnColType(col)) {
      case QueryUtils::ReturnColType::kType: {
        values[col] = Value(context_->edgeType_);
      } break;
      case QueryUtils::ReturnColType::kSrc: {
        auto vId = NebulaKeyUtils::getSrcId(context_->vIdLen(), key);
        if (context_->isIntId()) {
          values[col] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
          values[col] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
        }
      } break;
      case QueryUtils::ReturnColType::kDst: {
        auto vId = NebulaKeyUtils::getDstId(context_->vIdLen(), key);
        if (context_->isIntId()) {
          values[col] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
          values[col] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
        }
      } break;
      case QueryUtils::ReturnColType::kRank: {
        values[col] = Value(NebulaKeyUtils::getRank(context_->vIdLen(), key));
      } break;
      case QueryUtils::ReturnColType::kOther: {
        auto field = edge_.back()->field(col);
        if (field == nullptr) {
          values[col] = Value::kNullUnknownProp;
        } else {
          auto retVal = QueryUtils::readValue(reader.get(), col, field);
          if (!retVal.ok()) {
            LOG(FATAL) << "Bad value for field" << col;
          }
          values[col] = std::move(retVal.value());
        }
      } break;
      default:
        LOG(FATAL) << "Unexpect column name:" << col;
    }
  }
  return values;
}

const std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>>&
IndexEdgeScanNode::getSchema() {
  return edge_;
}

std::unique_ptr<IndexNode> IndexEdgeScanNode::copy() {
  return std::make_unique<IndexEdgeScanNode>(*this);
}

}  // namespace storage
}  // namespace nebula
