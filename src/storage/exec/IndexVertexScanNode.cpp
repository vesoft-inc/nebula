/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include "storage/exec/IndexVertexScanNode.h"

#include "codec/RowReaderWrapper.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/exec/QueryUtils.h"
namespace nebula {
namespace storage {

IndexVertexScanNode::IndexVertexScanNode(const IndexVertexScanNode& node)
    : IndexScanNode(node), tag_(node.tag_) {}

IndexVertexScanNode::IndexVertexScanNode(RuntimeContext* context,
                                         IndexID indexId,
                                         const std::vector<cpp2::IndexColumnHint>& columnHint,
                                         ::nebula::kvstore::KVStore* kvstore,
                                         bool hasNullableCol)
    : IndexScanNode(context, "IndexVertexScanNode", indexId, columnHint, kvstore, hasNullableCol) {
  getIndex = std::function([this](std::shared_ptr<IndexItem>& index) {
    auto env = this->context_->env();
    auto indexMgr = env->indexMan_;
    auto indexVal = indexMgr->getTagIndex(this->spaceId_, this->indexId_);
    if (!indexVal.ok()) {
      return ::nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    index = indexVal.value();
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  });
  getTag = std::function([this](TagSchemas& tag) {
    auto env = this->context_->env();
    auto schemaMgr = env->schemaMan_;
    auto allSchema = schemaMgr->getAllVerTagSchema(this->spaceId_);
    auto tagId = this->index_->get_schema_id().get_tag_id();
    if (!allSchema.ok() || !allSchema.value().count(tagId)) {
      return ::nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
    }
    tag = allSchema.value().at(tagId);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  });
}

::nebula::cpp2::ErrorCode IndexVertexScanNode::init(InitContext& ctx) {
  if (auto ret = getIndex(this->index_); UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  if (auto ret = getTag(tag_); UNLIKELY(ret != ::nebula::cpp2::ErrorCode::SUCCEEDED)) {
    return ret;
  }
  return IndexScanNode::init(ctx);
}

nebula::cpp2::ErrorCode IndexVertexScanNode::getBaseData(folly::StringPiece key,
                                                         std::pair<std::string, std::string>& kv) {
  kv.first = NebulaKeyUtils::tagKey(context_->vIdLen(),
                                    partId_,
                                    key.subpiece(key.size() - context_->vIdLen()).toString(),
                                    context_->tagId_);
  return kvstore_->get(context_->spaceId(), partId_, kv.first, &kv.second);
}

Row IndexVertexScanNode::decodeFromIndex(folly::StringPiece key) {
  std::vector<Value> values(requiredColumns_.size());
  if (colPosMap_.count(kVid)) {
    auto vId = IndexKeyUtils::getIndexVertexID(context_->vIdLen(), key);
    if (context_->isIntId()) {
      values[colPosMap_[kVid]] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      values[colPosMap_[kVid]] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
    }
  }
  if (colPosMap_.count(kTag)) {
    values[colPosMap_[kTag]] = Value(context_->tagId_);
  }
  key.subtract(context_->vIdLen());
  decodePropFromIndex(key, colPosMap_, values);
  return Row(std::move(values));
}

Map<std::string, Value> IndexVertexScanNode::decodeFromBase(const std::string& key,
                                                            const std::string& value) {
  Map<std::string, Value> values;
  auto reader = RowReaderWrapper::getRowReader(tag_, folly::StringPiece(value));
  for (auto& col : requiredAndHintColumns_) {
    switch (QueryUtils::toReturnColType(col)) {
      case QueryUtils::ReturnColType::kVid: {
        auto vId = NebulaKeyUtils::getVertexId(context_->vIdLen(), key);
        if (context_->isIntId()) {
          values[col] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
        } else {
          values[col] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
        }
      } break;
      case QueryUtils::ReturnColType::kTag: {
        values[col] = Value(context_->tagId_);
      } break;
      case QueryUtils::ReturnColType::kOther: {
        auto field = tag_.back()->field(col);
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

std::unique_ptr<IndexNode> IndexVertexScanNode::copy() {
  return std::make_unique<IndexVertexScanNode>(*this);
}

}  // namespace storage
}  // namespace nebula
