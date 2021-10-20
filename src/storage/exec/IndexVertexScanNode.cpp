/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexVertexScanNode.h"

#include "codec/RowReaderWrapper.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/exec/QueryUtils.h"
namespace nebula {
namespace storage {

IndexVertexScanNode::IndexVertexScanNode(const IndexVertexScanNode& node)
    : IndexScanNode(node), tag_(node.tag_) {
  getIndex = std::function([this]() {
    auto env = this->context_->env();
    auto indexMgr = env->indexMan_;
    auto index = indexMgr->getTagIndex(this->spaceId_, this->indexId_).value();
    return index;
  });
  getTag = std::function([this]() {
    auto env = this->context_->env();
    auto schemaMgr = env->schemaMan_;
    auto schema =
        schemaMgr->getTagSchema(this->spaceId_, this->index_->get_schema_id().get_tag_id());
    return schema;
  });
}

IndexVertexScanNode::IndexVertexScanNode(RuntimeContext* context,
                                         IndexID indexId,
                                         const std::vector<cpp2::IndexColumnHint>& clolumnHint,
                                         ::nebula::kvstore::KVStore* kvstore)
    : IndexScanNode(context, "IndexVertexScanNode", indexId, clolumnHint, kvstore) {
  getIndex = std::function([this]() {
    auto env = this->context_->env();
    auto indexMgr = env->indexMan_;
    auto index = indexMgr->getTagIndex(this->spaceId_, this->indexId_).value();
    return index;
  });
  getTag = std::function([this]() {
    auto env = this->context_->env();
    auto schemaMgr = env->schemaMan_;
    auto schema =
        schemaMgr->getTagSchema(this->spaceId_, this->index_->get_schema_id().get_tag_id());
    return schema;
  });
}
::nebula::cpp2::ErrorCode IndexVertexScanNode::init(InitContext& ctx) {
  this->index_ = getIndex().value();
  this->tag_ = getTag();
  return IndexScanNode::init(ctx);
}
nebula::cpp2::ErrorCode IndexVertexScanNode::getBaseData(folly::StringPiece key,
                                                         std::pair<std::string, std::string>& kv) {
  kv.first = NebulaKeyUtils::vertexKey(context_->vIdLen(),
                                       partId_,
                                       key.subpiece(key.size() - context_->vIdLen()).toString(),
                                       context_->tagId_);
  DVLOG(3) << partId_;
  DVLOG(1) << '\n' << folly::hexDump(kv.first.data(), kv.first.size());
  return kvstore_->get(context_->spaceId(), partId_, kv.first, &kv.second);
}
Row IndexVertexScanNode::decodeFromIndex(folly::StringPiece key) {
  std::vector<Value> values(requiredColumns_.size());
  Map<std::string, size_t> colPosMap;
  for (size_t i = 0; i < requiredColumns_.size(); i++) {
    colPosMap[requiredColumns_[i]] = i;
  }
  if (colPosMap.count(kVid)) {
    auto vId = IndexKeyUtils::getIndexVertexID(context_->vIdLen(), key);
    if (context_->isIntId()) {
      values[colPosMap[kVid]] = Value(*reinterpret_cast<const int64_t*>(vId.data()));
    } else {
      values[colPosMap[kVid]] = Value(vId.subpiece(0, vId.find_first_of('\0')).toString());
    }
  }
  if (colPosMap.count(kTag)) {
    values[colPosMap[kTag]] = Value(context_->tagId_);
  }
  key.subtract(context_->vIdLen());
  decodePropFromIndex(key, colPosMap, values);
  return Row(std::move(values));
}
Map<std::string, Value> IndexVertexScanNode::decodeFromBase(const std::string& key,
                                                            const std::string& value) {
  Map<std::string, Value> values;
  auto reader = RowReaderWrapper::getRowReader(tag_.get(), value);
  for (auto& col : requiredColumns_) {
    DLOG(INFO) << col;
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
        auto retVal = QueryUtils::readValue(reader.get(), col, tag_->field(col));
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
std::unique_ptr<IndexNode> IndexVertexScanNode::copy() {
  return std::make_unique<IndexVertexScanNode>(*this);
}

}  // namespace storage
}  // namespace nebula
