/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once

#include "common/base/Base.h"
#include "storage/exec/IndexScanNode2.h"
#include "storage/exec/RelNode.h"
#include "storage/exec/StorageIterator.h"
namespace nebula {
namespace storage {
class IndexVertexScanNode final : public IndexScanNode {
 public:
  static StatusOr<IndexVertexScanNode> make(RuntimeContext* context,
                                            IndexID indexId,
                                            const std::vector<cpp2::IndexColumnHint>& columnHint) {
    IndexVertexScanNode node(context, indexId, columnHint);
    auto env = context->env();
    auto spaceId = context->spaceId();
    auto indexMgr = env->indexMan_;
    auto schemaMgr = env->schemaMan_;
    auto index = indexMgr->getTagIndex(spaceId, indexId).value();
    auto tagSchema = schemaMgr->getTagSchema(spaceId, index->get_schema_id()).value();
    node.index_ = index;
    node.tag_ = tagSchema;
    return StatusOr<IndexVertexScanNode>(std::move(node));
  }

 private:
  IndexVertexScanNode(RuntimeContext* context,
                      IndexID indexId,
                      const std::vector<cpp2::IndexColumnHint>& clolumnHint)
      : IndexScanNode(context, indexId, clolumnHint) {}
  nebula::cpp2::ErrorCode init() override {}
  nebula::cpp2::ErrorCode execute(PartitionID partId) override {
    auto ret = resetIter(partId);
    return ret;
  }

  nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                      std::pair<std::string, std::string>& kv) override {
    kv.first = NebulaKeyUtils::vertexKey(context_->vIdLen(),
                                         partId_,
                                         key.subpiece(key.size() - context_->vIdLen()).toString(),
                                         context_->tagId_);
    return context_->env()->kvstore_->get(context_->spaceId(), partId_, kv.first, &kv.second);
  }
  Row decodeFromIndex(folly::StringPiece key) override {
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
    size_t offset = 0;
    for (auto& field : index_->get_fields()) {
    }
    index_->get_fields();
    for (auto& col : requiredColumns_) {
      switch (QueryUtils::toReturnColType(col)) {
        case QueryUtils::ReturnColType::kVid: {
          auto vId = IndexKeyUtils::getIndexVertexID(context_->vIdLen(), key);
          if (context_->isIntId()) {
            values.emplace_back(*reinterpret_cast<const int64_t*>(vId.data()));
          } else {
            values.emplace_back(vId.subpiece(0, vId.find_first_of('\0')).toString());
          }
          break;
        }
        case QueryUtils::ReturnColType::kTag: {
          values.emplace_back(context_->tagId_);
          break;
        }
        case QueryUtils::ReturnColType::kOther: {
          auto v = IndexKeyUtils::getValueFromIndexKey(
              context_->vIdLen(), key, col, fields_, false, hasNullableCol_);
          values.emplace_back(std::move(v));
          break;
        }
        default:
          LOG(FATAL) << "Unexpect column " << col << " in IndexVertexScanNode";
      }
    }
    return Row(std::move(values));
  }
  Map<std::string, Value> decodeFromBase(const std::string& key,
                                         const std::string& value) override {}
  const meta::SchemaProviderIf* getSchema() override { return tag_.get(); }

 private:
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> tag_;
  std::unique_ptr<kvstore::KVIterator> iter_;
};
}  // namespace storage
}  // namespace nebula
