/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/exec/GetPropNode3.h"

#include "common/base/Status.h"
namespace nebula::storage {

TagScan::TagScan(RuntimeContext* context,
                 TagContext* ctx,
                 TagID tagId,
                 const std::vector<PropContext>* props)
    : context_(context), tagContext_(ctx), tagId_(tagId), props_(props) {
  auto schemaIter = tagContext_->schemas_.find(tagId_);
  CHECK(schemaIter != tagContext_->schemas_.end());
  CHECK(!schemaIter->second.empty());
  schemas_ = &(schemaIter->second);
  ttl_ = QueryUtils::getTagTTLInfo(tagContext_, tagId_);
  tagName_ = tagContext_->tagNames_[tagId_];
  name_ = "TagScan";
}

nebula::cpp2::ErrorCode TagScan::doExecute(PartitionID partId, const std::vector<VertexID>& vIds) {
  {
    auto ret = RelNode::doExecute(partId, vIds);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
  }
  keys_.clear();
  values_.clear();
  statuses_.clear();

  // for (size_t i = 0; i < vIds.size(); i += 100) {
  //   std::vector<std::string> keys;
  //   for (size_t j = 0; j < 100 && i + j < vIds.size(); j++) {
  //     auto key = NebulaKeyUtils::tagKey(context_->vIdLen(), partId, vIds[i + j], tagId_);
  //     keys.emplace_back(std::move(key));
  //   }
  //   std::vector<std::string> values;
  //   auto ret = context_->env()->kvstore_->multiGet(context_->spaceId(), partId, keys, &values);
  //   if (ret.first != nebula::cpp2::ErrorCode::SUCCEEDED &&
  //       ret.first != nebula::cpp2::ErrorCode::E_PARTIAL_RESULT) {
  //     LOG(INFO) << static_cast<int>(ret.first);
  //     return ret.first;
  //   }
  //   keys_.insert(keys_.end(), keys.begin(), keys.end());
  //   values_.insert(values_.end(), values.begin(), values.end());
  //   statuses_.insert(statuses_.end(), ret.second.begin(), ret.second.end());
  // }
  for (size_t i = 0; i < vIds.size(); i++) {
    auto key = NebulaKeyUtils::tagKey(context_->vIdLen(), partId, vIds[i], tagId_);
    std::string value;
    auto ret = context_->env()->kvstore_->get(context_->spaceId(), partId, key, &value);
    keys_.push_back(key);
    values_.push_back(value);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      statuses_.push_back(Status::OK());
    } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      statuses_.push_back(Status::KeyNotFound());
    } else {
      return ret;
    }
  }
  next_ = 0;
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

bool TagScan::valid() const {
  if (next_ >= keys_.size()) {
    valid_ = false;
    return false;
  }
  auto& status = statuses_[next_];
  if (status.ok()) {
    key_ = keys_[next_];
    value_ = values_[next_];
    reader_.reset(*schemas_, value_);
    if (!reader_ ||
        (ttl_.has_value() &&
         CommonUtils::checkDataExpiredForTTL(
             schemas_->back().get(), reader_.get(), ttl_.value().first, ttl_.value().second))) {
      reader_.reset();
      valid_ = false;
    } else {
      valid_ = true;
    }
  } else if (status.isKeyNotFound()) {
    key_ = keys_[next_];
    value_ = "";
    valid_ = false;
  } else {
    CHECK(false);
  }
  return true;
}

void TagScan::next() {
  next_++;
}

nebula::cpp2::ErrorCode TagScan::collectTagPropsIfValid(NullHandler nullHandler,
                                                        PropHandler valueHandler) {
  if (!valid_) {
    return nullHandler(props_);
  }
  return valueHandler(key_, reader_.get(), props_);
}
folly::StringPiece TagScan::key() const {
  return key_;
}

folly::StringPiece TagScan::val() const {
  return value_;
}
RowReader* TagScan::reader() const {
  return reader_.get();
}
const std::string& TagScan::getTagName() const {
  return tagName_;
}
TagID TagScan::tagId() const {
  return tagId_;
}

GetTagPropNode3::GetTagPropNode3(RuntimeContext* context,
                                 std::vector<TagScan*> tagScans,
                                 nebula::DataSet* resultDataSet,
                                 Expression* filter,
                                 std::size_t limit)
    : context_(context),
      tagScans_(std::move(tagScans)),
      resultDataSet_(resultDataSet),
      expCtx_(filter == nullptr
                  ? nullptr
                  : new StorageExpressionContext(context->vIdLen(), context->isIntId())),
      filter_(filter),
      limit_(limit) {
  name_ = "GetTagPropNode3";
}

nebula::cpp2::ErrorCode GetTagPropNode3::doExecute(PartitionID partId,
                                                   const std::vector<VertexID>& vIds) {
  if (resultDataSet_->size() >= limit_) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  auto ret = RelNode::doExecute(partId, vIds);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << static_cast<int>(ret);
    return ret;
  }
  auto vIdLen = context_->vIdLen();
  auto isIntId = context_->isIntId();

  for (size_t i = 0; i < vIds.size() && resultDataSet_->size() < limit_; i++) {
    List row;
    // vertexId is the first column
    if (context_->isIntId()) {
      row.emplace_back(*reinterpret_cast<const int64_t*>(vIds[i].data()));
    } else {
      row.emplace_back(vIds[i]);
    }
    bool isEmpty = true;

    for (auto tagScan : tagScans_) {
      bool validResult = tagScan->valid();
      CHECK(validResult);
      ret = tagScan->collectTagPropsIfValid(
          [&row, tagScan, this](const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            for (const auto& prop : *props) {
              if (prop.returned_) {
                row.emplace_back(Value());
              }
              if (prop.filtered_ && expCtx_ != nullptr) {
                expCtx_->setTagProp(tagScan->getTagName(), prop.name_, Value());
              }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          },
          [&row, &isEmpty, vIdLen, isIntId, tagScan, this](
              folly::StringPiece key,
              RowReader* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            isEmpty = false;
            auto status = QueryUtils::collectVertexProps(
                key, vIdLen, isIntId, reader, props, row, expCtx_.get(), tagScan->getTagName());
            if (!status.ok()) {
              LOG(INFO) << "E_TAG_PROP_NOT_FOUND";
              return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << static_cast<int>(ret);
        return ret;
      }
      tagScan->next();
    }

    if (isEmpty) {
      continue;
    }

    if (filter_ == nullptr) {
      resultDataSet_->rows.emplace_back(std::move(row));
    } else {
      auto result = QueryUtils::vTrue(filter_->eval(*expCtx_));
      if (result.ok() && result.value()) {
        resultDataSet_->rows.emplace_back(std::move(row));
      }
    }
    if (expCtx_ != nullptr) {
      expCtx_->clear();
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

EdgeScan::EdgeScan(RuntimeContext* context,
                   EdgeContext* edgeContext,
                   EdgeType edgeType,
                   const std::vector<PropContext>* props)
    : context_(context), edgeContext_(edgeContext), edgeType_(edgeType), props_(props) {
  auto schemaIter = edgeContext_->schemas_.find(std::abs(edgeType_));
  CHECK(schemaIter != edgeContext_->schemas_.end());
  CHECK(!schemaIter->second.empty());
  schemas_ = &(schemaIter->second);
  ttl_ = QueryUtils::getEdgeTTLInfo(edgeContext_, std::abs(edgeType_));
  edgeName_ = edgeContext_->edgeNames_[edgeType_];
  name_ = "EdgeScan";
}

const std::string& EdgeScan::getEdgeName() const {
  return edgeName_;
}

EdgeType EdgeScan::edgeType() const {
  return edgeType_;
}

nebula::cpp2::ErrorCode EdgeScan::collectEdgePropsIfValid(NullHandler nullHandler,
                                                          PropHandler valueHandler) {
  if (!valid_) {
    return nullHandler(props_);
  }
  return valueHandler(this->key(), this->reader(), props_);
}

void EdgeScan::next() {
  next_++;
}

bool EdgeScan::valid() const {
  if (next_ >= keys_.size()) {
    valid_ = false;
    return false;
  }
  auto& status = statuses_[next_];
  if (status.ok()) {
    key_ = keys_[next_];
    val_ = values_[next_];
    reader_.reset(*schemas_, val_);
    if (!reader_ ||
        (ttl_.has_value() &&
         CommonUtils::checkDataExpiredForTTL(
             schemas_->back().get(), reader_.get(), ttl_.value().first, ttl_.value().second))) {
      reader_.reset();
      valid_ = false;
    } else {
      valid_ = true;
    }
  } else if (status.isKeyNotFound()) {
    key_ = keys_[next_];
    val_ = "";
    valid_ = false;
  } else {
    CHECK(false);
  }
  return true;
}

folly::StringPiece EdgeScan::key() const {
  return key_;
}

folly::StringPiece EdgeScan::val() const {
  return val_;
}

RowReader* EdgeScan::reader() const {
  return reader_.get();
}

nebula::cpp2::ErrorCode EdgeScan::doExecute(PartitionID partId,
                                            const std::vector<cpp2::EdgeKey>& edgeKeys) {
  {
    auto ret = RelNode::doExecute(partId, edgeKeys);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
  }

  std::vector<std::string> keys;
  for (auto& edgeKey : edgeKeys) {
    std::string key;
    if (*edgeKey.edge_type_ref() != edgeType_) {
      key = "";
    } else {
      key = NebulaKeyUtils::edgeKey(context_->vIdLen(),
                                    partId,
                                    (*edgeKey.src_ref()).getStr(),
                                    *edgeKey.edge_type_ref(),
                                    *edgeKey.ranking_ref(),
                                    (*edgeKey.dst_ref()).getStr());
    }
    keys.emplace_back(std::move(key));
  }

  std::vector<std::string> values;
  auto ret = context_->env()->kvstore_->multiGet(context_->spaceId(), partId, keys, &values);
  if (ret.first != nebula::cpp2::ErrorCode::SUCCEEDED &&
      ret.first != nebula::cpp2::ErrorCode::E_PARTIAL_RESULT) {
    LOG(INFO) << static_cast<int>(ret.first);
    return ret.first;
  }
  keys_ = std::move(keys);
  values_ = std::move(values);
  statuses_ = std::move(ret.second);
  next_ = 0;
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

GetEdgePropNode3::GetEdgePropNode3(RuntimeContext* context,
                                   std::vector<EdgeScan*> edgeScans,
                                   nebula::DataSet* resultDataSet,
                                   Expression* filter,
                                   std::size_t limit)
    : context_(context),
      edgeScans_(std::move(edgeScans)),
      resultDataSet_(resultDataSet),
      expCtx_(filter == nullptr
                  ? nullptr
                  : new StorageExpressionContext(context->vIdLen(), context->isIntId())),
      filter_(filter),
      limit_(limit) {
  name_ = "GetEdgePropNode";
}

nebula::cpp2::ErrorCode GetEdgePropNode3::doExecute(PartitionID partId,
                                                    const std::vector<cpp2::EdgeKey>& edgeKeys) {
  if (resultDataSet_->size() >= limit_) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  auto ret = RelNode::doExecute(partId, edgeKeys);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }

  auto vIdLen = context_->vIdLen();
  auto isIntId = context_->isIntId();
  for (size_t i = 0; i < edgeKeys.size() && resultDataSet_->size() < limit_; i++) {
    List row;
    for (auto edgeScan : edgeScans_) {
      bool validResult = edgeScan->valid();
      CHECK(validResult);
      ret = edgeScan->collectEdgePropsIfValid(
          [&row, edgeScan, this](const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            for (const auto& prop : *props) {
              if (prop.returned_) {
                row.emplace_back(Value());
              }
              if (prop.filtered_ && expCtx_ != nullptr) {
                expCtx_->setEdgeProp(edgeScan->getEdgeName(), prop.name_, Value());
              }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          },
          [&row, vIdLen, isIntId, edgeScan, this](
              folly::StringPiece key,
              RowReader* reader,
              const std::vector<PropContext>* props) -> nebula::cpp2::ErrorCode {
            auto status = QueryUtils::collectEdgeProps(
                key, vIdLen, isIntId, reader, props, row, expCtx_.get(), edgeScan->getEdgeName());
            if (!status.ok()) {
              return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
          });
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << static_cast<int>(ret);
        return ret;
      }
      edgeScan->next();
    }
    if (filter_ == nullptr) {
      resultDataSet_->rows.emplace_back(std::move(row));
    } else {
      auto result = QueryUtils::vTrue(filter_->eval(*expCtx_));
      if (result.ok() && result.value()) {
        resultDataSet_->rows.emplace_back(std::move(row));
      }
    }
    if (expCtx_ != nullptr) {
      expCtx_->clear();
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace nebula::storage
