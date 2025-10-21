/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_EXEC_QUERYUTILS_H_
#define STORAGE_EXEC_QUERYUTILS_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/utils/DefaultValueContext.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/CommonUtils.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class QueryUtils final {
 public:
  // The behavior keep same with filter executor
  static inline StatusOr<bool> vTrue(const Value& val) {
    if (val.isBadNull() || (!val.empty() && !val.isBool() && !val.isNull())) {
      return Status::Error("Wrong type result, the type should be NULL, EMPTY or BOOL");
    }
    if (val.empty() || val.isNull() || !val.getBool()) {
      return false;
    } else {
      return true;
    }
  }

  enum class ReturnColType : uint16_t {
    kVid,
    kTag,
    kSrc,
    kType,
    kRank,
    kDst,
    kDis,
    kOther,
  };
  /**
   * @brief Get return col type by name
   *
   * @param name
   * @return ReturnColType
   * @see ReturnColType
   */
  static ReturnColType toReturnColType(const std::string& name) {
    if (name == kVid) {
      return ReturnColType::kVid;
    } else if (name == kTag) {
      return ReturnColType::kTag;
    } else if (name == kSrc) {
      return ReturnColType::kSrc;
    } else if (name == kType) {
      return ReturnColType::kType;
    } else if (name == kRank) {
      return ReturnColType::kRank;
    } else if (name == kDst) {
      return ReturnColType::kDst;
    } else if (name == kDis) {
      return ReturnColType::kDis;
    } else {
      return ReturnColType::kOther;
    }
  }
  /**
   * @brief Get value with propName from reader
   *
   * @param reader Value set
   * @param propName Filed name
   * @param field Field definition
   * @return StatusOr<nebula::Value>
   */
  static StatusOr<nebula::Value> readValue(RowReaderWrapper* reader,
                                           const std::string& propName,
                                           const meta::NebulaSchemaProvider::SchemaField* field) {
    auto value = reader->getValueByName(propName);
    if (value.type() == Value::Type::NULLVALUE) {
      // read null value
      auto nullType = value.getNull();

      if (nullType == NullType::UNKNOWN_PROP) {
        VLOG(1) << "Fail to read prop " << propName;
        if (!field) {
          return value;
        }
        if (field->hasDefault()) {
          DefaultValueContext expCtx;
          ObjectPool pool;
          auto& exprStr = field->defaultValue();
          auto expr = Expression::decode(&pool, folly::StringPiece(exprStr.data(), exprStr.size()));
          return Expression::eval(expr, expCtx);
        } else if (field->nullable()) {
          return NullType::__NULL__;
        }
      } else if (nullType == NullType::__NULL__) {
        // Need to check whether the field is nullable
        if (field->nullable()) {
          return value;
        }
      }
      return Status::Error(folly::stringPrintf("Fail to read prop %s ", propName.c_str()));
    }
    if (field->type() == nebula::cpp2::PropertyType::FIXED_STRING) {
      const auto& fixedStr = value.getStr();
      return fixedStr.substr(0, fixedStr.find_first_of('\0'));
    }
    return value;
  }

  static StatusOr<nebula::Value> readVectorValue(
      RowReaderWrapper* reader,
      const std::string& propName,
      const meta::NebulaSchemaProvider::SchemaField* field) {
    auto value = reader->getVectorValueByName(propName);
    if (value.type() == Value::Type::NULLVALUE) {
      // read null value
      auto nullType = value.getNull();

      if (nullType == NullType::UNKNOWN_PROP) {
        VLOG(1) << "Fail to read prop " << propName;
        if (!field) {
          return value;
        }
        if (field->hasDefault()) {
          DefaultValueContext expCtx;
          ObjectPool pool;
          auto& exprStr = field->defaultValue();
          auto expr = Expression::decode(&pool, folly::StringPiece(exprStr.data(), exprStr.size()));
          return Expression::eval(expr, expCtx);
        } else if (field->nullable()) {
          return NullType::__NULL__;
        }
      } else if (nullType == NullType::__NULL__) {
        // Need to check whether the field is nullable
        if (field->nullable()) {
          return value;
        }
      }
      return Status::Error(folly::stringPrintf("Fail to read prop %s ", propName.c_str()));
    }
    return value;
  }

  /**
   * @brief read prop value, If the RowReader contains this field, read from the rowreader,
   * otherwise read the default value or null value from the latest schema
   *
   * @param reader
   * @param propName
   * @param schema
   * @return StatusOr<nebula::Value>
   */
  static StatusOr<nebula::Value> readValue(RowReaderWrapper* reader,
                                           const std::string& propName,
                                           const meta::NebulaSchemaProvider* schema) {
    auto field = schema->field(propName);
    if (!field) {
      return Status::Error(folly::stringPrintf("Fail to read prop %s ", propName.c_str()));
    }
    return readValue(reader, propName, field);
  }
  static StatusOr<nebula::Value> readVectorValue(RowReaderWrapper* reader,
                                                 const std::string& propName,
                                                 const meta::NebulaSchemaProvider* schema) {
    auto field = schema->vectorField(propName);
    if (!field) {
      return Status::Error(folly::stringPrintf("Fail to read prop %s ", propName.c_str()));
    }
    return readVectorValue(reader, propName, field);
  }

  static StatusOr<nebula::Value> readEdgeProp(folly::StringPiece key,
                                              size_t vIdLen,
                                              bool isIntId,
                                              RowReaderWrapper* reader,
                                              const PropContext& prop) {
    switch (prop.propInKeyType_) {
      // prop in value
      case PropContext::PropInKeyType::NONE: {
        return readValue(reader, prop.name_, prop.field_);
      }
      case PropContext::PropInKeyType::SRC: {
        auto srcId = NebulaKeyUtils::getSrcId(vIdLen, key);
        if (isIntId) {
          return *reinterpret_cast<const int64_t*>(srcId.data());
        } else {
          return srcId.subpiece(0, srcId.find_first_of('\0')).toString();
        }
      }
      case PropContext::PropInKeyType::TYPE: {
        auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen, key);
        return edgeType;
      }
      case PropContext::PropInKeyType::RANK: {
        auto edgeRank = NebulaKeyUtils::getRank(vIdLen, key);
        return edgeRank;
      }
      case PropContext::PropInKeyType::DST: {
        auto dstId = NebulaKeyUtils::getDstId(vIdLen, key);
        if (isIntId) {
          return *reinterpret_cast<const int64_t*>(dstId.data());
        } else {
          return dstId.subpiece(0, dstId.find_first_of('\0')).toString();
        }
      }
      default:
        LOG(FATAL) << "Should not read here";
    }
    return Status::Error(folly::stringPrintf("Invalid property %s", prop.name_.c_str()));
  }

  static StatusOr<nebula::Value> readVertexProp(folly::StringPiece key,
                                                size_t vIdLen,
                                                bool isIntId,
                                                RowReaderWrapper* reader,
                                                const PropContext& prop) {
    switch (prop.propInKeyType_) {
      // prop in value
      case PropContext::PropInKeyType::NONE: {
        if (NebulaKeyUtils::isVector(key)) {
          return readVectorValue(reader, prop.name_, prop.field_);
        }
        return readValue(reader, prop.name_, prop.field_);
      }
      case PropContext::PropInKeyType::VID: {
        auto vId = NebulaKeyUtils::getVertexId(vIdLen, key);
        if (isIntId) {
          return *reinterpret_cast<const int64_t*>(vId.data());
        } else {
          return vId.subpiece(0, vId.find_first_of('\0')).toString();
        }
      }
      case PropContext::PropInKeyType::TAG: {
        auto tag = NebulaKeyUtils::getTagId(vIdLen, key);
        return tag;
      }
      default:
        LOG(FATAL) << "Should not read here";
    }
    return Status::Error(folly::stringPrintf("Invalid property %s", prop.name_.c_str()));
  }

  static Status collectVertexProps(folly::StringPiece key,
                                   size_t vIdLen,
                                   bool isIntId,
                                   RowReaderWrapper* reader,
                                   const std::vector<PropContext>* props,
                                   nebula::List& list,
                                   StorageExpressionContext* expCtx = nullptr,
                                   const std::string& tagName = "") {
    for (const auto& prop : *props) {
      if (!(prop.returned_ || (prop.filtered_ && expCtx != nullptr)) || prop.isVector()) {
        continue;
      }
      auto value = QueryUtils::readVertexProp(key, vIdLen, isIntId, reader, prop);
      NG_RETURN_IF_ERROR(value);
      if (prop.returned_) {
        VLOG(2) << "Collect prop " << prop.name_;
        list.emplace_back(value.value());
      }
      if (prop.filtered_ && expCtx != nullptr) {
        expCtx->setTagProp(tagName, prop.name_, std::move(value).value());
      }
    }
    return Status::OK();
  }

  static Status collectVertexVectorProp(folly::StringPiece key,
                                        size_t vIdLen,
                                        bool isIntId,
                                        RowReaderWrapper* reader,
                                        PropContext prop,
                                        nebula::List& list,
                                        StorageExpressionContext* expCtx = nullptr,
                                        const std::string& tagName = "") {
    LOG(ERROR) << "collect vector prop: " << prop.name_;
    if (!(prop.returned_ || (prop.filtered_ && expCtx != nullptr))) {
      return Status::OK();
    }
    auto value = QueryUtils::readVertexProp(key, vIdLen, isIntId, reader, prop);
    NG_RETURN_IF_ERROR(value);
    LOG(ERROR) << "Query Collect Vector Prop: " << value.value().toString();
    if (prop.returned_) {
      VLOG(2) << "Collect prop " << prop.name_;
      list.emplace_back(value.value());
    }
    if (prop.filtered_ && expCtx != nullptr) {
      expCtx->setTagProp(tagName, prop.name_, std::move(value).value());
    }
    return Status::OK();
  }

  /**
   * @brief Unified function to collect all vertex properties including regular props,
   *        vector props, and _tag. Handles both cases with and without vector properties.
   *
   * @param key The key of the vertex (for regular properties)
   * @param vectorKeys Vector of keys for vector properties, each vector prop has its own key
   * @param vIdLen Length of vertex ID
   * @param isIntId Whether vertex ID is integer type
   * @param reader Row reader for regular properties
   * @param vectorReaders Vector of row readers for vector properties (can be empty)
   * @param props All properties to collect (including regular, vector, and _tag)
   * @param list Output list to append collected values
   * @param expCtx Expression context for filters
   * @param tagName Tag name for expression context
   * @return Status OK if successful
   */
  static Status collectAllVertexProps(folly::StringPiece key,
                                      const std::vector<folly::StringPiece>& vectorKeys,
                                      const std::vector<int32_t>& vectorIndexes,
                                      size_t vIdLen,
                                      bool isIntId,
                                      RowReaderWrapper* reader,
                                      const std::vector<RowReaderWrapper*>& vectorReaders,
                                      const std::vector<PropContext>* props,
                                      nebula::List& list,
                                      StorageExpressionContext* expCtx = nullptr,
                                      const std::string& tagName = "") {
    LOG(ERROR) << "collectAllVertexProps for tag: " << tagName
               << ", vectorKeys size: " << vectorKeys.size()
               << ", vectorReaders size: " << vectorReaders.size()
               << ", vectorIndexes size: " << vectorIndexes.size()
               << ", props size: " << props->size();

    // Log the order of properties to collect
    std::string propOrder = "Props order: ";
    for (size_t i = 0; i < props->size(); i++) {
      propOrder +=
          props->at(i).name_ + "(isVector:" + (props->at(i).isVector() ? "true" : "false") + ") ";
    }
    LOG(ERROR) << propOrder;

    int propIdx = 0;
    for (const auto& prop : *props) {
      if (!(prop.returned_ || (prop.filtered_ && expCtx != nullptr))) {
        propIdx++;
        continue;
      }

      LOG(ERROR) << "Processing prop[" << propIdx << "]: " << prop.name_
                 << ", isVector: " << prop.isVector() << ", returned: " << prop.returned_;

      // Determine which reader and key to use based on property type
      RowReaderWrapper* propReader = reader;
      folly::StringPiece propKey = key;

      if (prop.isVector() && !vectorReaders.empty()) {
        // For vector properties, we need to find the corresponding vector reader and key
        // vectorIndexes stores the schema field index for each vector property
        // We need to find which position in vectorReaders/vectorKeys corresponds to this prop
        auto schema = reader->getSchema();
        if (schema != nullptr) {
          auto vecFieldIdx = schema->getVectorFieldIndex(prop.name_);
          if (vecFieldIdx >= 0) {
            // Find the position in vectorIndexes array that matches this field index
            auto it = std::find(vectorIndexes.begin(), vectorIndexes.end(), vecFieldIdx);
            if (it != vectorIndexes.end()) {
              auto position = std::distance(vectorIndexes.begin(), it);
              if (static_cast<size_t>(position) < vectorReaders.size()) {
                propReader = vectorReaders[position];
              }
              if (static_cast<size_t>(position) < vectorKeys.size()) {
                propKey = vectorKeys[position];
              }
              LOG(ERROR) << "Vector prop: " << prop.name_ << ", vecFieldIdx: " << vecFieldIdx
                         << ", position in arrays: " << position << ", using vectorKey[" << position
                         << "]";
            }
          }
        }
      }

      auto value = QueryUtils::readVertexProp(propKey, vIdLen, isIntId, propReader, prop);
      NG_RETURN_IF_ERROR(value);

      LOG(ERROR) << "Collect prop[" << propIdx << "]: " << prop.name_
                 << " (isVector: " << prop.isVector() << ")"
                 << ", value: " << value.value().toString();

      if (prop.returned_) {
        VLOG(2) << "Collect prop " << prop.name_;
        list.emplace_back(value.value());
        LOG(ERROR) << "Added to list at position " << (list.size() - 1)
                   << ", current list size: " << list.size();
      }
      if (prop.filtered_ && expCtx != nullptr) {
        expCtx->setTagProp(tagName, prop.name_, std::move(value).value());
      }

      propIdx++;
    }
    return Status::OK();
  }

  static Status collectEdgeProps(folly::StringPiece key,
                                 size_t vIdLen,
                                 bool isIntId,
                                 RowReaderWrapper* reader,
                                 const std::vector<PropContext>* props,
                                 nebula::List& list,
                                 StorageExpressionContext* expCtx = nullptr,
                                 const std::string& edgeName = "") {
    for (const auto& prop : *props) {
      if (!(prop.returned_ || (prop.filtered_ && expCtx != nullptr))) {
        continue;
      }
      auto value = QueryUtils::readEdgeProp(key, vIdLen, isIntId, reader, prop);
      NG_RETURN_IF_ERROR(value);
      if (prop.returned_) {
        VLOG(2) << "Collect prop " << prop.name_;
        list.emplace_back(value.value());
      }
      if (prop.filtered_ && expCtx != nullptr) {
        expCtx->setEdgeProp(edgeName, prop.name_, std::move(value).value());
      }
    }
    return Status::OK();
  }

  /**
   * @brief Get the Edge TTL Info object
   *
   * @param edgeContext
   * @param edgeType
   * @return return none if no valid ttl, else return the ttl property name and time
   */
  static std::optional<std::pair<std::string, int64_t>> getEdgeTTLInfo(EdgeContext* edgeContext,
                                                                       EdgeType edgeType) {
    std::optional<std::pair<std::string, int64_t>> ret;
    auto edgeFound = edgeContext->ttlInfo_.find(std::abs(edgeType));
    if (edgeFound != edgeContext->ttlInfo_.end()) {
      ret.emplace(edgeFound->second.first, edgeFound->second.second);
    }
    return ret;
  }

  /**
   * @brief Get the Tag TTL Info object
   *
   * @param tagContext
   * @param tagId
   * @return return none if no valid ttl, else return the ttl property name and time
   */
  static std::optional<std::pair<std::string, int64_t>> getTagTTLInfo(TagContext* tagContext,
                                                                      TagID tagId) {
    std::optional<std::pair<std::string, int64_t>> ret;
    auto tagFound = tagContext->ttlInfo_.find(tagId);
    if (tagFound != tagContext->ttlInfo_.end()) {
      ret.emplace(tagFound->second.first, tagFound->second.second);
    }
    return ret;
  }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_EXEC_QUERYUTILS_H_
