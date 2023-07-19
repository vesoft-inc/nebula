/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/context/StorageExpressionContext.h"

#include "common/utils/DefaultValueContext.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/exec/QueryUtils.h"

namespace nebula {
namespace storage {

void StorageExpressionContext::setInnerVar(const std::string& var, Value val) {
  exprValueMap_[var] = std::move(val);
}

const Value& StorageExpressionContext::getInnerVar(const std::string& var) const {
  auto it = exprValueMap_.find(var);
  if (it == exprValueMap_.end()) {
    return Value::kEmpty;
  }
  return it->second;
}

Value StorageExpressionContext::readValue(const std::string& propName) const {
  if (!schema_) {
    return Value::kNullValue;
  }

  auto ret = QueryUtils::readValue(reader_, propName, schema_);
  if (!ret.ok()) {
    return Value::kNullValue;
  }

  return std::move(ret).value();
}

// Get the specified property from the tag, such as tag_name.prop_name
Value StorageExpressionContext::getTagProp(const std::string& tagName,
                                           const std::string& prop) const {
  if (isIndex_) {
    return getIndexValue(prop, false);
  }
  return getSrcProp(tagName, prop);
}

// Get the specified property from the edge, such as edgename.prop_name
Value StorageExpressionContext::getEdgeProp(const std::string& edgeName,
                                            const std::string& prop) const {
  if (isIndex_) {
    return getIndexValue(prop, true);
  }
  if (isEdge_ && reader_ != nullptr) {
    if (edgeName != name_) {
      return Value::kEmpty;
    }
    if (prop == kSrc) {
      auto srcId = NebulaKeyUtils::getSrcId(vIdLen_, key_);
      if (isIntId_) {
        int64_t val;
        memcpy(reinterpret_cast<void*>(&val), srcId.begin(), sizeof(int64_t));
        return val;
      } else {
        return srcId.subpiece(0, srcId.find_first_of('\0')).toString();
      }
    } else if (prop == kDst) {
      auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key_);
      if (isIntId_) {
        int64_t val;
        memcpy(reinterpret_cast<void*>(&val), dstId.begin(), sizeof(int64_t));
        return val;
      } else {
        return dstId.subpiece(0, dstId.find_first_of('\0')).toString();
      }
    } else if (prop == kRank) {
      return NebulaKeyUtils::getRank(vIdLen_, key_);
    } else if (prop == kType) {
      return NebulaKeyUtils::getEdgeType(vIdLen_, key_);
    } else {
      return readValue(prop);
    }
  } else {
    auto iter = edgeFilters_.find(std::make_pair(edgeName, prop));
    if (iter == edgeFilters_.end()) {
      return Value::kEmpty;
    }
    VLOG(1) << "Hit srcProp filter for edge " << edgeName << ", prop " << prop;
    return iter->second;
  }
}

Value StorageExpressionContext::getSrcProp(const std::string& tagName,
                                           const std::string& prop) const {
  if (!isEdge_ && reader_ != nullptr) {
    if (tagName != name_) {
      return Value::kEmpty;
    }
    if (prop == kVid) {
      auto vId = NebulaKeyUtils::getVertexId(vIdLen_, key_);
      if (isIntId_) {
        int64_t val;
        memcpy(reinterpret_cast<void*>(&val), vId.begin(), sizeof(int64_t));
        return val;
      } else {
        return vId.subpiece(0, vId.find_first_of('\0')).toString();
      }
    } else if (prop == kTag) {
      return NebulaKeyUtils::getTagId(vIdLen_, key_);
    } else {
      return readValue(prop);
    }
  } else {
    auto iter = tagFilters_.find(std::make_pair(tagName, prop));
    if (iter == tagFilters_.end()) {
      return Value::kEmpty;
    }
    VLOG(1) << "Hit srcProp filter for tag " << tagName << ", prop " << prop;
    return iter->second;
  }
}

Value StorageExpressionContext::getIndexValue(const std::string& prop, bool isEdge) const {
  // Handle string type values.
  // when field type is FIXED_STRING type,
  // actual length of the value is called "len"
  // FIXED_STRING.type.len is called "fixed_len"
  // if (len > fixed_len) : v = value.substr(0, fixed_len)
  // if (len < fixed_len) : v.append(fixed_len - len, '\0')
  return IndexKeyUtils::getValueFromIndexKey(vIdLen_, key_, prop, fields_, isEdge, hasNullableCol_);
}

}  // namespace storage
}  // namespace nebula
