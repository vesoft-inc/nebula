/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_PROCESSORS_BASEPROCESSOR_INL_H
#define META_PROCESSORS_BASEPROCESSOR_INL_H

#include "interface/gen-cpp2/storage_types.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::doSyncPut(std::vector<kvstore::KV> data) {
  folly::Baton<true, std::atomic> baton;
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiPut(kDefaultSpaceId,
                          kDefaultPartId,
                          std::move(data),
                          [&ret, &baton](nebula::cpp2::ErrorCode code) {
                            if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                              ret = code;
                              VLOG(2) << "Put data error on meta server";
                            }
                            baton.post();
                          });
  baton.wait();
  return ret;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<kvstore::KVIterator>>
BaseProcessor<RESP>::doPrefix(const std::string& key, bool canReadFromFollower) {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, key, &iter, canReadFromFollower);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "Prefix Failed";
    return code;
  }
  return iter;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, std::string> BaseProcessor<RESP>::doGet(const std::string& key) {
  std::string value;
  auto code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "Get Failed";
    return code;
  }
  return value;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> BaseProcessor<RESP>::doMultiGet(
    const std::vector<std::string>& keys) {
  std::vector<std::string> values;
  auto ret = kvstore_->multiGet(kDefaultSpaceId, kDefaultPartId, keys, &values);
  auto code = ret.first;
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "MultiGet Failed";
    return code;
  }
  return values;
}

template <typename RESP>
void BaseProcessor<RESP>::doRemove(const std::string& key) {
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncRemove(
      kDefaultSpaceId, kDefaultPartId, key, [this, &baton](nebula::cpp2::ErrorCode code) {
        this->handleErrorCode(code);
        baton.post();
      });
  baton.wait();
  this->onFinished();
}

template <typename RESP>
void BaseProcessor<RESP>::doBatchOperation(std::string batchOp) {
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncAppendBatch(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(batchOp),
                             [this, &baton](nebula::cpp2::ErrorCode code) {
                               this->handleErrorCode(code);
                               baton.post();
                             });
  baton.wait();
  this->onFinished();
}

template <typename RESP>
void BaseProcessor<RESP>::doRemoveRange(const std::string& start, const std::string& end) {
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncRemoveRange(
      kDefaultSpaceId, kDefaultPartId, start, end, [this, &baton](nebula::cpp2::ErrorCode code) {
        this->handleErrorCode(code);
        baton.post();
      });
  baton.wait();
  this->onFinished();
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, int32_t> BaseProcessor<RESP>::autoIncrementId() {
  const std::string kIdKey = MetaKeyUtils::idKey();
  int32_t id;
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, kIdKey, &val);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }
    id = 1;
  } else {
    id = *reinterpret_cast<const int32_t*>(val.c_str()) + 1;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(kIdKey, std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
      });
  baton.wait();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  } else {
    return id;
  }
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, int32_t> BaseProcessor<RESP>::getAvailableGlobalId() {
  // A read lock has been added before call
  static const std::string kIdKey = MetaKeyUtils::idKey();
  int32_t id;
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, kIdKey, &val);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }
    id = 1;
  } else {
    id = *reinterpret_cast<const int32_t*>(val.c_str()) + 1;
  }

  return id;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, int32_t> BaseProcessor<RESP>::autoIncrementIdInSpace(
    GraphSpaceID spaceId) {
  auto localIdkey = MetaKeyUtils::localIdKey(spaceId);
  int32_t id;
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, localIdkey, &val);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }

    // In order to be compatible with the existing old schema, and simple to implement,
    // when the local_id record does not exist in space, directly use the smallest
    // id available globally.
    auto globalIdRet = getAvailableGlobalId();
    if (!nebula::ok(globalIdRet)) {
      return nebula::error(globalIdRet);
    }
    id = nebula::value(globalIdRet);
  } else {
    id = *reinterpret_cast<const int32_t*>(val.c_str()) + 1;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(localIdkey, std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
      });
  baton.wait();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  } else {
    return id;
  }
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::spaceExist(GraphSpaceID spaceId) {
  auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
  auto ret = doGet(std::move(spaceKey));
  if (nebula::ok(ret)) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::userExist(const std::string& account) {
  auto userKey = MetaKeyUtils::userKey(account);
  auto ret = doGet(std::move(userKey));
  if (nebula::ok(ret)) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_USER_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::machineExist(const std::string& machineKey) {
  auto ret = doGet(machineKey);
  if (nebula::ok(ret)) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  return nebula::error(ret);
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::hostExist(const std::string& hostKey) {
  auto ret = doGet(hostKey);
  if (nebula::ok(ret)) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  return nebula::error(ret);
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::includeByZone(const std::vector<HostAddr>& hosts) {
  const auto& prefix = MetaKeyUtils::zonePrefix();
  auto iterRet = doPrefix(prefix);
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  if (!nebula::ok(iterRet)) {
    code = nebula::error(iterRet);
    LOG(INFO) << "Get zones failed, error: " << apache::thrift::util::enumNameSafe(code);
    return code;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto name = MetaKeyUtils::parseZoneName(iter->key());
    auto zoneHosts = MetaKeyUtils::parseZoneHosts(iter->val());
    for (const auto& host : hosts) {
      if (std::find(zoneHosts.begin(), zoneHosts.end(), host) != zoneHosts.end()) {
        LOG(INFO) << "Host overlap found in zone " << name;
        code = nebula::cpp2::ErrorCode::E_CONFLICT;
        break;
      }
    }

    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      break;
    }
    iter->next();
  }

  return code;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> BaseProcessor<RESP>::getSpaceId(
    const std::string& name) {
  auto indexKey = MetaKeyUtils::indexSpaceKey(name);
  auto ret = doGet(indexKey);
  if (nebula::ok(ret)) {
    return *reinterpret_cast<const GraphSpaceID*>(nebula::value(ret).c_str());
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, TagID> BaseProcessor<RESP>::getTagId(GraphSpaceID spaceId,
                                                                      const std::string& name) {
  auto indexKey = MetaKeyUtils::indexTagKey(spaceId, name);
  std::string val;
  auto ret = doGet(std::move(indexKey));
  if (nebula::ok(ret)) {
    return *reinterpret_cast<const TagID*>(nebula::value(ret).c_str());
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, EdgeType> BaseProcessor<RESP>::getEdgeType(
    GraphSpaceID spaceId, const std::string& name) {
  auto indexKey = MetaKeyUtils::indexEdgeKey(spaceId, name);
  auto ret = doGet(std::move(indexKey));
  if (nebula::ok(ret)) {
    return *reinterpret_cast<const EdgeType*>(nebula::value(ret).c_str());
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema> BaseProcessor<RESP>::getLatestTagSchema(
    GraphSpaceID spaceId, const TagID tagId) {
  const auto& key = MetaKeyUtils::schemaTagPrefix(spaceId, tagId);
  auto ret = doPrefix(key);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Tag Prefix " << key << " failed";
    return nebula::error(ret);
  }

  auto iter = nebula::value(ret).get();
  if (iter->valid()) {
    return MetaKeyUtils::parseSchema(iter->val());
  } else {
    LOG(INFO) << "Tag Prefix " << key << " not found";
    return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
  }
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema> BaseProcessor<RESP>::getLatestEdgeSchema(
    GraphSpaceID spaceId, const EdgeType edgeType) {
  const auto& key = MetaKeyUtils::schemaEdgePrefix(spaceId, edgeType);
  auto ret = doPrefix(key);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Edge Prefix " << key << " failed";
    return nebula::error(ret);
  }

  auto iter = nebula::value(ret).get();
  if (iter->valid()) {
    return MetaKeyUtils::parseSchema(iter->val());
  } else {
    LOG(INFO) << "Edge Prefix " << key << " not found";
    return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
  }
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, IndexID> BaseProcessor<RESP>::getIndexID(
    GraphSpaceID spaceId, const std::string& indexName) {
  auto indexKey = MetaKeyUtils::indexIndexKey(spaceId, indexName);
  auto ret = doGet(indexKey);
  if (nebula::ok(ret)) {
    return *reinterpret_cast<const IndexID*>(nebula::value(ret).c_str());
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, bool> BaseProcessor<RESP>::checkPassword(
    const std::string& account, const std::string& password) {
  auto userKey = MetaKeyUtils::userKey(account);
  auto ret = doGet(userKey);
  if (nebula::ok(ret)) {
    return MetaKeyUtils::parseUserPwd(nebula::value(ret)) == password;
  }
  return nebula::error(ret);
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::IndexItem>> BaseProcessor<RESP>::getIndexes(
    GraphSpaceID spaceId, int32_t tagOrEdge) {
  std::vector<cpp2::IndexItem> items;
  const auto& indexPrefix = MetaKeyUtils::indexPrefix(spaceId);
  auto iterRet = doPrefix(indexPrefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Tag or edge index prefix failed, error :"
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto indexIter = nebula::value(iterRet).get();

  while (indexIter->valid()) {
    auto item = MetaKeyUtils::parseIndex(indexIter->val());
    if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::tag_id &&
        item.get_schema_id().get_tag_id() == tagOrEdge) {
      items.emplace_back(std::move(item));
    } else if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type &&
               item.get_schema_id().get_edge_type() == tagOrEdge) {
      items.emplace_back(std::move(item));
    }
    indexIter->next();
  }
  return items;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, cpp2::FTIndex> BaseProcessor<RESP>::getFTIndex(
    GraphSpaceID spaceId, int32_t tagOrEdge) {
  const auto& indexPrefix = MetaKeyUtils::fulltextIndexPrefix();
  auto iterRet = doPrefix(indexPrefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Tag or edge fulltext index prefix failed, error :"
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto indexIter = nebula::value(iterRet).get();

  while (indexIter->valid()) {
    auto index = MetaKeyUtils::parsefulltextIndex(indexIter->val());
    auto id = index.get_depend_schema().getType() == nebula::cpp2::SchemaID::Type::edge_type
                  ? index.get_depend_schema().get_edge_type()
                  : index.get_depend_schema().get_tag_id();
    if (spaceId == index.get_space_id() && tagOrEdge == id) {
      return index;
    }
    indexIter->next();
  }
  return nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND;
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::indexCheck(
    const std::vector<cpp2::IndexItem>& items,
    const std::vector<cpp2::AlterSchemaItem>& alterItems) {
  for (const auto& index : items) {
    for (const auto& tagItem : alterItems) {
      if (*tagItem.op_ref() == nebula::meta::cpp2::AlterSchemaOp::CHANGE ||
          *tagItem.op_ref() == nebula::meta::cpp2::AlterSchemaOp::DROP) {
        const auto& tagCols = tagItem.get_schema().get_columns();
        const auto& indexCols = index.get_fields();
        for (const auto& tCol : tagCols) {
          auto it = std::find_if(indexCols.begin(), indexCols.end(), [&](const auto& iCol) {
            return tCol.name == iCol.name;
          });
          if (it != indexCols.end()) {
            LOG(INFO) << "Index conflict, index :" << index.get_index_name()
                      << ", column : " << tCol.name;
            return nebula::cpp2::ErrorCode::E_CONFLICT;
          }
        }
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::ftIndexCheck(
    const std::vector<std::string>& cols, const std::vector<cpp2::AlterSchemaItem>& alterItems) {
  for (const auto& item : alterItems) {
    if (*item.op_ref() == nebula::meta::cpp2::AlterSchemaOp::CHANGE ||
        *item.op_ref() == nebula::meta::cpp2::AlterSchemaOp::DROP) {
      const auto& itemCols = item.get_schema().get_columns();
      for (const auto& iCol : itemCols) {
        auto it =
            std::find_if(cols.begin(), cols.end(), [&](const auto& c) { return c == iCol.name; });
        if (it != cols.end()) {
          LOG(INFO) << "fulltext index conflict";
          return nebula::cpp2::ErrorCode::E_CONFLICT;
        }
      }
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
bool BaseProcessor<RESP>::checkIndexExist(const std::vector<cpp2::IndexFieldDef>& fields,
                                          const cpp2::IndexItem& item) {
  const auto& itemFields = item.get_fields();
  if (fields.size() != itemFields.size()) {
    return false;
  }
  for (size_t i = 0; i < fields.size(); i++) {
    if (fields[i].get_name() != itemFields[i].get_name()) {
      return false;
    }
  }
  LOG(INFO) << "Index " << item.get_index_name() << " has existed";
  return true;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, ZoneID> BaseProcessor<RESP>::getZoneId(
    const std::string& zoneName) {
  auto indexKey = MetaKeyUtils::indexZoneKey(zoneName);
  auto ret = doGet(std::move(indexKey));
  if (nebula::ok(ret)) {
    return *reinterpret_cast<const ZoneID*>(nebula::value(ret).c_str());
  }
  auto retCode = nebula::error(ret);
  if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    retCode = nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND;
  }
  return retCode;
}

template <typename RESP>
nebula::cpp2::ErrorCode BaseProcessor<RESP>::listenerExist(GraphSpaceID space,
                                                           cpp2::ListenerType type) {
  const auto& prefix = MetaKeyUtils::listenerPrefix(space, type);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    return nebula::error(ret);
  }

  auto iterRet = nebula::value(ret).get();
  if (!iterRet->valid()) {
    return nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID, std::vector<HostAddr>>>
BaseProcessor<RESP>::getAllParts(GraphSpaceID spaceId) {
  std::unordered_map<PartitionID, std::vector<HostAddr>> partHostsMap;

  const auto& prefix = MetaKeyUtils::partPrefix(spaceId);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List Parts Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    std::vector<HostAddr> partHosts = MetaKeyUtils::parsePartVal(iter->val());
    partHostsMap.emplace(partId, std::move(partHosts));
    iter->next();
  }

  return partHostsMap;
}

}  // namespace meta
}  // namespace nebula
#endif
