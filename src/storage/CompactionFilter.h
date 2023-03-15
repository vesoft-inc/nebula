/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_COMPACTIONFILTER_H_
#define STORAGE_COMPACTIONFILTER_H_

#include "codec/RowReaderWrapper.h"
#include "common/base/Base.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "common/utils/IndexKeyUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "common/utils/OperationKeyUtils.h"
#include "kvstore/CompactionFilter.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageCompactionFilter final : public kvstore::KVFilter {
 public:
  StorageCompactionFilter(meta::SchemaManager* schemaMan,
                          meta::IndexManager* indexMan,
                          size_t vIdLen)
      : schemaMan_(schemaMan), indexMan_(indexMan), vIdLen_(vIdLen) {
    CHECK_NOTNULL(schemaMan_);
  }

  bool filter(GraphSpaceID spaceId,
              const folly::StringPiece& key,
              const folly::StringPiece& val) const override {
    if (NebulaKeyUtils::isTag(vIdLen_, key)) {
      return !tagValid(spaceId, key, val);
    } else if (NebulaKeyUtils::isEdge(vIdLen_, key)) {
      return !edgeValid(spaceId, key, val);
    } else if (IndexKeyUtils::isIndexKey(key)) {
      return !indexValid(spaceId, key, val);
    } else if (!FLAGS_use_vertex_key && NebulaKeyUtils::isVertex(key)) {
      return true;
    } else if (NebulaKeyUtils::isLock(vIdLen_, key)) {
      return !lockValid(spaceId, key);
    } else {
      // skip uuid/system/operation
      VLOG(3) << "Skip the system key inside, key " << key;
    }
    return false;
  }

 private:
  bool tagValid(GraphSpaceID spaceId,
                const folly::StringPiece& key,
                const folly::StringPiece& val) const {
    auto tagId = NebulaKeyUtils::getTagId(vIdLen_, key);
    auto schema = schemaMan_->getTagSchema(spaceId, tagId);
    if (!schema) {
      VLOG(3) << "Space " << spaceId << ", Tag " << tagId << " invalid";
      return false;
    }
    auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId, tagId, val);
    if (reader == nullptr) {
      VLOG(3) << "Remove the bad format vertex";
      return false;
    }
    if (ttlExpired(schema.get(), reader.get())) {
      VLOG(3) << "Ttl expired";
      return false;
    }
    return true;
  }

  bool edgeValid(GraphSpaceID spaceId,
                 const folly::StringPiece& key,
                 const folly::StringPiece& val) const {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
    if (edgeType < 0 && val.empty()) {
      VLOG(3) << "Invalid reverse edge key";
      return false;
    }
    auto schema = schemaMan_->getEdgeSchema(spaceId, std::abs(edgeType));
    if (!schema) {
      VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
      return false;
    }
    auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId, std::abs(edgeType), val);
    if (reader == nullptr) {
      VLOG(3) << "Remove the bad format edge!";
      return false;
    }
    if (ttlExpired(schema.get(), reader.get())) {
      VLOG(3) << "Ttl expired";
      return false;
    }
    return true;
  }

  bool lockValid(GraphSpaceID spaceId, const folly::StringPiece& key) const {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
    auto schema = schemaMan_->getEdgeSchema(spaceId, std::abs(edgeType));
    if (!schema) {
      VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
      return false;
    }
    return true;
  }

  // TODO(panda) Optimize the method in the future
  bool ttlExpired(const meta::SchemaProviderIf* schema, nebula::RowReaderWrapper* reader) const {
    if (schema == nullptr) {
      return true;
    }
    auto ttl = CommonUtils::ttlProps(schema);
    // Only support the specified ttl_col mode
    // Not specifying or non-positive ttl_duration behaves like ttl_duration =
    // infinity
    if (!ttl.first) {
      return false;
    }
    return CommonUtils::checkDataExpiredForTTL(schema, reader, ttl.second.second, ttl.second.first);
  }

  bool ttlExpired(const meta::SchemaProviderIf* schema, const Value& v) const {
    if (schema == nullptr) {
      return true;
    }
    auto ttl = CommonUtils::ttlProps(schema);
    if (!ttl.first) {
      return false;
    }
    return CommonUtils::checkDataExpiredForTTL(schema, v, ttl.second.second, ttl.second.first);
  }

  bool indexValid(GraphSpaceID spaceId,
                  const folly::StringPiece& key,
                  const folly::StringPiece& val) const {
    auto indexId = IndexKeyUtils::getIndexId(key);
    auto eRet = indexMan_->getEdgeIndex(spaceId, indexId);
    if (eRet.ok()) {
      if (!val.empty()) {
        auto id = eRet.value()->get_schema_id().get_edge_type();
        auto schema = schemaMan_->getEdgeSchema(spaceId, id);
        if (!schema) {
          VLOG(3) << "Space " << spaceId << ", EdgeType " << id << " invalid";
          return false;
        }
        if (ttlExpired(schema.get(), IndexKeyUtils::parseIndexTTL(val))) {
          return false;
        }
      }
      return true;
    }
    auto tRet = indexMan_->getTagIndex(spaceId, indexId);
    if (tRet.ok()) {
      if (!val.empty()) {
        auto id = tRet.value()->get_schema_id().get_tag_id();
        auto schema = schemaMan_->getTagSchema(spaceId, id);
        if (!schema) {
          VLOG(3) << "Space " << spaceId << ", tagId " << id << " invalid";
          return false;
        }
        if (ttlExpired(schema.get(), IndexKeyUtils::parseIndexTTL(val))) {
          return false;
        }
      }
      return true;
    }
    return !(eRet.status() == Status::IndexNotFound() && tRet.status() == Status::IndexNotFound());
  }

 private:
  meta::SchemaManager* schemaMan_ = nullptr;
  meta::IndexManager* indexMan_ = nullptr;
  size_t vIdLen_;
};

class StorageCompactionFilterFactory final : public kvstore::KVCompactionFilterFactory {
 public:
  StorageCompactionFilterFactory(meta::SchemaManager* schemaMan,
                                 meta::IndexManager* indexMan,
                                 GraphSpaceID spaceId,
                                 size_t vIdLen)
      : KVCompactionFilterFactory(spaceId),
        schemaMan_(schemaMan),
        indexMan_(indexMan),
        vIdLen_(vIdLen) {}

  std::unique_ptr<kvstore::KVFilter> createKVFilter() override {
    return std::make_unique<StorageCompactionFilter>(schemaMan_, indexMan_, vIdLen_);
  }

  const char* Name() const override {
    return "StorageCompactionFilterFactory";
  }

 private:
  meta::SchemaManager* schemaMan_ = nullptr;
  meta::IndexManager* indexMan_ = nullptr;
  size_t vIdLen_;
};

class StorageCompactionFilterFactoryBuilder : public kvstore::CompactionFilterFactoryBuilder {
 public:
  StorageCompactionFilterFactoryBuilder(meta::SchemaManager* schemaMan,
                                        meta::IndexManager* indexMan)
      : schemaMan_(schemaMan), indexMan_(indexMan) {}

  virtual ~StorageCompactionFilterFactoryBuilder() = default;

  std::shared_ptr<kvstore::KVCompactionFilterFactory> buildCfFactory(
      GraphSpaceID spaceId) override {
    auto vIdLen = schemaMan_->getSpaceVidLen(spaceId);
    if (!vIdLen.ok()) {
      return nullptr;
    }
    return std::make_shared<StorageCompactionFilterFactory>(
        schemaMan_, indexMan_, spaceId, vIdLen.value());
  }

 private:
  meta::SchemaManager* schemaMan_ = nullptr;
  meta::IndexManager* indexMan_ = nullptr;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMPACTIONFILTER_H_
