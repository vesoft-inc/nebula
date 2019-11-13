/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMPACTIONFILTER_H_
#define STORAGE_COMPACTIONFILTER_H_

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include "kvstore/CompactionFilter.h"

DEFINE_bool(storage_kv_mode, false, "True for kv mode");

namespace nebula {
namespace storage {

class StorageCompactionFilter final : public kvstore::KVFilter {
public:
    explicit StorageCompactionFilter(meta::SchemaManager* schemaMan)
        : schemaMan_(schemaMan) {
        CHECK_NOTNULL(schemaMan_);
    }

    bool filter(GraphSpaceID spaceId,
                const folly::StringPiece& key,
                const folly::StringPiece& val) const override {
        if (FLAGS_storage_kv_mode) {
            // in kv mode, we don't delete any data
            return false;
        }

        if (NebulaKeyUtils::isDataKey(key)) {
            if (!schemaValid(spaceId, key)) {
                return true;
            }
            if (!ttlValid(spaceId, val)) {
                VLOG(3) << "TTL invalid for key " << key;
                return true;
            }
            if (filterVersions(key)) {
                VLOG(3) << "Extra versions has been filtered!";
                return true;
            }
        } else if (NebulaKeyUtils::isIndexKey(key)) {
            if (!indexValid(key)) {
                VLOG(3) << "Index invalid for the key " << key;
                return true;
            }
        } else {
            VLOG(3) << "Skip the system key inside, key " << key;
        }
        return false;
    }

    bool schemaValid(GraphSpaceID spaceId, const folly::StringPiece& key) const {
        if (NebulaKeyUtils::isVertex(key)) {
            auto tagId = NebulaKeyUtils::getTagId(key);
            auto ret = schemaMan_->getNewestTagSchemaVer(spaceId, tagId);
            if (ret.ok() && ret.value() == -1) {
                VLOG(3) << "Space " << spaceId << ", Tag " << tagId << " invalid";
                return false;
            }
        } else if (NebulaKeyUtils::isEdge(key)) {
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            if (edgeType < 0) {
                edgeType = -edgeType;
            }
            auto ret = schemaMan_->getNewestEdgeSchemaVer(spaceId, edgeType);
            if (ret.ok() && ret.value() == -1) {
                VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << " invalid";
                return false;
            }
        }
        return true;
    }

    bool ttlValid(GraphSpaceID, const folly::StringPiece&) const {
        // TODO(heng): Implement ttl filter after ttl schema pr merged in
        return true;
    }

    bool filterVersions(const folly::StringPiece& key) const {
        folly::StringPiece keyWithNoVersion = NebulaKeyUtils::keyWithNoVersion(key);
        if (keyWithNoVersion == lastKeyWithNoVerison_) {
            // TODO(heng): we could support max-versions configuration in schema if needed.
            return true;
        }
        lastKeyWithNoVerison_ = keyWithNoVersion.str();
        return false;
    }

    bool indexValid(const folly::StringPiece&) const {
        return true;
    }

private:
    mutable std::string lastKeyWithNoVerison_;
    meta::SchemaManager* schemaMan_ = nullptr;
};

class StorageCompactionFilterFactory final : public kvstore::KVCompactionFilterFactory {
public:
    StorageCompactionFilterFactory(meta::SchemaManager* schemaMan,
                                   GraphSpaceID spaceId,
                                   int32_t customFilterIntervalSecs):
        KVCompactionFilterFactory(spaceId, customFilterIntervalSecs),
        schemaMan_(schemaMan) {}

    std::unique_ptr<kvstore::KVFilter> createKVFilter() override {
        return std::make_unique<StorageCompactionFilter>(schemaMan_);
    }

    const char* Name() const override {
        return "StorageCompactionFilterFactory";
    }

private:
    meta::SchemaManager* schemaMan_ = nullptr;
};

class StorageCompactionFilterFactoryBuilder : public kvstore::CompactionFilterFactoryBuilder {
public:
    explicit StorageCompactionFilterFactoryBuilder(meta::SchemaManager* schemaMan)
        : schemaMan_(schemaMan) {}

    virtual ~StorageCompactionFilterFactoryBuilder() = default;

    std::shared_ptr<kvstore::KVCompactionFilterFactory>
    buildCfFactory(GraphSpaceID spaceId, int32_t customFilterIntervalSecs) override {
        return std::make_shared<StorageCompactionFilterFactory>(schemaMan_,
                                                                spaceId,
                                                                customFilterIntervalSecs);
    }

private:
    meta::SchemaManager* schemaMan_ = nullptr;
};


}   // namespace storage
}   // namespace nebula
#endif   // STORAGE_COMPACTIONFILTER_H_
