/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_COMPACTIONFILTER_H_
#define META_COMPACTIONFILTER_H_

#include "base/Base.h"
#include "kvstore/CompactionFilter.h"
#include "meta/MetaCFHelper.h"

namespace nebula {
namespace meta {


#define GET_SPACEID_IF_START_WITH(key, prefix, spaceId) \
    if (key.startsWith(prefix)) { \
        spaceId = *reinterpret_cast<const GraphSpaceID*>(key.data() + prefix.size()); \
        return true; \
    }

class MetaCompactionFilter final : public kvstore::KVFilter {
public:
    explicit MetaCompactionFilter(meta::MetaCFHelper* cfHelper)
        : cfHelper_(cfHelper) {}

    bool filter(GraphSpaceID,
                const folly::StringPiece& key,
                const folly::StringPiece& val) const override {
        UNUSED(val);
        GraphSpaceID spaceId;
        if (hasSpaceId(key, spaceId) && !cfHelper_->spaceValid(spaceId)) {
            return true;
        }

        return false;
    }

    bool hasSpaceId(const folly::StringPiece& key, GraphSpaceID& spaceId) const {
        // kSpacesTable has been scanned when we build compaction filter
        // table of part/tag/edge/index/role/defaultValue has spaceId in its key
        GET_SPACEID_IF_START_WITH(key, kPartsTable, spaceId);
        GET_SPACEID_IF_START_WITH(key, kTagsTable, spaceId);
        GET_SPACEID_IF_START_WITH(key, kEdgesTable, spaceId);
        GET_SPACEID_IF_START_WITH(key, kIndexesTable, spaceId);
        GET_SPACEID_IF_START_WITH(key, kRolesTable, spaceId);
        GET_SPACEID_IF_START_WITH(key, kDefaultTable, spaceId);
        return false;
    }

private:
    meta::MetaCFHelper* cfHelper_ = nullptr;
};

class MetaCompactionFilterFactory final : public kvstore::KVCompactionFilterFactory {
public:
    MetaCompactionFilterFactory(meta::MetaCFHelper* cfHelper, GraphSpaceID spaceId,
                                int32_t customFilterIntervalSecs)
        : KVCompactionFilterFactory(spaceId, customFilterIntervalSecs), cfHelper_(cfHelper) {}

    std::unique_ptr<kvstore::KVFilter> createKVFilter() override {
        cfHelper_->init();
        return std::make_unique<MetaCompactionFilter>(cfHelper_);
    }

    const char* Name() const override {
        return "MetaCompactionFilterFactory";
    }

private:
    meta::MetaCFHelper* cfHelper_ = nullptr;
};

class MetaCompactionFilterFactoryBuilder final : public kvstore::CompactionFilterFactoryBuilder {
public:
    explicit MetaCompactionFilterFactoryBuilder(meta::MetaCFHelper* cfHelper)
        : cfHelper_(cfHelper) {}

    std::shared_ptr<kvstore::KVCompactionFilterFactory>
    buildCfFactory(GraphSpaceID spaceId, int32_t customFilterIntervalSecs) override {
        return std::make_shared<MetaCompactionFilterFactory>(cfHelper_, spaceId,
                                                             customFilterIntervalSecs);
    }

private:
    meta::MetaCFHelper* cfHelper_ = nullptr;
};


}   // namespace meta
}   // namespace nebula
#endif  // META_COMPACTIONFILTER_H_

