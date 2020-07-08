/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_METACACHE_H_
#define EXEC_METACACHE_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

class MetaCache final {
public:
    static MetaCache& instance() {
        static MetaCache instance;
        return instance;
    }

    Status createSpace(const meta::cpp2::CreateSpaceReq &req, GraphSpaceID &spaceId);

    StatusOr<meta::cpp2::SpaceItem> getSpace(const meta::cpp2::GetSpaceReq &req);

    StatusOr<std::vector<meta::cpp2::IdName>> listSpaces();

    Status dropSpace(const meta::cpp2::DropSpaceReq &req);

    Status createTag(const meta::cpp2::CreateTagReq &req, TagID &tagId);

    StatusOr<meta::cpp2::Schema> getTag(const meta::cpp2::GetTagReq &req);

    StatusOr<std::vector<meta::cpp2::TagItem>> listTags(const meta::cpp2::ListTagsReq &req);

    Status createEdge(const meta::cpp2::CreateEdgeReq &req, EdgeType &edgeType);

    StatusOr<meta::cpp2::Schema> getEdge(const meta::cpp2::GetEdgeReq &req);

    StatusOr<std::vector<meta::cpp2::EdgeItem>> listEdges(const meta::cpp2::ListEdgesReq &req);

    Status dropTag(const meta::cpp2::DropTagReq &req);

    Status dropEdge(const meta::cpp2::DropEdgeReq &req);

    Status AlterTag(const meta::cpp2::AlterTagReq &req);

    Status AlterEdge(const meta::cpp2::AlterEdgeReq &req);

    Status createTagIndex(const meta::cpp2::CreateTagIndexReq &req);

    Status createEdgeIndex(const meta::cpp2::CreateEdgeIndexReq &req);

    Status dropTagIndex(const meta::cpp2::DropTagIndexReq &req);

    Status dropTagIndex(const meta::cpp2::DropEdgeIndexReq &req);

    Status regConfigs(const std::vector<meta::cpp2::ConfigItem> &items);

    Status setConfig(const meta::cpp2::ConfigItem &req);

    Status heartBeat(const meta::cpp2::HBReq &req);

    Status listUsers(const meta::cpp2::ListUsersReq& req);

    std::vector<meta::cpp2::HostItem> listHosts();

    std::unordered_map<PartitionID, std::vector<HostAddr>> getParts();

private:
    MetaCache() = default;

    Status alterColumnDefs(meta::cpp2::Schema &schema,
                           const std::vector<meta::cpp2::AlterSchemaItem> &items);

    Status alterSchemaProp(meta::cpp2::Schema &schema,
                           const meta::cpp2::SchemaProp &alterSchemaProp);


private:
    enum class EntryType : int8_t {
        SPACE       = 0x01,
        TAG         = 0x02,
        EDGE        = 0x03,
        INDEX       = 0x04,
        CONFIG      = 0x05,
    };

    template<class T>
    std::enable_if_t<std::is_integral<T>::value, meta::cpp2::ID>
    to(T id, EntryType type) {
        meta::cpp2::ID thriftID;
        switch (type) {
            case EntryType::SPACE:
                thriftID.set_space_id(static_cast<GraphSpaceID>(id));
                break;
            case EntryType::TAG:
                thriftID.set_tag_id(static_cast<TagID>(id));
                break;
            case EntryType::EDGE:
                thriftID.set_edge_type(static_cast<EdgeType>(id));
                break;
            case EntryType::CONFIG:
                break;
            case EntryType::INDEX:
                thriftID.set_index_id(static_cast<IndexID>(id));
                break;
            }
        return thriftID;
    }

private:
    using HostMap = std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr>;
    using TagSchemas = std::unordered_map<std::string, meta::cpp2::TagItem>;
    using EdgeSchemas = std::unordered_map<std::string, meta::cpp2::EdgeItem>;
    using Indexes = std::unordered_map<std::string, meta::cpp2::IndexItem>;

    struct SpaceInfoCache {
        TagSchemas              tagSchemas_;
        EdgeSchemas             edgeSchemas_;
        Indexes                 tagIndexes_;
        Indexes                 edgeIndexes_;
    };

    std::unordered_set<HostAddr>                             hostSet_;
    std::unordered_map<GraphSpaceID, SpaceInfoCache>         cache_;
    std::unordered_map<std::string, meta::cpp2::SpaceItem>   spaces_;
    int64_t                                                  id_{0};
    mutable folly::RWSpinLock                                lock_;
};

}  // namespace graph
}  // namespace nebula
#endif  // EXEC_METACACHE_H_
