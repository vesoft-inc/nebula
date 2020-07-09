/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/MetaCache.h"
#include "MetaCache.h"
#include "common/network/NetworkUtils.h"

namespace nebula {
namespace graph {

#define CHECK_SPACE_ID(spaceId) \
    auto spaceIter = cache_.find(spaceId); \
        if (spaceIter == cache_.end()) { \
        return Status::Error("SpaceID `%d' not found", spaceId); \
    }

Status MetaCache::createSpace(const meta::cpp2::CreateSpaceReq &req, GraphSpaceID &spaceId) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto ifNotExists = req.get_if_not_exists();
    auto properties = req.get_properties();
    auto spaceName = properties.get_space_name();
    auto findIter = spaces_.find(spaceName);
    if (ifNotExists && findIter != spaces_.end()) {
        spaceId = findIter->second.get_space_id();
        return Status::OK();
    }
    if (findIter != spaces_.end()) {
        return Status::Error("Space `%s' existed", spaceName.c_str());
    }
    spaceId = ++id_;
    meta::cpp2::SpaceItem space;
    space.set_space_id(spaceId);
    space.set_properties(std::move(properties));
    spaces_[spaceName] = space;
    VLOG(1) << "space name: " << space.get_properties().get_space_name()
            << ", partition_num: " << space.get_properties().get_partition_num()
            << ", replica_factor: " << space.get_properties().get_replica_factor()
            << ", rvid_size: " << space.get_properties().get_vid_size();
    cache_[spaceId] = SpaceInfoCache();
    return Status::OK();
}

StatusOr<meta::cpp2::SpaceItem> MetaCache::getSpace(const meta::cpp2::GetSpaceReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    auto findIter = spaces_.find(req.get_space_name());
    if (findIter == spaces_.end()) {
        LOG(ERROR) << "Space " << req.get_space_name().c_str() << " not found";
        return Status::Error("Space `%s' not found", req.get_space_name().c_str());
    }
    VLOG(1) << "space name: " << findIter->second.get_properties().get_space_name()
            << ", partition_num: " << findIter->second.get_properties().get_partition_num()
            << ", replica_factor: " << findIter->second.get_properties().get_replica_factor()
            << ", rvid_size: " << findIter->second.get_properties().get_vid_size();
    return findIter->second;
}

StatusOr<std::vector<meta::cpp2::IdName>> MetaCache::listSpaces() {
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<meta::cpp2::IdName> spaces;
    for (auto &item : spaces_) {
        meta::cpp2::IdName idName;
        idName.set_id(to(item.second.get_space_id(), EntryType::SPACE));
        idName.set_name(item.first);
        spaces.emplace_back(idName);
    }
    return spaces;
}

Status MetaCache::dropSpace(const meta::cpp2::DropSpaceReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto spaceName  = req.get_space_name();
    auto findIter = spaces_.find(spaceName);
    auto ifExists = req.get_if_exists();

    if (ifExists && findIter == spaces_.end()) {
        Status::OK();
    }

    if (findIter == spaces_.end()) {
        return Status::Error("Space `%s' not existed", req.get_space_name().c_str());
    }
    auto id = findIter->second.get_space_id();
    spaces_.erase(spaceName);
    cache_.erase(id);
    return Status::OK();
}

Status MetaCache::createTag(const meta::cpp2::CreateTagReq &req, TagID &tagId) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifNotExists = req.get_if_not_exists();
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (ifNotExists && findIter != tagSchemas.end()) {
        tagId = findIter->second.get_tag_id();
        return Status::OK();
    }

    tagId = ++id_;
    meta::cpp2::TagItem tagItem;
    tagItem.set_tag_id(tagId);
    tagItem.set_tag_name(tagName);
    tagItem.set_version(0);
    tagItem.set_schema(req.get_schema());
    tagSchemas[tagName] = std::move(tagItem);
    return Status::OK();
}

StatusOr<meta::cpp2::Schema> MetaCache::getTag(const meta::cpp2::GetTagReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (findIter == tagSchemas.end()) {
        LOG(ERROR) << "Tag name: " << tagName << " not found";
        return Status::Error("Not found");
    }
    return findIter->second.get_schema();
}

StatusOr<std::vector<meta::cpp2::TagItem>>
MetaCache::listTags(const meta::cpp2::ListTagsReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    std::vector<meta::cpp2::TagItem> tagItems;
    for (const auto& item : spaceIter->second.tagSchemas_) {
        tagItems.emplace_back(item.second);
    }
    return tagItems;
}

Status MetaCache::dropTag(const meta::cpp2::DropTagReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifExists = req.get_if_exists();
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (ifExists && findIter == tagSchemas.end()) {
        return Status::OK();
    }
    if (findIter == tagSchemas.end()) {
        return Status::Error("Tag `%s' not existed", req.get_tag_name().c_str());
    }

    tagSchemas.erase(findIter);
    return Status::OK();
}

Status MetaCache::createEdge(const meta::cpp2::CreateEdgeReq &req, EdgeType &edgeType) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifNotExists = req.get_if_not_exists();
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (ifNotExists && findIter != edgeSchemas.end()) {
        edgeType = findIter->second.get_edge_type();
        return Status::OK();
    }

    edgeType = ++id_;
    meta::cpp2::EdgeItem edgeItem;
    edgeItem.set_edge_type(edgeType);
    edgeItem.set_edge_name(edgeName);
    edgeItem.set_version(0);
    edgeItem.set_schema(req.get_schema());
    edgeSchemas[edgeName] = std::move(edgeItem);
    return Status::OK();
}

StatusOr<meta::cpp2::Schema> MetaCache::getEdge(const meta::cpp2::GetEdgeReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (findIter == edgeSchemas.end()) {
        return Status::Error("Not found");
    }
    return findIter->second.get_schema();
}

StatusOr<std::vector<meta::cpp2::EdgeItem>>
MetaCache::listEdges(const meta::cpp2::ListEdgesReq &req) {
    folly::RWSpinLock::ReadHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    std::vector<meta::cpp2::EdgeItem> edgeItems;
    for (const auto& item : spaceIter->second.edgeSchemas_) {
        edgeItems.emplace_back(item.second);
    }
    return edgeItems;
}

Status MetaCache::dropEdge(const meta::cpp2::DropEdgeReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto ifExists = req.get_if_exists();
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (ifExists && findIter == edgeSchemas.end()) {
        return Status::OK();
    }

    if (findIter == edgeSchemas.end()) {
        return Status::Error("Edge `%s' not existed", req.get_edge_name().c_str());
    }

    edgeSchemas.erase(findIter);
    return Status::OK();
}

Status MetaCache::AlterTag(const meta::cpp2::AlterTagReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto tagName = req.get_tag_name();
    auto &tagSchemas = spaceIter->second.tagSchemas_;
    auto findIter = tagSchemas.find(tagName);
    if (findIter == tagSchemas.end()) {
        return Status::Error("Tag `%s' not existed", req.get_tag_name().c_str());
    }

    auto &schema = findIter->second.schema;
    auto items = req.get_tag_items();
    auto prop = req.get_schema_prop();
    auto status = alterColumnDefs(schema, items);
    if (!status.ok()) {
        return status;
    }
    return alterSchemaProp(schema, prop);
}

Status MetaCache::AlterEdge(const meta::cpp2::AlterEdgeReq &req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    CHECK_SPACE_ID(req.get_space_id());
    auto edgeName = req.get_edge_name();
    auto &edgeSchemas = spaceIter->second.edgeSchemas_;
    auto findIter = edgeSchemas.find(edgeName);
    if (findIter == edgeSchemas.end()) {
        return Status::Error("Edge `%s' not existed", req.get_edge_name().c_str());
    }

    auto &schema = findIter->second.schema;
    auto items = req.get_edge_items();
    auto prop = req.get_schema_prop();
    auto status = alterColumnDefs(schema, items);
    if (!status.ok()) {
        return status;
    }
    return alterSchemaProp(schema, prop);
}

Status MetaCache::createTagIndex(const meta::cpp2::CreateTagIndexReq&) {
    return Status::OK();
}

Status MetaCache::createEdgeIndex(const meta::cpp2::CreateEdgeIndexReq&) {
    return Status::OK();
}

Status MetaCache::dropTagIndex(const meta::cpp2::DropTagIndexReq&) {
    return Status::OK();
}

Status MetaCache::dropTagIndex(const meta::cpp2::DropEdgeIndexReq&) {
    return Status::OK();
}

Status MetaCache::regConfigs(const std::vector<meta::cpp2::ConfigItem>&) {
    return Status::OK();
}

Status MetaCache::setConfig(const meta::cpp2::ConfigItem&) {
    return Status::OK();
}

Status MetaCache::heartBeat(const meta::cpp2::HBReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto host = req.get_host();
    if (host.port == 0) {
        return Status::OK();
    }
    hostSet_.emplace(std::move(host));
    return Status::OK();
}

Status MetaCache::listUsers(const meta::cpp2::ListUsersReq&) {
    return Status::OK();
}

std::vector<meta::cpp2::HostItem> MetaCache::listHosts() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    std::vector<meta::cpp2::HostItem> hosts;
    for (auto& spaceIdIt : spaces_) {
        auto spaceName = spaceIdIt.first;
        for (auto &h : hostSet_) {
            meta::cpp2::HostItem host;
            host.set_hostAddr(h);
            host.set_status(meta::cpp2::HostStatus::ONLINE);
            std::unordered_map<std::string, std::vector<PartitionID>> leaderParts;
            std::vector<PartitionID> parts = {1};
            leaderParts.emplace(spaceName, parts);
            host.set_leader_parts(leaderParts);
            host.set_all_parts(std::move(leaderParts));
        }
    }
    return hosts;
}

std::unordered_map<PartitionID, std::vector<HostAddr>> MetaCache::getParts() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
    parts[1] = {};
    for (auto &h : hostSet_) {
        parts[1].emplace_back(h);
    }
    return parts;
}

Status MetaCache::alterColumnDefs(meta::cpp2::Schema &schema,
                                  const std::vector<meta::cpp2::AlterSchemaItem> &items) {
    std::vector<meta::cpp2::ColumnDef> columns = schema.columns;
    for (auto& item : items) {
        auto& cols = item.get_schema().get_columns();
        auto op = item.op;
        for (auto& col : cols) {
            switch (op) {
                case meta::cpp2::AlterSchemaOp::ADD:
                    for (auto it = schema.columns.begin(); it != schema.columns.end(); ++it) {
                        if (it->get_name() == col.get_name()) {
                            return Status::Error("Column existing: `%s'", col.get_name().c_str());
                        }
                    }
                    columns.emplace_back(col);
                    break;
                case meta::cpp2::AlterSchemaOp::CHANGE: {
                    bool isOk = false;
                    for (auto it = columns.begin(); it != columns.end(); ++it) {
                        auto colName = col.get_name();
                        if (colName == it->get_name()) {
                            // If this col is ttl_col, change not allowed
                            if (schema.schema_prop.__isset.ttl_col &&
                                (*schema.schema_prop.get_ttl_col() == colName)) {
                                return Status::Error("Column: `%s' as ttl_col, change not allowed",
                                                     colName.c_str());
                            }
                            *it = col;
                            isOk = true;
                            break;
                        }
                    }
                    if (!isOk) {
                        return Status::Error("Column not found: `%s'", col.get_name().c_str());
                    }
                    break;
                }
                case meta::cpp2::AlterSchemaOp::DROP: {
                    bool isOk = false;
                    for (auto it = columns.begin(); it != columns.end(); ++it) {
                        auto colName = col.get_name();
                        if (colName == it->get_name()) {
                            if (schema.schema_prop.__isset.ttl_col &&
                                (*schema.schema_prop.get_ttl_col() == colName)) {
                                schema.schema_prop.set_ttl_duration(0);
                                schema.schema_prop.set_ttl_col("");
                            }
                            columns.erase(it);
                            isOk = true;
                            break;
                        }
                    }
                    if (!isOk) {
                        return Status::Error("Column not found: `%s'", col.get_name().c_str());
                    }
                    break;
                }
                default:
                    return Status::Error("Alter schema operator not supported");
            }
        }
    }
    schema.columns = std::move(columns);
    return Status::OK();
}

Status MetaCache::alterSchemaProp(meta::cpp2::Schema &schema,
                                  const meta::cpp2::SchemaProp &alterSchemaProp) {
    meta::cpp2::SchemaProp schemaProp = schema.get_schema_prop();
    if (alterSchemaProp.__isset.ttl_duration) {
        // Graph check  <=0 to = 0
        schemaProp.set_ttl_duration(*alterSchemaProp.get_ttl_duration());
    }
    if (alterSchemaProp.__isset.ttl_col) {
        auto ttlCol = *alterSchemaProp.get_ttl_col();
        // Disable ttl, ttl_col is empty, ttl_duration is 0
        if (ttlCol.empty()) {
            schemaProp.set_ttl_duration(0);
            schemaProp.set_ttl_col(ttlCol);
            return Status::OK();
        }

        auto existed = false;
        for (auto& col : schema.columns) {
            if (col.get_name() == ttlCol) {
                // Only integer and timestamp columns can be used as ttl_col
                if (col.type != meta::cpp2::PropertyType::INT32 &&
                    col.type != meta::cpp2::PropertyType::INT64 &&
                    col.type != meta::cpp2::PropertyType::TIMESTAMP) {
                    return Status::Error("TTL column type illegal");
                }
                existed = true;
                schemaProp.set_ttl_col(ttlCol);
                break;
            }
        }

        if (!existed) {
            return Status::Error("TTL column not found: `%s'", ttlCol.c_str());
        }
    }

    // Disable implicit TTL mode
    if ((schemaProp.get_ttl_duration() && (*schemaProp.get_ttl_duration() != 0)) &&
        (!schemaProp.get_ttl_col() || (schemaProp.get_ttl_col() &&
                                       schemaProp.get_ttl_col()->empty()))) {
        return Status::Error("Implicit ttl_col not support");
    }

    schema.set_schema_prop(std::move(schemaProp));
    return Status::OK();
}

Status MetaCache::createSnapshot() {
    folly::RWSpinLock::WriteHolder holder(lock_);
    if (cache_.empty()) {
        return Status::OK();
    }
    meta::cpp2::Snapshot snapshot;
    char ch[60];
    std::time_t t = std::time(nullptr);
    std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
    auto snapshotName = folly::stringPrintf("SNAPSHOT_%s", ch);
    snapshot.set_name(snapshotName);
    snapshot.set_status(meta::cpp2::SnapshotStatus::VALID);
    DCHECK(!hostSet_.empty());
    snapshot.set_hosts(network::NetworkUtils::toHostsStr(
            std::vector<HostAddr>(hostSet_.begin(), hostSet_.end())));
    snapshots_[snapshotName] = std::move(snapshot);
    return Status::OK();
}

Status MetaCache::dropSnapshot(const meta::cpp2::DropSnapshotReq& req) {
    folly::RWSpinLock::WriteHolder holder(lock_);
    auto name = req.get_name();
    auto find = snapshots_.find(name);
    if (find == snapshots_.end()) {
        return Status::Error("`%s' is nonexistent", name.c_str());
    }
    snapshots_.erase(find);
    return Status::OK();
}

StatusOr<std::vector<meta::cpp2::Snapshot>> MetaCache::listSnapshots() {
    folly::RWSpinLock::ReadHolder holder(lock_);
    std::vector<meta::cpp2::Snapshot> snapshots;
    for (auto& snapshot : snapshots_) {
        snapshots.emplace_back(snapshot.second);
    }
    return snapshots;
}
}  // namespace graph
}  // namespace nebula
