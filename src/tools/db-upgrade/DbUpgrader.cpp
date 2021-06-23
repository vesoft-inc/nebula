/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/fs/FileUtils.h"
#include "common/datatypes/Value.h"
#include "tools/db-upgrade/DbUpgrader.h"
#include "tools/db-upgrade/NebulaKeyUtilsV1.h"
#include "tools/db-upgrade/NebulaKeyUtilsV2.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"

DEFINE_string(src_db_path, "", "Source data path(data_path in storage 1.x conf), "
                               "multi paths should be split by comma");
DEFINE_string(dst_db_path, "", "Destination data path(data_path in storage 2.0 conf), "
                               "multi paths should be split by comma");
DEFINE_string(upgrade_meta_server, "127.0.0.1:45500", "Meta servers' address.");
DEFINE_uint32(write_batch_num, 100, "The size of the batch written to rocksdb");
DEFINE_uint32(upgrade_version, 0, "When the value is 1, upgrade the data from 1.x to 2.0 GA. "
                                  "When the value is 2, upgrade the data from 2.0 RC to 2.0 GA.");
DEFINE_bool(compactions, true, "When the upgrade of the space is completed, "
                               "whether to compact data");
DEFINE_uint32(max_concurrent_parts, 10, "The parts could be processed simultaneously");
DEFINE_uint32(max_concurrent_spaces, 5, "The spaces could be processed simultaneously");

namespace nebula {
namespace storage {

Status UpgraderSpace::init(meta::MetaClient* mclient,
                           meta::ServerBasedSchemaManager*sMan,
                           meta::IndexManager* iMan,
                           const std::string& srcPath,
                           const std::string& dstPath,
                           const std::string& entry) {
    metaClient_ = mclient;
    schemaMan_ = sMan;
    indexMan_ = iMan;
    srcPath_ = srcPath;
    dstPath_ = dstPath;
    entry_ = entry;

    auto ret = initSpace(entry_);
    if (!ret.ok()) {
        LOG(ERROR) << "Init " << srcPath << " space id " << entry_ << " failed";
        return ret;
    }

    return Status::OK();
}

Status UpgraderSpace::initSpace(const std::string& sId) {
    try {
        spaceId_ = folly::to<GraphSpaceID>(sId);
    } catch (const std::exception& ex) {
        LOG(ERROR) << "Cannot convert space id " << sId;
        return Status::Error("Cannot convert space id %s", sId.c_str());;
    }

    auto sRet = schemaMan_->toGraphSpaceName(spaceId_);
    if (!sRet.ok()) {
        LOG(ERROR) << "Space id " << spaceId_ << " no found";
        return sRet.status();
    }
    spaceName_ = sRet.value();

    auto spaceVidLen = metaClient_->getSpaceVidLen(spaceId_);
    if (!spaceVidLen.ok()) {
        return spaceVidLen.status();
    }
    spaceVidLen_ = spaceVidLen.value();

    // Use readonly rocksdb
    readEngine_.reset(new nebula::kvstore::RocksEngine(spaceId_, spaceVidLen_, srcPath_, "",
                                                       nullptr, nullptr, true));
    writeEngine_.reset(new nebula::kvstore::RocksEngine(spaceId_, spaceVidLen_, dstPath_));

    parts_.clear();
    parts_ = readEngine_->allParts();
    LOG(INFO) << "Src data path: " << srcPath_ << " space id " << spaceId_  << " has "
              << parts_.size() << " parts";

    tagSchemas_.clear();
    tagFieldName_.clear();
    tagIndexes_.clear();
    edgeSchemas_.clear();
    edgeFieldName_.clear();
    edgeIndexes_.clear();

    auto ret = buildSchemaAndIndex();
    if (!ret.ok()) {
         LOG(ERROR) << "Build schema and index in space id " << spaceId_ << " failed";
         return ret;
    }

    pool_ = std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_max_concurrent_parts);
    // Parallel process part
    for (auto& partId : parts_) {
        partQueue_.add(partId);
    }

    return Status::OK();
}

Status UpgraderSpace::buildSchemaAndIndex() {
    // Get all tag in space
    auto tags = schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        LOG(ERROR) << "Space id " << spaceId_ << " no found";
        return tags.status();
    }
    tagSchemas_ = std::move(tags).value();

    for (auto&tag : tagSchemas_) {
        auto tagId = tag.first;
        auto newestTagschema = tag.second.back();
        auto fields = newestTagschema->getNumFields();
        for (size_t i = 0; i < fields; i++) {
            tagFieldName_[tagId].emplace_back(newestTagschema->getFieldName(i));
        }
        if (fields == 0) {
            tagFieldName_[tagId] = {};
        }
        LOG(INFO) << "Tag id " << tagId << " has " << tagFieldName_[tagId].size()
                  << " fields!";
    }

    // Get all tag index in space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> tagIndexes;
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << "Space id " << spaceId_ << " no found";
        return iRet.status();
    }
    tagIndexes = std::move(iRet).value();

    // Handle tag index
    for (auto&tagIndex : tagIndexes) {
        auto tagId = tagIndex->get_schema_id().get_tag_id();
        tagIndexes_[tagId].emplace(tagIndex);
    }

    for (auto&tagindexes : tagIndexes_) {
        LOG(INFO) << "Tag id " << tagindexes.first  << " has "
                  << tagindexes.second.size() << " indexes";
    }

    // Get all edge in space
    auto edges = schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        LOG(ERROR) << "Space id " << spaceId_ << " no found";
        return edges.status();
    }
    edgeSchemas_ = std::move(edges).value();

    for (auto&edge : edgeSchemas_) {
        auto edgetype = edge.first;
        auto newestEdgeSchema = edge.second.back();
        auto fields = newestEdgeSchema->getNumFields();
        for (size_t i = 0; i < fields; i++) {
            edgeFieldName_[edgetype].emplace_back(newestEdgeSchema->getFieldName(i));
        }
        if (fields == 0) {
            edgeFieldName_[edgetype] = {};
        }
        LOG(INFO) << "Edgetype " << edgetype << " has "
                  << edgeFieldName_[edgetype].size() << " fields!";
    }

    // Get all edge index in space
    std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> edgeIndexes;
    iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << "Space id " << spaceId_ << " no found";
        return iRet.status();
    }
    edgeIndexes = std::move(iRet).value();

    // Handle edge index
    for (auto& edgeIndex : edgeIndexes) {
        auto edgetype = edgeIndex->get_schema_id().get_edge_type();
        edgeIndexes_[edgetype].emplace(edgeIndex);
    }

    for (auto&edgeindexes : edgeIndexes_) {
        LOG(INFO) << "EdgeType " << edgeindexes.first  << " has "
                  << edgeindexes.second.size() << " indexes";
    }
    return  Status::OK();
}

bool UpgraderSpace::isValidVidLen(VertexID srcVId, VertexID dstVId) {
    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, srcVId, dstVId)) {
        LOG(ERROR) << "Vertex id length is illegal, expect: " << spaceVidLen_
                   << " result: " << srcVId << " " << dstVId;
        return false;
    }
    return true;
}

void UpgraderSpace::runPartV1() {
    std::chrono::milliseconds take_dura{10};
    if (auto pId = partQueue_.try_take_for(take_dura)) {
        PartitionID partId = *pId;
        // Handle vertex and edge, if there is an index, generate index data
        LOG(INFO) << "Start to handle vertex/edge/index data in space id " << spaceId_
                  << " part id " << partId;
        const auto& prefix = NebulaKeyUtilsV1::prefix(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " part " << partId
                       << " no found!";
            LOG(ERROR) << "Handle vertex/edge/index data in space id " << spaceId_
                       << " part id " << partId << " failed";

            auto unFinishedPart = --unFinishedPart_;
            if (unFinishedPart == 0) {
                // all parts has finished
                LOG(INFO) << "Handle last part: " << partId
                          << " vertex/edge/index data in space id "
                          << spaceId_ << " finished";
            } else {
                pool_->add(std::bind(&UpgraderSpace::runPartV1, this));
            }
            return;
        }

        std::vector<kvstore::KV> data;
        TagID                                lastTagId = 0;
        int64_t                              lastVertexId = 0;

        int64_t                              lastSrcVertexId = 0;
        EdgeType                             lastEdgeType = 0;
        int64_t                              lastDstVertexId = 0;
        EdgeRanking                          lastRank = 0;

        while (iter && iter->valid()) {
            auto key = iter->key();
            if (NebulaKeyUtilsV1::isVertex(key)) {
                auto vId = NebulaKeyUtilsV1::getVertexId(key);
                auto tagId = NebulaKeyUtilsV1::getTagId(key);

                auto it = tagSchemas_.find(tagId);
                if (it == tagSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = tagFieldName_.find(tagId);
                if (iterField == tagFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (vId == lastVertexId && tagId == lastTagId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto strVid = std::string(reinterpret_cast<const char*>(&vId), sizeof(vId));
                auto newTagSchema = it->second.back().get();
                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::vertexKey(spaceVidLen_,
                                                        partId,
                                                        strVid,
                                                        tagId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, val);
                if (!reader) {
                    LOG(ERROR) << "Can't get tag reader of " << tagId;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeVertexValue(partId, reader.get(), newTagSchema,
                                  newKey, strVid, tagId, data);

                lastTagId = tagId;
                lastVertexId = vId;
            } else if (NebulaKeyUtilsV1::isEdge(key)) {
                auto svId = NebulaKeyUtilsV1::getSrcId(key);
                auto edgetype = NebulaKeyUtilsV1::getEdgeType(key);
                auto ranking = NebulaKeyUtilsV1::getRank(key);
                auto dvId = NebulaKeyUtilsV1::getDstId(key);

                auto it = edgeSchemas_.find(std::abs(edgetype));
                if (it == edgeSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = edgeFieldName_.find(std::abs(edgetype));
                if (iterField == edgeFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (svId == lastSrcVertexId &&
                    edgetype == lastEdgeType &&
                    ranking == lastRank &&
                    dvId == lastDstVertexId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto strsvId = std::string(reinterpret_cast<const char*>(&svId), sizeof(svId));
                auto strdvId = std::string(reinterpret_cast<const char*>(&dvId), sizeof(dvId));

                auto newEdgeSchema = it->second.back().get();

                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                      partId,
                                                      strsvId,
                                                      edgetype,
                                                      ranking,
                                                      strdvId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                                  spaceId_,
                                                                  std::abs(edgetype),
                                                                  val);
                if (!reader) {
                    LOG(ERROR) << "Can't get edge reader of " << edgetype;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeEdgeValue(partId, reader.get(), newEdgeSchema, newKey, strsvId,
                                edgetype, ranking, strdvId, data);
                lastSrcVertexId = svId;
                lastEdgeType  = edgetype;
                lastRank = ranking;
                lastDstVertexId = dvId;
            }

            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send record total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put in space id " << spaceId_
                               << " part id " << partId << " failed.";
                }
                data.clear();
            }

            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put in space id " << spaceId_
                       << " part id " << partId << " failed.";
        }
        data.clear();
        LOG(INFO) << "Handle vertex/edge/index data in space id " << spaceId_
                  << " part id " << partId << " finished";

        auto unFinishedPart = --unFinishedPart_;
        if (unFinishedPart == 0) {
            // all parts has finished
            LOG(INFO) << "Handle last part: " << partId << " vertex/edge/index data in space id "
                      << spaceId_ << " finished";
        } else {
            pool_->add(std::bind(&UpgraderSpace::runPartV1, this));
        }
    } else  {
        LOG(INFO) << "Handle vertex/edge/index of parts data in space id " << spaceId_
                  << " finished";
    }
}

void UpgraderSpace::doProcessV1() {
    LOG(INFO) << "Start to handle data in space id " << spaceId_;

    // Parallel process part
    auto partConcurrency = std::min(static_cast<size_t>(FLAGS_max_concurrent_parts),
                                    parts_.size());
    LOG(INFO) << "Max concurrenct parts: " << partConcurrency;

    unFinishedPart_ = parts_.size();

    LOG(INFO) << "Start to handle vertex/edge/index of parts data in space id " << spaceId_;
    for (size_t i = 0; i < partConcurrency; ++i) {
        pool_->add(std::bind(&UpgraderSpace::runPartV1, this));
    }

    while (unFinishedPart_ != 0) {
        sleep(10);
    }

    // handle system data
    {
        LOG(INFO) << "Start to handle system data in space id " << spaceId_;
        auto prefix = NebulaKeyUtilsV1::systemPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " get system data failed";
            LOG(ERROR) << "Handle system data in space id " << spaceId_ << " failed";
            return;
        }
        std::vector<kvstore::KV> data;
        while (iter && iter->valid()) {
            auto key = iter->key();
            auto val = iter->val();
            data.emplace_back(std::move(key), std::move(val));
            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send system data total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put in space id " << spaceId_ << " failed.";
                }
                data.clear();
            }
            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put in space id " << spaceId_ << " failed.";
        }
        LOG(INFO) << "Handle system data in space id " << spaceId_ << " success";
        LOG(INFO) << "Handle data in space id " << spaceId_ << " success";
    }
}

void UpgraderSpace::runPartV2() {
    std::chrono::milliseconds take_dura{10};
    if (auto pId = partQueue_.try_take_for(take_dura)) {
        PartitionID partId = *pId;
        // Handle vertex and edge, if there is an index, generate index data
        LOG(INFO) << "Start to handle vertex/edge/index data in space id " << spaceId_
                  << " part id " << partId;
        auto prefix = NebulaKeyUtilsV2::partPrefix(partId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " part " << partId
                       << " no found!";
            LOG(ERROR) << "Handle vertex/edge/index data in space id " << spaceId_
                       << " part id " << partId << " failed";

            auto unFinishedPart = --unFinishedPart_;
            if (unFinishedPart == 0) {
                // all parts has finished
                LOG(INFO) << "Handle last part: " << partId
                          << " vertex/edge/index data in space id "
                          << spaceId_ << " finished";
            } else {
                pool_->add(std::bind(&UpgraderSpace::runPartV2, this));
            }
            return;
        }

        std::vector<kvstore::KV> data;
        TagID                                lastTagId = 0;
        VertexID                             lastVertexId = "";

        VertexID                             lastSrcVertexId = "";
        EdgeType                             lastEdgeType = 0;
        VertexID                             lastDstVertexId = "";
        EdgeRanking                          lastRank = 0;

        while (iter && iter->valid()) {
            auto key = iter->key();
            if (NebulaKeyUtilsV2::isVertex(spaceVidLen_, key)) {
                auto vId = NebulaKeyUtilsV2::getVertexId(spaceVidLen_, key).str();
                auto tagId = NebulaKeyUtilsV2::getTagId(spaceVidLen_, key);

                auto it = tagSchemas_.find(tagId);
                if (it == tagSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = tagFieldName_.find(tagId);
                if (iterField == tagFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (vId == lastVertexId && tagId == lastTagId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto newTagSchema = it->second.back().get();
                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::vertexKey(spaceVidLen_,
                                                        partId,
                                                        vId,
                                                        tagId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, val);
                if (!reader) {
                    LOG(ERROR) << "Can't get tag reader of " << tagId;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeVertexValue(partId, reader.get(), newTagSchema,
                                  newKey, vId, tagId, data);

                lastTagId = tagId;
                lastVertexId = vId;
            } else if (NebulaKeyUtilsV2::isEdge(spaceVidLen_, key)) {
                auto svId = NebulaKeyUtilsV2::getSrcId(spaceVidLen_, key).str();
                auto edgetype = NebulaKeyUtilsV2::getEdgeType(spaceVidLen_, key);
                auto ranking = NebulaKeyUtilsV2::getRank(spaceVidLen_, key);
                auto dvId = NebulaKeyUtilsV2::getDstId(spaceVidLen_, key).str();

                auto it = edgeSchemas_.find(std::abs(edgetype));
                if (it == edgeSchemas_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                auto iterField = edgeFieldName_.find(std::abs(edgetype));
                if (iterField == edgeFieldName_.end()) {
                    // Invalid data
                    iter->next();
                    continue;
                }
                if (svId == lastSrcVertexId &&
                    edgetype == lastEdgeType &&
                    ranking == lastRank &&
                    dvId == lastDstVertexId) {
                    // Multi version
                    iter->next();
                    continue;
                }

                auto newEdgeSchema = it->second.back().get();

                // Generate 2.0 key
                auto newKey = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                      partId,
                                                      svId,
                                                      edgetype,
                                                      ranking,
                                                      dvId);
                auto val = iter->val();
                auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                                  spaceId_,
                                                                  std::abs(edgetype),
                                                                  val);
                if (!reader) {
                    LOG(ERROR) << "Can't get edge reader of " << edgetype;
                    iter->next();
                    continue;
                }
                // Generate 2.0 value and index records
                encodeEdgeValue(partId, reader.get(), newEdgeSchema, newKey, svId,
                                edgetype, ranking, dvId, data);
                lastSrcVertexId = svId;
                lastEdgeType  = edgetype;
                lastRank = ranking;
                lastDstVertexId = dvId;
            }

            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send record total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put in space id " << spaceId_
                               << " part id " << partId << " failed.";
                }
                data.clear();
            }

            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put in space id " << spaceId_
                       << " part id " << partId << " failed.";
        }
        data.clear();
        LOG(INFO) << "Handle vertex/edge/index data in space id " << spaceId_
                  << " part id " << partId << " succeed";

        auto unFinishedPart = --unFinishedPart_;
        if (unFinishedPart == 0) {
            // all parts has finished
            LOG(INFO) << "Handle last part: " << partId << " vertex/edge/index data in space id "
                      << spaceId_ << " finished.";
        } else {
            pool_->add(std::bind(&UpgraderSpace::runPartV2, this));
        }
    } else {
        LOG(INFO) << "Handle vertex/edge/index of parts data in space id "
                  << spaceId_ << " finished";
    }
}

void UpgraderSpace::doProcessV2() {
    LOG(INFO) << "Start to handle data in space id " << spaceId_;

    // Parallel process part
    auto partConcurrency = std::min(static_cast<size_t>(FLAGS_max_concurrent_parts),
                                    parts_.size());
    LOG(INFO) << "Max concurrenct parts: " << partConcurrency;
    unFinishedPart_ = parts_.size();

    LOG(INFO) << "Start to handle vertex/edge/index of parts data in space id " << spaceId_;
    for (size_t i = 0; i < partConcurrency; ++i) {
        pool_->add(std::bind(&UpgraderSpace::runPartV2, this));
    }

    while (unFinishedPart_ != 0) {
        sleep(10);
    }

    // handle system data
    {
        LOG(INFO) << "Start to handle system data in space id " << spaceId_;
        auto prefix = NebulaKeyUtilsV2::systemPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = readEngine_->prefix(prefix, &iter);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Space id " << spaceId_ << " get system data failed.";
            LOG(ERROR) << "Handle system data in space id " << spaceId_ << " failed.";
            return;
        }
        std::vector<kvstore::KV> data;
        while (iter && iter->valid()) {
            auto key = iter->key();
            auto val = iter->val();
            data.emplace_back(std::move(key), std::move(val));
            if (data.size() >= FLAGS_write_batch_num) {
                VLOG(2) << "Send system data total rows " << data.size();
                auto code = writeEngine_->multiPut(data);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    LOG(FATAL) << "Write multi put in space id " << spaceId_ << " failed.";
                }
                data.clear();
            }
            iter->next();
        }

        auto code = writeEngine_->multiPut(data);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(FATAL) << "Write multi put in space id " << spaceId_ << " failed.";
        }
        LOG(INFO) << "Handle system data in space id " << spaceId_ << " success";
        LOG(INFO) << "Handle data in space id " << spaceId_ << " success";
    }
}

void UpgraderSpace::encodeVertexValue(PartitionID partId,
                                      RowReader* reader,
                                      const meta::NebulaSchemaProvider* schema,
                                      std::string& newkey,
                                      VertexID& strVid,
                                      TagID tagId,
                                      std::vector<kvstore::KV>& data) {
    // Get all returned field name
    auto& fieldNames = tagFieldName_[tagId];

    auto ret = encodeRowVal(reader, schema, fieldNames);
    if (ret.empty()) {
        LOG(ERROR)  << "Vertex or edge value is empty";
        return;
    }
    data.emplace_back(std::move(newkey), ret);

    // encode v2 index value
    auto it = tagIndexes_.find(tagId);
    if (it != tagIndexes_.end()) {
        // Use new RowReader
        auto nReader = RowReaderWrapper::getTagPropReader(schemaMan_, spaceId_, tagId, ret);
        if (nReader == nullptr) {
             LOG(ERROR) << "Bad format row: space id " << spaceId_
                        << " tag id " << tagId << " vertex id " << strVid;
             return;
        }
        for (auto& index : it->second) {
            auto newIndexKey = indexVertexKey(partId, strVid, nReader.get(), index);
            if (!newIndexKey.empty()) {
                data.emplace_back(std::move(newIndexKey), "");
            }
        }
    }
}

// If the field types are inconsistent, can be converted
WriteResult UpgraderSpace::convertValue(const meta::NebulaSchemaProvider* nSchema,
                                        const meta::SchemaProviderIf* oSchema,
                                        std::string& name,
                                        Value& val) {
    auto newpropType = nSchema->getFieldType(name);
    auto oldpropType = oSchema->getFieldType(name);
    if (newpropType == oldpropType) {
        return WriteResult::SUCCEEDED;
    }

    bool bval;
    double fval;
    int64_t ival;
    std::string sval;

    // need convert
    switch (val.type()) {
        case Value::Type::NULLVALUE:
            return WriteResult::SUCCEEDED;;
        case Value::Type::BOOL: {
            switch (newpropType) {
                case meta::cpp2::PropertyType::INT8:
                case meta::cpp2::PropertyType::INT16:
                case meta::cpp2::PropertyType::INT32:
                case meta::cpp2::PropertyType::INT64:
                case meta::cpp2::PropertyType::TIMESTAMP:
                case meta::cpp2::PropertyType::VID: {
                    bval = val.getBool();
                    if (bval) {
                        val.setInt(1);
                    } else {
                        val.setInt(0);
                    }
                    return WriteResult::SUCCEEDED;
                }
                case meta::cpp2::PropertyType::STRING:
                case meta::cpp2::PropertyType::FIXED_STRING: {
                    try {
                        bval = val.getBool();
                        sval = folly::to<std::string>(bval);
                        val.setStr(sval);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                case meta::cpp2::PropertyType::FLOAT:
                case meta::cpp2::PropertyType::DOUBLE: {
                    try {
                        bval = val.getBool();
                        fval = folly::to<double>(bval);
                        val.setFloat(fval);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                // other not need convert
                default:
                    return WriteResult::SUCCEEDED;
            }
        }
        case Value::Type::INT: {
            switch (newpropType) {
                case meta::cpp2::PropertyType::STRING:
                case meta::cpp2::PropertyType::FIXED_STRING: {
                    try {
                       ival = val.getInt();
                       sval = folly::to<std::string>(ival);
                       val.setStr(sval);
                       return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                // other not need convert
                default:
                    return WriteResult::SUCCEEDED;
            }
        }
        case Value::Type::FLOAT: {
            switch (newpropType) {
                case meta::cpp2::PropertyType::STRING:
                case meta::cpp2::PropertyType::FIXED_STRING: {
                    try {
                        fval = val.getFloat();
                        sval = folly::to<std::string>(fval);
                        val.setStr(sval);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                case meta::cpp2::PropertyType::BOOL: {
                    try {
                        fval = val.getFloat();
                        bval = folly::to<bool>(fval);
                        val.setBool(bval);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                // other not need convert
                default:
                    return WriteResult::SUCCEEDED;
            }
        }
        case Value::Type::STRING: {
            switch (newpropType) {
                case meta::cpp2::PropertyType::INT8:
                case meta::cpp2::PropertyType::INT16:
                case meta::cpp2::PropertyType::INT32:
                case meta::cpp2::PropertyType::INT64:
                case meta::cpp2::PropertyType::TIMESTAMP:
                case meta::cpp2::PropertyType::VID: {
                    try {
                        sval = val.getStr();
                        ival = folly::to<int64_t>(sval);
                        val.setInt(ival);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                case meta::cpp2::PropertyType::BOOL: {
                    try {
                        sval = val.getStr();
                        bval = folly::to<bool>(sval);
                        val.setBool(bval);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                case meta::cpp2::PropertyType::FLOAT:
                case meta::cpp2::PropertyType::DOUBLE: {
                    try {
                        sval = val.getStr();
                        fval = folly::to<double>(sval);
                        val.setFloat(fval);
                        return WriteResult::SUCCEEDED;
                    } catch (const std::exception& e) {
                        return WriteResult::TYPE_MISMATCH;
                    }
                }
                // other not need convert
                default:
                    return WriteResult::SUCCEEDED;
            }
        }
        // other not need convert
        default:
            return WriteResult::SUCCEEDED;
    }
}


// Used for vertex and edge
std::string UpgraderSpace::encodeRowVal(const RowReader* reader,
                                        const meta::NebulaSchemaProvider* schema,
                                        std::vector<std::string>& fieldName) {
    auto oldSchema = reader->getSchema();
    if (oldSchema == nullptr) {
        LOG(ERROR)  << "Schema not found from RowReader.";
        return "";
    }

    // encode v2 value, use new schema
    WriteResult wRet;
    RowWriterV2 rowWrite(schema);

    // fieldName contains all the fields of the latest schema.
    // The data reader may not use the latest schema,
    // If it does not contain the field, the default value or null value will be used.
    for (auto& name : fieldName) {
        auto val = reader->getValueByName(name);
        if (val.type() != Value::Type::NULLVALUE) {
            // If the field types are inconsistent, can be converted
            wRet = convertValue(schema, oldSchema, name, val);
            if (wRet != WriteResult::SUCCEEDED) {
                LOG(ERROR)  << "Convert value failed";
                return "";
            }
            wRet = rowWrite.setValue(name, val);
            if (wRet != WriteResult::SUCCEEDED) {
                LOG(ERROR)  << "Write rowWriterV2 failed";
                return "";
            }
        } else {
            // read null value
            auto nullType = val.getNull();
            if (nullType == NullType::__NULL__) {
                wRet = rowWrite.setValue(name, val);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(ERROR)  << "Write rowWriterV2 failed";
                    return "";
                }
            } else if (nullType != NullType::UNKNOWN_PROP) {
                // nullType == NullType::kNullUnknownProp, indicates that the field is
                // only in the latest schema, maybe use default value or null value.
                LOG(ERROR)  << "Data is illegal in " << name << " field";
                return "";
            }
        }
    }

    wRet = rowWrite.finish();
    if (wRet != WriteResult::SUCCEEDED) {
        LOG(ERROR)  << "Write rowWriterV2 failed";
        return "";
    }

    return std::move(rowWrite).moveEncodedStr();
}

std::string UpgraderSpace::indexVertexKey(PartitionID partId,
                VertexID& vId,
                RowReader* reader,
                std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::vertexIndexKey(spaceVidLen_,
                                         partId,
                                         index->get_index_id(),
                                         vId,
                                         std::move(values).value());
}

void UpgraderSpace::encodeEdgeValue(PartitionID partId,
                                    RowReader* reader,
                                    const meta::NebulaSchemaProvider* schema,
                                    std::string& newkey,
                                    VertexID& svId,
                                    EdgeType type,
                                    EdgeRanking rank,
                                    VertexID& dstId,
                                    std::vector<kvstore::KV>& data) {
    // Get all returned field name
    auto& fieldNames = edgeFieldName_[std::abs(type)];

    auto ret = encodeRowVal(reader, schema, fieldNames);
    if (ret.empty()) {
        return;
    }
    data.emplace_back(std::move(newkey), ret);

    if (type <= 0) {
        return;
    }

    // encode v2 index value
    auto it = edgeIndexes_.find(type);
    if (it != edgeIndexes_.end()) {
        // Use new RowReader
        auto nReader = RowReaderWrapper::getEdgePropReader(schemaMan_, spaceId_, type, ret);
        if (nReader == nullptr) {
            LOG(ERROR) << "Bad format row: space id " << spaceId_
                       << " edgetype " << type
                       << " srcVertexId " << svId
                       << " rank " << rank
                       << " dstVertexId " << dstId;
            return;
        }
        for (auto& index : it->second) {
            auto newIndexKey = indexEdgeKey(partId, nReader.get(), svId, rank, dstId, index);
            if (!newIndexKey.empty()) {
                data.emplace_back(std::move(newIndexKey), "");
            }
        }
    }
}

std::string UpgraderSpace::indexEdgeKey(PartitionID partId,
                        RowReader* reader,
                        VertexID& svId,
                        EdgeRanking rank,
                        VertexID& dstId,
                        std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::edgeIndexKey(spaceVidLen_,
                                       partId,
                                       index->get_index_id(),
                                       svId,
                                       rank,
                                       dstId,
                                       std::move(values).value());
}


void UpgraderSpace::doCompaction() {
    LOG(INFO) << "Path " << dstPath_ << " space id " << spaceId_
              << " compaction begin";
    auto ret = writeEngine_->compact();
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Path " << dstPath_ << " space id " << spaceId_
                   << " compaction failed!";
    }
    LOG(INFO) << "Path " << dstPath_ << " space id " << spaceId_
              << " compaction success!";
}

bool UpgraderSpace::copyWal() {
    LOG(INFO) << "Copy space id " << entry_ << " wal file begin";
    // Get source wal directory
    auto srcPath = folly::stringPrintf("%s/%s/%s/%s", srcPath_.c_str(), "nebula",
                                       entry_.c_str(), "wal");
    if (!fs::FileUtils::exist(srcPath)) {
        LOG(ERROR) << "Source data wal path " << srcPath << " not exists!";
        return false;
    }
    // Get destination wal directory
    auto dstPath = folly::stringPrintf("%s/%s/%s", dstPath_.c_str(), "nebula", entry_.c_str());
    if (!fs::FileUtils::exist(dstPath)) {
        LOG(ERROR) << "Destination data wal path " << dstPath << " not exists!";
        return false;
    }
    dstPath = fs::FileUtils::joinPath(dstPath, "wal");
    if (!fs::FileUtils::makeDir(dstPath)) {
        LOG(FATAL) << "makeDir " << dstPath << " failed";
    }

    auto partDirs = fs::FileUtils::listAllDirsInDir(srcPath.c_str());
    for (size_t i = 0; i < partDirs.size(); i++) {
        // In general, there are only two wal files left for each part
        auto files = fs::FileUtils::listAllFilesInDir(folly::stringPrintf("%s/%s", srcPath.c_str(),
                                                      partDirs[i].c_str()).c_str());
        // If the number of wal files is greater than two, find the latest two wal files
        auto walNum = files.size();
        if (walNum > 2) {
            std::sort(files.begin(), files.end());
            auto newestFile = files[walNum -2];
            auto latestFile = files[walNum -1];
            files.resize(2);
            files[0] = newestFile;
            files[1] = latestFile;
        }

        for (const auto &file : files) {
            std::fstream srcF(folly::stringPrintf("%s/%s/%s", srcPath.c_str(), partDirs[i].c_str(),
                              file.c_str()), std::ios::binary | std::ios::in);
            auto dstwalpart = folly::stringPrintf("%s/%s", dstPath.c_str(), partDirs[i].c_str());
            if (!fs::FileUtils::makeDir(dstwalpart)) {
                LOG(FATAL) << "makeDir " << dstwalpart << " failed";
            }
            std::fstream destF(folly::stringPrintf("%s/%s", dstwalpart.c_str(), file.c_str()),
                               std::ios::binary | std::ios::out);
            destF << srcF.rdbuf();
            destF.close();
            srcF.close();
        }
    }
    return true;
}

Status DbUpgrader::init(meta::MetaClient* mclient,
                        meta::ServerBasedSchemaManager*sMan,
                        meta::IndexManager* iMan,
                        const std::string& srcPath,
                        const std::string& dstPath) {
    metaClient_ = mclient;
    schemaMan_ = sMan;
    indexMan_ = iMan;
    srcPath_ = srcPath;
    dstPath_ = dstPath;
    pool_ = std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_max_concurrent_spaces);
    return listSpace();
}

Status DbUpgrader::listSpace() {
    // from srcPath_ to srcPath_/nebula
    auto path = fs::FileUtils::joinPath(srcPath_, "nebula");
    if (!fs::FileUtils::exist(path)) {
        LOG(ERROR) << "Source data path " << srcPath_ << " not exists!";
        return Status::Error("Db path '%s' not exists.", srcPath_.c_str());
    }

    if (!fs::FileUtils::exist(dstPath_)) {
         LOG(ERROR)  << "Destination data path " << dstPath_ << " not exists!";
         return Status::Error("Db path '%s' not exists.", dstPath_.c_str());
    }
    subDirs_ = fs::FileUtils::listAllDirsInDir(path.c_str());
    return Status::OK();
}

void DbUpgrader::run() {
    LOG(INFO) << "Upgrade from path " << srcPath_ << " to path "
              << dstPath_ << " in DbUpgrader run begin";
    // Get all the directories in the data directory,
    // each directory name is spaceId, traverse each directory
    std::vector<std::unique_ptr<nebula::storage::UpgraderSpace>> upgraderSpaces;
    for (auto& entry : subDirs_) {
        auto it = std::make_unique<nebula::storage::UpgraderSpace>();

        // When the init space fails, ignore to upgrade this space
        auto ret = it->init(metaClient_, schemaMan_, indexMan_, srcPath_, dstPath_, entry);
        if (!ret.ok()) {
            LOG(WARNING) << "Upgrade from path " << srcPath_  << " space id " << entry
                         << " to path " << dstPath_ << " init failed";
            LOG(WARNING) << "Ignore upgrade " << srcPath_  << " space id " << entry;
        } else {
            upgraderSpaces.emplace_back(std::move(it));
        }
    }

    unFinishedSpace_ = upgraderSpaces.size();
    for (size_t i = 0; i < upgraderSpaces.size(); i++) {
        spaceQueue_.add(upgraderSpaces[i].get());
    }

    // Parallel process space
    auto spaceConcurrency = std::min(static_cast<size_t>(FLAGS_max_concurrent_spaces),
                                     upgraderSpaces.size());
    LOG(INFO) << "Max concurrenct spaces: " << spaceConcurrency;

    for (size_t i = 0; i < spaceConcurrency; ++i) {
        pool_->add(std::bind(&DbUpgrader::doSpace, this));
    }

    while (unFinishedSpace_ != 0) {
        sleep(10);
    }

    LOG(INFO) << "Upgrade from path " << srcPath_ << " to path "
              << dstPath_ << " in DbUpgrader run end";
}

void DbUpgrader::doSpace() {
    std::chrono::milliseconds take_dura{10};
    if (auto UpSpace = spaceQueue_.try_take_for(take_dura)) {
        auto upgraderSpaceIter = *UpSpace;
        LOG(INFO) << "Upgrade from path " << upgraderSpaceIter->srcPath_
                  << " space id " << upgraderSpaceIter->entry_ << " to path "
                  << upgraderSpaceIter->dstPath_ << " begin";
        if (FLAGS_upgrade_version == 1) {
            upgraderSpaceIter->doProcessV1();
        } else {
            upgraderSpaceIter->doProcessV2();
        }

        auto ret = upgraderSpaceIter->copyWal();
        if (!ret) {
            LOG(ERROR) << "Copy space id " << upgraderSpaceIter->entry_
                       << " wal file failed";
        } else {
            LOG(INFO) << "Copy space id " << upgraderSpaceIter->entry_
                      << " wal file success";
        }

        if (FLAGS_compactions) {
            upgraderSpaceIter->doCompaction();
        }

        auto unFinishedSpace = --unFinishedSpace_;
        if (unFinishedSpace == 0) {
            // all spaces has finished
            LOG(INFO) << "Upgrade last space: " << upgraderSpaceIter->entry_
                      << " from " << upgraderSpaceIter->srcPath_ << " to path "
                      << upgraderSpaceIter->dstPath_ << " end";
        } else {
            pool_->add(std::bind(&DbUpgrader::doSpace, this));
        }
    } else {
        LOG(INFO) << "Upgrade from path " << srcPath_ << " to path "
                  << dstPath_ << " end";
    }
}

}  // namespace storage
}  // namespace nebula
