/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "DbDumper.h"
#include "utils/NebulaKeyUtils.h"
#include "fs/FileUtils.h"
#include "kvstore/RocksEngine.h"
#include "time/Duration.h"

DEFINE_string(space, "", "The space name.");
DEFINE_string(db_path, "./", "Path to rocksdb.");
DEFINE_string(meta_server, "127.0.0.1:45500", "Meta servers' address.");
DEFINE_string(mode, "scan", "Dump mode, scan | stat");
DEFINE_string(parts, "", "A list of partition id seperated by comma.");
DEFINE_string(vids, "", "A list of vertex ids seperated by comma.");
DEFINE_string(tags, "", "A list of tag name seperated by comma.");
DEFINE_string(edges, "", "A list of edge name seperated by comma.");
DEFINE_int64(limit, 1000, "Limit to output.");

namespace nebula {
namespace storage {
Status DbDumper::init() {
    auto status = initMeta();
    if (!status.ok()) {
        return status;
    }

    status = initSpace();
    if (!status.ok()) {
        return status;
    }

    status = initParams();
    if (!status.ok()) {
        return status;
    }

    status = openDb();
    if (!status.ok()) {
        return status;
    }

    return Status::OK();
}

Status DbDumper::initMeta() {
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server);
    if (!addrs.ok()) {
        return addrs.status();
    }

    auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(1);
    meta::MetaClientOptions options;
    options.skipConfig_ = true;
    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                     std::move(addrs.value()),
                                                     options);
    if (!metaClient_->waitForMetadReady(1)) {
        return Status::Error("Meta is not ready: '%s'.", FLAGS_meta_server.c_str());
    }
    schemaMng_ = std::make_unique<meta::ServerBasedSchemaManager>();
    schemaMng_->init(metaClient_.get());
    return Status::OK();
}

Status DbDumper::initSpace() {
    if (FLAGS_space.empty()) {
        return Status::Error("Space is not given.");
    }
    auto space = schemaMng_->toGraphSpaceID(FLAGS_space);
    if (!space.ok()) {
        return Status::Error("Space '%s' not found in meta server.", FLAGS_space.c_str());
    }
    spaceId_ = space.value();

    auto partNum = metaClient_->partsNum(spaceId_);
    if (!partNum.ok()) {
        return Status::Error("Get partition number from '%s' failed.", FLAGS_space.c_str());
    }
    partNum_ = partNum.value();
    return Status::OK();
}

Status DbDumper::initParams() {
    std::vector<std::string> tags, edges;
    try {
        folly::splitTo<PartitionID>(',', FLAGS_parts, std::inserter(parts_, parts_.begin()), true);
        folly::splitTo<VertexID>(',', FLAGS_vids, std::inserter(vids_, vids_.begin()), true);
        folly::splitTo<std::string>(',', FLAGS_tags, std::inserter(tags, tags.begin()), true);
        folly::splitTo<std::string>(',', FLAGS_edges, std::inserter(edges, edges.begin()), true);
    } catch (const std::exception& e) {
        return Status::Error("Parse parts/tags/edges error: %s", e.what());
    }

    for (auto& tag : tags) {
        auto tagId = schemaMng_->toTagID(spaceId_, tag);
        if (!tagId.ok()) {
            return Status::Error("Tag '%s' not found in meta.", tag.c_str());
        }
        tags_.emplace(tagId.value());
    }
    for (auto& edge : edges) {
        auto edgeType = schemaMng_->toEdgeType(spaceId_, edge);
        if (!edgeType.ok()) {
            return Status::Error("Edge '%s' not found in meta.", edge.c_str());
        }
        edges_.emplace(edgeType.value());
    }

    if (FLAGS_mode.compare("scan") != 0 && FLAGS_mode.compare("stat") != 0) {
        return Status::Error("Unkown mode '%s'.", FLAGS_mode.c_str());
    }
    return Status::OK();
}

Status DbDumper::openDb() {
    if (!fs::FileUtils::exist(FLAGS_db_path)) {
        return Status::Error("Db path '%s' not exists.", FLAGS_db_path.c_str());
    }
    auto subDirs = fs::FileUtils::listAllDirsInDir(FLAGS_db_path.c_str());
    auto spaceFound = std::find(subDirs.begin(), subDirs.end(),
                                    folly::stringPrintf("%d", spaceId_));
    if (spaceFound == subDirs.end()) {
        return Status::Error("Space '%s' not found in directory '%s'.",
                                FLAGS_space.c_str(), FLAGS_db_path.c_str());
    }
    auto path = fs::FileUtils::joinPath(FLAGS_db_path, *spaceFound);
    path = fs::FileUtils::joinPath(path, "data");

    rocksdb::DB* dbPtr;
    auto status = rocksdb::DB::OpenForReadOnly(options_, path, &dbPtr);
    if (!status.ok()) {
        return Status::Error("Unable to open database '%s' for reading: '%s'",
         path.c_str(), status.ToString().c_str());
    }
    db_.reset(dbPtr);
    return Status::OK();
}

void DbDumper::run() {
    time::Duration dur;
    auto noPrint = [] (const folly::StringPiece& key) -> bool {
        UNUSED(key);
        return false;
    };
    auto printIfTagFound = [this] (const folly::StringPiece& key) -> bool {
        auto tag = NebulaKeyUtils::getTagId(key);
        auto tagFound = tags_.find(tag);
        return !(tagFound == tags_.end());
    };
    auto printIfEdgeFound = [this] (const folly::StringPiece& key) -> bool {
        auto edge = NebulaKeyUtils::getEdgeType(key);
        auto edgeFound = edges_.find(edge);
        return !(edgeFound == edges_.end());
    };

    uint32_t bitmap =
        (parts_.empty() ? 0 : 1 << 3)
        | (vids_.empty() ? 0 : 1 << 2)
        | (tags_.empty() ? 0 : 1 << 1)
        | (edges_.empty() ? 0 : 1);
    switch (bitmap) {
        case 0b0000: {
            // nothing specified,seek to first and print them all
            seekToFirst();
            break;
        }
        case 0b0001: {
            // specified edges, seek to first and only print edges if found.
            beforePrintVertex_.emplace_back(noPrint);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            seekToFirst();
            break;
        }
        case 0b0010: {
            // specified tags, seek to first and only print vertices if found.
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(noPrint);
            seekToFirst();
            break;
        }
        case 0b0011: {
            // specified tags and edges, seek to first and print if found.
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            seekToFirst();
            break;
        }
        case 0b0100: {
            // specified vids, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                auto prefix = NebulaKeyUtils::vertexPrefix(part, vid);
                seek(prefix);
            }
            break;
        }
        case 0b0101: {
            // specified vids and edges, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto edge : edges_) {
                    auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                    seek(prefix);
                }
            }
            break;
        }
        case 0b0110: {
            // specified vids and tags, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto tag : tags_) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                    seek(prefix);
                }
            }
            break;
        }
        case 0b0111: {
            // specified vids and edges, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto edge : edges_) {
                    auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                    seek(prefix);
                }
            }
            // specified vids and tags, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto tag : tags_) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                    seek(prefix);
                }
            }
            break;
        }
        case 0b1000: {
            // specified part, seek with prefix and print them all
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                seek(prefix);
            }
            break;
        }
        case 0b1001: {
            // specified part and edge, seek with prefix and print edge if found
            beforePrintVertex_.emplace_back(noPrint);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                seek(prefix);
            }
            break;
        }
        case 0b1010: {
            // specified part and tag, seek with prefix and print vertex if found
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(noPrint);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                seek(prefix);
            }
            break;
        }
        case 0b1011: {
            // specified part/tag/edge, with prefix and print
            beforePrintVertex_.emplace_back(noPrint);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                seek(prefix);
            }

            beforePrintVertex_.clear();
            beforePrintEdge_.clear();
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(noPrint);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                seek(prefix);
            }
            break;
        }
        case 0b1100: {
            // specified part and vid
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(part, vid);
                    seek(prefix);
                }
            }
            break;
        }
        case 0b1101: {
            // specified part/vid/edge
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto edge : edges_) {
                        auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                        seek(prefix);
                    }
                }
            }
            break;
        }
        case 0b1110: {
            // specified part/vid/tag
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto tag : tags_) {
                        auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                        seek(prefix);
                    }
                }
            }
            break;
        }
        case 0b1111: {
            // specified part/vid/tag/edge
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto edge : edges_) {
                        auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                        seek(prefix);
                    }
                }
            }

            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto tag : tags_) {
                        auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                        seek(prefix);
                    }
                }
            }
            break;
        }
        default: {
            std::cerr << "error";
        }
    }

    std::cout << "===========================STATISTICS============================\n";
    std::cout << "COUNT: " << count_ << "\n";
    std::cout << "VERTEX COUNT: " << vertexCount_ << "\n";
    std::cout << "EDGE COUNT: " << edgeCount_ << "\n";
    std::cout << "TAG STATISTICS: \n";
    for (auto &t : tagStat_) {
        std::cout << "\t" << getTagName(t.first) << " : " << t.second << "\n";
    }
    std::cout << "EDGE STATISTICS: \n";
    for (auto &e : edgeStat_) {
        std::cout << "\t" << getEdgeName(e.first) << " : " << e.second << "\n";
    }
    std::cout << "============================STATISTICS===========================\n";
    std::cout << "Time cost: " << dur.elapsedInUSec() << " us\n\n";
}

void DbDumper::seekToFirst() {
    const auto it = db_->NewIterator(rocksdb::ReadOptions());
    it->SeekToFirst();
    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, "");
    iterates(prefixIt.get());
}

void DbDumper::seek(std::string& prefix) {
    const auto it = db_->NewIterator(rocksdb::ReadOptions());
    it->Seek(rocksdb::Slice(prefix));
    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
    iterates(prefixIt.get());
}

void DbDumper::iterates(kvstore::RocksPrefixIter* it) {
    for (; it->valid(); it->next()) {
        if (FLAGS_limit > 0 && count_ >= FLAGS_limit) {
            break;
        }

        auto key = it->key();
        auto value = it->val();

        if (NebulaKeyUtils::isVertex(key)) {
            // filts the data
            bool isFiltered = false;
            for (auto& cb : beforePrintVertex_) {
                if (!cb(key)) {
                    isFiltered = true;
                    break;
                }
            }
            if (isFiltered) {
                continue;
            }

            auto tagId = NebulaKeyUtils::getTagId(key);
            // only print to screen with scan mode
            if (FLAGS_mode == "scan") {
                printTagKey(key);
                auto reader =
                    RowReader::getTagPropReader(schemaMng_.get(), value, spaceId_, tagId);
                printValue(reader.get());
            }

            // statistics
            auto tagStat = tagStat_.find(NebulaKeyUtils::getTagId(key));
            if (tagStat == tagStat_.end()) {
                tagStat_.emplace(tagId, 1);
            } else {
                ++(tagStat->second);
            }
            ++vertexCount_;
            ++count_;
        } else if (NebulaKeyUtils::isEdge(key)) {
            // filts the data
            bool isFiltered = false;
            for (auto &cb : beforePrintEdge_) {
                if (!cb(key)) {
                    isFiltered = true;
                    break;
                }
            }
            if (isFiltered) {
                continue;
            }

            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            if (edgeType < 0) {
                // reverse edge will be discarded
                continue;
            }
            // only print to screen with scan mode
            if (FLAGS_mode == "scan") {
                printEdgeKey(key);
                auto reader =
                    RowReader::getEdgePropReader(schemaMng_.get(), value, spaceId_, edgeType);
                printValue(reader.get());
            }

            // statistics
            auto edgeStat = edgeStat_.find(edgeType);
            if (edgeStat == edgeStat_.end()) {
                edgeStat_.emplace(edgeType, 1);
            } else {
                ++(edgeStat->second);
            }
            ++edgeCount_;
            ++count_;
        }
    }
}

inline void DbDumper::printTagKey(const folly::StringPiece& key) {
    auto part = NebulaKeyUtils::getPart(key);
    auto vid = NebulaKeyUtils::getVertexId(key);
    auto tagId = NebulaKeyUtils::getTagId(key);
    std::cout << "[vertex] key: "
            << part << ", "
            << vid << ", "
            << getTagName(tagId);
}

inline void DbDumper::printEdgeKey(const folly::StringPiece& key) {
    auto part = NebulaKeyUtils::getPart(key);
    auto edgeType = NebulaKeyUtils::getEdgeType(key);
    auto src = NebulaKeyUtils::getSrcId(key);
    auto dst = NebulaKeyUtils::getDstId(key);
    auto rank = NebulaKeyUtils::getRank(key);
    std::cout << "[edge] key: "
            << part << ", "
            << src << ", "
            << getEdgeName(edgeType) << ", "
            << rank << ", "
            << dst;
}

void DbDumper::printValue(const RowReader* reader) {
    if (reader == nullptr) {
        return;
    }
    std::cout << " value: ";
    auto schema = reader->getSchema();
    if (schema == nullptr) {
        std::cerr << "schema not found.";
        return;
    }
    auto iter = schema->begin();
    size_t index = 0;
    while (iter) {
        auto field = RowReader::getPropByIndex(reader, index);
        if (!ok(field)) {
            continue;
        }
        std::cout << value(field) << ", ";
        ++iter;
        ++index;
    }
    std::cout << "\n";
}

std::string DbDumper::getTagName(const TagID tagId) {
    auto name = schemaMng_->toTagName(spaceId_, tagId);
    if (!name.ok()) {
        return folly::stringPrintf("%d", tagId);
    } else {
        return name.value();
    }
}

std::string DbDumper::getEdgeName(const EdgeType edgeType) {
    auto name = schemaMng_->toEdgeName(spaceId_, edgeType);
    if (!name.ok()) {
        return folly::stringPrintf("%d", edgeType);
    } else {
        return name.value();
    }
}
}  // namespace storage
}  // namespace nebula
