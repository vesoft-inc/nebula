/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "DbDumper.h"
#include "base/NebulaKeyUtils.h"
#include "dataman/RowReader.h"
#include "fs/FileUtils.h"
#include "kvstore/RocksEngine.h"

DEFINE_string(space, "", "The space name.");
DEFINE_string(db_path, "./", "Path to rocksdb.");
DEFINE_string(meta_server, "127.0.0.1:45500", "Meta servers' address.");
DEFINE_string(mode, "scan", "Dump mode, scan | prefix | stat");
DEFINE_string(parts, "", "A list of partition id seperated by comma.");
DEFINE_string(vids, "", "A list of vertex id seperated by comma.");
DEFINE_string(tags, "", "Filtering with given tags.");
DEFINE_string(edges, "", "Filtering with given edges.");
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
    FLAGS_load_data_interval_secs = 3600;
    std::cout << "Meta server: " << FLAGS_meta_server << "\n";
    auto addrs = network::NetworkUtils::toHosts(FLAGS_meta_server);
    if (!addrs.ok()) {
        return addrs.status();
    }

    auto ioExecutor = std::make_shared<folly::IOThreadPoolExecutor>(1);
    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor,
                                                    std::move(addrs.value()),
                                                    HostAddr(0, 0),
                                                    0,
                                                    false);
    if (!metaClient_->waitForMetadReady(1)) {
        return Status::Error("Meta is not ready: '%s'.", FLAGS_meta_server.c_str());
    }
    schemaMng_ = std::make_unique<meta::ServerBasedSchemaManager>();
    schemaMng_->init(metaClient_.get());
    return Status::OK();
}

Status DbDumper::initSpace() {
    std::cout << "Space: " << FLAGS_space << "\n";
    if (FLAGS_space == "") {
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
        std::cout << "parts: " << FLAGS_parts << "\n";
        folly::splitTo<PartitionID>(',', FLAGS_parts, std::inserter(parts_, parts_.begin()), true);
        std::cout << "vids: " << FLAGS_vids << "\n";
        folly::splitTo<VertexID>(',', FLAGS_vids, std::inserter(vids_, vids_.begin()), true);
        std::cout << "tags: " << FLAGS_tags << "\n";
        folly::splitTo<std::string>(',', FLAGS_tags, std::inserter(tags, tags.begin()), true);
        std::cout << "edges: " << FLAGS_edges << "\n";
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
    return Status::OK();
}

Status DbDumper::openDb() {
    std::cout << "Path: " << FLAGS_db_path << "\n";
    if (!fs::FileUtils::exist(FLAGS_db_path)) {
        return Status::Error("Db path '%s' not exist.", FLAGS_db_path.c_str());
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
    auto noPrint = [] (const folly::StringPiece& key) -> bool {
        UNUSED(key);
        return false;
    };
    auto printIfTagFound = [this] (const folly::StringPiece& key) -> bool {
        auto tag = NebulaKeyUtils::getTagId(key);
        auto tagFound = tags_.find(tag);
        if (tagFound == tags_.end()) {
            return false;
        }

        return true;
    };
    auto printIfEdgeFound = [this] (const folly::StringPiece& key) -> bool {
        auto edge = NebulaKeyUtils::getEdgeType(key);
        auto edgeFound = edges_.find(edge);
        if (edgeFound == edges_.end()) {
            return false;
        }

        return true;
    };

    uint8_t bitmap =
        (parts_.empty() ? 0 : 1 << 3)
        | (vids_.empty() ? 0 : 1 << 2)
        | (tags_.empty() ? 0 : 1 << 1)
        | (edges_.empty() ? 0 : 1);
    std::cout << bitmap;
    switch (bitmap) {
        case 0b00000000: {
            // nothing specified,seek to first and print them all
            const auto it = db_->NewIterator(rocksdb::ReadOptions());
            it->SeekToFirst();
            const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, "");
            scan(prefixIt.get());
            break;
        }
        case 0b00000001: {
            // specified edges, seek to first and only print edges if found.
            beforePrintVertex_.emplace_back(noPrint);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            const auto it = db_->NewIterator(rocksdb::ReadOptions());
            it->SeekToFirst();
            const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, "");
            scan(prefixIt.get());
            break;
        }
        case 0b00000010: {
            // specified tags, seek to first and only print vertices if found.
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(noPrint);
            const auto it = db_->NewIterator(rocksdb::ReadOptions());
            it->SeekToFirst();
            const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, "");
            scan(prefixIt.get());
            break;
        }
        case 0b00000011: {
            // specified tags and edges, seek to first and print if found.
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            const auto it = db_->NewIterator(rocksdb::ReadOptions());
            it->SeekToFirst();
            const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, "");
            scan(prefixIt.get());
            break;
        }
        case 0b00000100: {
            // specified vids, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                auto prefix = NebulaKeyUtils::vertexPrefix(part, vid);
                const auto it = db_->NewIterator(rocksdb::ReadOptions());
                it->Seek(rocksdb::Slice(prefix));
                const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                scan(prefixIt.get());
            }
            break;
        }
        case 0b00000101: {
            // specified vids and edges, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto edge : edges_) {
                    auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                    const auto it = db_->NewIterator(rocksdb::ReadOptions());
                    it->Seek(rocksdb::Slice(prefix));
                    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                    scan(prefixIt.get());
                }
            }
            break;
        }
        case 0b00000110: {
            // specified vids and tags, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto tag : tags_) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                    const auto it = db_->NewIterator(rocksdb::ReadOptions());
                    it->Seek(rocksdb::Slice(prefix));
                    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                    scan(prefixIt.get());
                }
            }
            break;
        }
        case 0b00000111: {
            // specified vids and edges, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto edge : edges_) {
                    auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                    const auto it = db_->NewIterator(rocksdb::ReadOptions());
                    it->Seek(rocksdb::Slice(prefix));
                    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                    scan(prefixIt.get());
                }
            }
            // specified vids and tags, seek with prefix and print.
            for (auto vid : vids_) {
                auto part = ID_HASH(vid, partNum_);
                for (auto tag : tags_) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                    const auto it = db_->NewIterator(rocksdb::ReadOptions());
                    it->Seek(rocksdb::Slice(prefix));
                    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                    scan(prefixIt.get());
                }
            }
            break;
        }
        case 0b00001000: {
            // specified part, seek with prefix and print them all
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                const auto it = db_->NewIterator(rocksdb::ReadOptions());
                it->Seek(rocksdb::Slice(prefix));
                const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                scan(prefixIt.get());
            }
            break;
        }
        case 0b00001001: {
            // specified part, seek with prefix and print edge if found
            beforePrintVertex_.emplace_back(noPrint);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                const auto it = db_->NewIterator(rocksdb::ReadOptions());
                it->Seek(rocksdb::Slice(prefix));
                const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                scan(prefixIt.get());
            }
            break;
        }
        case 0b00001010: {
            // specified part, seek with prefix and print vertex if found
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(noPrint);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                const auto it = db_->NewIterator(rocksdb::ReadOptions());
                it->Seek(rocksdb::Slice(prefix));
                const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                scan(prefixIt.get());
            }
            break;
        }
        case 0b00001011: {
            beforePrintVertex_.emplace_back(noPrint);
            beforePrintEdge_.emplace_back(printIfEdgeFound);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                const auto it = db_->NewIterator(rocksdb::ReadOptions());
                it->Seek(rocksdb::Slice(prefix));
                const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                scan(prefixIt.get());
            }

            beforePrintVertex_.clear();
            beforePrintEdge_.clear();
            beforePrintVertex_.emplace_back(printIfTagFound);
            beforePrintEdge_.emplace_back(noPrint);
            for (auto part : parts_) {
                auto prefix = NebulaKeyUtils::prefix(part);
                const auto it = db_->NewIterator(rocksdb::ReadOptions());
                it->Seek(rocksdb::Slice(prefix));
                const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                scan(prefixIt.get());
            }
            break;
        }
        case 0b00001100: {
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(part, vid);
                    const auto it = db_->NewIterator(rocksdb::ReadOptions());
                    it->Seek(rocksdb::Slice(prefix));
                    const auto prefixIt = std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                    scan(prefixIt.get());
                }
            }
            break;
        }
        case 0b00001101: {
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto edge : edges_) {
                        auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                        const auto it = db_->NewIterator(rocksdb::ReadOptions());
                        it->Seek(rocksdb::Slice(prefix));
                        const auto prefixIt =
                            std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                        scan(prefixIt.get());
                    }
                }
            }
            break;
        }
        case 0b00001110: {
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto tag : tags_) {
                        auto prefix = NebulaKeyUtils::vertexPrefix(part, vid, tag);
                        const auto it = db_->NewIterator(rocksdb::ReadOptions());
                        it->Seek(rocksdb::Slice(prefix));
                        const auto prefixIt =
                            std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                        scan(prefixIt.get());
                    }
                }
            }
            break;
        }
        case 0b00001111: {
            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto edge : edges_) {
                        auto prefix = NebulaKeyUtils::edgePrefix(part, vid, edge);
                        const auto it = db_->NewIterator(rocksdb::ReadOptions());
                        it->Seek(rocksdb::Slice(prefix));
                        const auto prefixIt =
                            std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                        scan(prefixIt.get());
                    }
                }
            }

            for (auto part : parts_) {
                for (auto vid : vids_) {
                    for (auto tag : tags_) {
                        auto prefix = NebulaKeyUtils::edgePrefix(part, vid, tag);
                        const auto it = db_->NewIterator(rocksdb::ReadOptions());
                        it->Seek(rocksdb::Slice(prefix));
                        const auto prefixIt =
                            std::make_unique<kvstore::RocksPrefixIter>(it, prefix);
                        scan(prefixIt.get());
                    }
                }
            }
            break;
        }
        default: {
            std::cerr << "error";
        }
    }
}

void DbDumper::scan(kvstore::RocksPrefixIter* it) {
    for (; it->valid(); it->next()) {
        auto key = it->key();
        auto value = it->val();

        if (NebulaKeyUtils::isVertex(key)) {
            if (!printTagKey(key)) {
                continue;
            }

            auto schema = schemaMng_->getTagSchema(spaceId_, NebulaKeyUtils::getTagId(key));
            if (schema == nullptr) {
                continue;
            }
            printValue(value, schema);
            ++count_;
        }

        if (NebulaKeyUtils::isEdge(key)) {
            if (!printEdgeKey(key)) {
                continue;
            }

            auto schema = schemaMng_->getEdgeSchema(spaceId_, NebulaKeyUtils::getEdgeType(key));
            if (schema == nullptr) {
                continue;
            }
            printValue(value, schema);
            ++count_;
        }

        if (FLAGS_limit > 0 && count_ > FLAGS_limit) {
            break;
        }
    }
}

bool DbDumper::printTagKey(const folly::StringPiece& key) {
    for (auto& cb : beforePrintVertex_) {
        if (!cb(key)) {
            return false;
        }
    }

    auto part = NebulaKeyUtils::getPart(key);
    auto vid = NebulaKeyUtils::getVertexId(key);
    auto tagId = NebulaKeyUtils::getTagId(key);
    std::cout << "[vertex] key: "
            << part << ", "
            << vid << ", "
            << getTagName(tagId);
    return true;
}

bool DbDumper::printEdgeKey(const folly::StringPiece& key) {
    for (auto &cb : beforePrintEdge_) {
        if (!cb(key)) {
            return false;
        }
    }
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
    return true;
}

void DbDumper::printValue(const folly::StringPiece& data,
                          const std::shared_ptr<const meta::SchemaProviderIf> schema) {
    std::cout << " value:";
    auto reader = RowReader::getRowReader(data, schema);
    auto iter = schema->begin();
    size_t index = 0;
    while (iter) {
        auto field = RowReader::getPropByIndex(reader.get(), index);
        if (!ok(field)) {
            continue;
        }
        std::cout << value(field) << ",";
        ++iter;
        ++index;
    }
    std::cout << "\n";
}

std::string DbDumper::getTagName(const TagID tagId) {
    // return metaClient_->getTagName();
    return folly::stringPrintf("%d", tagId);
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
