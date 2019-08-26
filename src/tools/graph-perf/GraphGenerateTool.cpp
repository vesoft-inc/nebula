/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "GraphGenerateTool.h"
#include "base/Base.h"
#include "network/NetworkUtils.h"
#include "thread/GenericWorker.h"
#include "time/Duration.h"
#include "graph/GraphFlags.h"

#include <algorithm>
#include <limits>
#include <folly/gen/Base.h>

DEFINE_string(graph_server_addrs, "127.0.0.1:3699", "The graph server address");
DEFINE_string(space_name, "graph_pref_test", "Specify the space name");
DEFINE_int32(max_string_prop_size, 32, "The max size of string property");
DEFINE_int64(vertices, 100000, "Estimated number of vertex in real scenario");
DEFINE_int64(edges, 400000, "Estimated number of edge real scenario");
DEFINE_int32(batch_size, 1024, "the unit of a batch");
DEFINE_int32(batch_cross, 3, "batch cross index, the greater value, the lower probability");
DEFINE_string(desc_file, "desc.conf", "The full path of the description file");

namespace nebula {
namespace graph {

static const char constChars[] = "!@#$%^&*0123456789"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz";

int GraphGenerateTool::run() {
    if (!init() || !getGraphClients() || !getTags() || !getEdgeTypes()) {
        return EXIT_FAILURE;
    }

    time::Duration duration;
    this->genRatableGraph();
    LOG(INFO) << "Generate the ratable graph cost " << duration.elapsedInMSec() << " ms.";
    return 0;
}


void GraphGenerateTool::genRatableGraph() {
    if (minBatchSize_ > FLAGS_batch_size) {
        LOG(ERROR) << "The batch size should greater than " << minBatchSize_;
        return;
    }
    VertexID vid = 0;
    VertexID srcVid = 0;
    VertexID dstVid = 0;
    int64_t totalVertices = 0L;
    int64_t totalEdges = 0L;
    std::default_random_engine generator;
    for (auto& tag : tags_) {
        std::vector<VertexID> vIds;
        vIds.reserve(FLAGS_batch_size);
        tagMapVertex_[tag] = std::move(vIds);
    }
    for (int64_t i = 0; i < FLAGS_vertices / FLAGS_batch_size + 1; i++) {
        // default: rand 2/3 clearing the batch vertex pools
        for (auto& tmv : tagMapVertex_) {
            if (folly::Random::rand32(FLAGS_batch_cross)) {
                tmv.second.clear();
            }
        }
        for (int64_t j = 0; (j < FLAGS_batch_size) && (totalVertices < FLAGS_vertices); j++) {
            auto number = distributionTag_(generator);
            auto tag = descVertex_[number].first;
            vid = folly::Random::rand64(i * FLAGS_batch_size, (i + 1) * FLAGS_batch_size);
            tagMapVertex_[tag].emplace_back(vid);
            auto query = genInsertVertexSentence(tag, vid);
            LOG(INFO) << query;
            auto c = folly::Random::rand64(gClients_.size());
            cpp2::ExecutionResponse resp;
            auto code = gClients_[c]->execute(query, resp);
            if (cpp2::ErrorCode::SUCCEEDED != code) {
                LOG(ERROR) << "Do insert vertex: " << query << " failed";
            }
            totalVertices++;
        }
        for (int64_t j = 0;
             j < FLAGS_batch_size * (FLAGS_edges / FLAGS_vertices) && totalEdges < FLAGS_edges;
             j++) {
            auto number = distributionArc_(generator);
            auto srcTag = std::get<0>(descEdge_[number].first);
            auto srcIt = tagMapVertex_.find(srcTag);
            if (srcIt != tagMapVertex_.end()) {
                if (srcIt->second.size() > 0) {
                    auto srcIndex = folly::Random::rand64(srcIt->second.size());
                    srcVid = srcIt->second[srcIndex];
                } else {
                    srcVid = folly::Random::rand64(FLAGS_vertices);
                    srcIt->second.emplace_back(srcVid);
                }
            } else {
                LOG(ERROR) << "Invalid src tag: " << srcTag;
                return;
            }
            auto dstTag = std::get<2>(descEdge_[number].first);
            auto dstIt = tagMapVertex_.find(dstTag);
            if (dstIt != tagMapVertex_.end()) {
                if (dstIt->second.size() > 0) {
                    auto dstIndex = folly::Random::rand64(dstIt->second.size());
                    dstVid = dstIt->second[dstIndex];
                } else {
                    dstVid = folly::Random::rand64(FLAGS_vertices);
                    dstIt->second.emplace_back(dstVid);
                }
            } else {
                LOG(ERROR) << "Invalid dst tag: " << dstTag;
                return;
            }
            auto edgeType = std::get<1>(descEdge_[number].first);
            auto query = genInsertEdgeSentence(edgeType, srcVid, dstVid);
            LOG(INFO) << query;
            cpp2::ExecutionResponse resp;
            auto c = folly::Random::rand64(gClients_.size());
            auto code = gClients_[c]->execute(query, resp);
            if (cpp2::ErrorCode::SUCCEEDED != code) {
                LOG(ERROR) << "Do insert edge: " << query << " failed";
            }
            totalEdges++;
        }
    }
    for (auto& tmv : tagMapVertex_) {
        tmv.second.clear();
    }
}


bool GraphGenerateTool::init() {
    DCHECK(!FLAGS_desc_file.empty()) << "desc file is required";
    Configuration conf;
    auto status = conf.parseFromFile(FLAGS_desc_file);
    CHECK(status.ok()) << status;

    CHECK(conf.forEachItem([this] (const std::string& name,
                                   const folly::dynamic& items) {
        CHECK(items.isArray());
        if (name == "VERTEX") {
            LOG(INFO) << "Read the tag proportion ...";
            for (auto& fields : items) {
                LOG(INFO) << "fields: " << fields;
                std::vector<std::pair<std::string, std::string>> itemKV(2);
                for (auto& item : fields) {
                    CHECK(item.isString());
                    std::vector<std::string> parts;
                    folly::split(':', item.getString(), parts, true);
                    CHECK_EQ(parts.size(), 2);
                    folly::StringPiece key = folly::trimWhitespace(parts[0]);
                    folly::StringPiece value = folly::trimWhitespace(parts[1]);
                    if (key == "TagName") {
                        itemKV[0] = std::make_pair(key.toString(), value.toString());
                    } else if (key == "PrOrNumber") {
                        itemKV[1] = std::make_pair(key.toString(), value.toString());
                    } else {
                        LOG(ERROR) << "Invalid VERTEX description";
                        CHECK(false);
                    }
                }
                descVertex_.emplace_back(std::make_pair(itemKV[0].second,
                                                        folly::to<int64_t>(itemKV[1].second)));
            }
            auto tagWeights = folly::gen::from(descVertex_)
                              | folly::gen::map([&] (const std::pair<std::string, int64_t>& item) {
                                      return item.second;
                              })
                              | folly::gen::as<std::vector>();
            for (auto& tag : tagWeights) {
                minBatchSize_ += tag;
                LOG(INFO) << "Tag Weight: " << tag;
            }
            distributionTag_ = std::discrete_distribution<int64_t>(tagWeights.begin(),
                                                                   tagWeights.end());
        } else if (name == "EDGE") {
            LOG(INFO) << "Read the edge proportion ...";
            for (auto& fields : items) {
                LOG(INFO) << "fields: " << fields;
                std::vector<std::pair<std::string, std::string>> itemKV(4);
                for (auto& item : fields) {
                    CHECK(item.isString());
                    std::vector<std::string> parts;
                    folly::split(':', item.getString(), parts, true);
                    CHECK_EQ(parts.size(), 2);
                    folly::StringPiece key = folly::trimWhitespace(parts[0]);
                    folly::StringPiece value = folly::trimWhitespace(parts[1]);
                    if (key == "SrcTag") {
                        itemKV[0] = std::make_pair(key.toString(), value.toString());
                    } else if (key == "EdgeType") {
                        itemKV[1] = std::make_pair(key.toString(), value.toString());
                    } else if (key == "DstTag") {
                        itemKV[2] = std::make_pair(key.toString(), value.toString());
                    } else if (key == "PrOrNumber") {
                        itemKV[3] = std::make_pair(key.toString(), value.toString());
                    } else {
                        LOG(ERROR) << "Invalid EDGE description";
                        CHECK(false);
                    }
                }
                Arc arc = std::make_tuple(itemKV[0].second, itemKV[1].second, itemKV[2].second);
                descEdge_.emplace_back(std::make_pair(std::move(arc),
                                                      folly::to<int64_t>(itemKV[3].second)));
            }
            auto edgeWeights = folly::gen::from(descEdge_)
                               | folly::gen::map([&] (const std::pair<Arc, int64_t>& item) {
                                       return item.second;
                               })
                               | folly::gen::as<std::vector>();
            for (auto& edge : edgeWeights) {
                LOG(INFO) << "Edge Weight: " << edge;
            }
            distributionArc_ = std::discrete_distribution<int64_t>(edgeWeights.begin(),
                                                                   edgeWeights.end());
        } else {
            LOG(ERROR) << "Invalid description file: " << FLAGS_desc_file;
            CHECK(false);
        }
    }).ok());

    return true;
}


bool GraphGenerateTool::getGraphClients() {
    auto graphAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_graph_server_addrs);
    if (!graphAddrsRet.ok() || graphAddrsRet.value().empty()) {
        LOG(ERROR) << "Can't get graphServer address, status:" << graphAddrsRet.status()
                   << ", FLAGS_graph_server_addrs:" << FLAGS_graph_server_addrs;
        return false;
    }
    auto hosts = graphAddrsRet.value();
    for (auto& host : hosts) {
        auto ipv4 = network::NetworkUtils::intToIPv4(host.first);
        auto client = std::make_unique<graph::GraphClient>(ipv4, host.second);
        if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
            LOG(ERROR) << "Failed connect to " << ipv4 << ":" << host.second;
            return false;
        }
        gClients_.emplace_back(std::move(client));
    }
    for (auto& client : gClients_) {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE " + FLAGS_space_name;
        auto code = client->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
           LOG(ERROR) << "Do cmd:" << cmd << " failed";
           return false;
        }
    }
    return gClients_.size() > 0UL;
}


bool GraphGenerateTool::getTags() {
    std::string query = "SHOW TAGS;";
    cpp2::ExecutionResponse resp;
    auto code = gClients_[0]->execute(query, resp);
    if (cpp2::ErrorCode::SUCCEEDED != code) {
        LOG(ERROR) << "Do query: " << query << " failed";
        return false;
    }
    CHECK(resp.get_rows() != nullptr);
    for (auto &row : *resp.get_rows()) {
        auto &columns = row.get_columns();
        tags_.emplace_back(columns[0].get_str());
    }
    for (auto& tag : tags_) {
        query = "DESCRIBE TAG " + tag;
        auto schema = getSchema(query);
        if (schema.empty()) return false;
        tagSchema_[tag] = std::move(schema);
    }
    return true;
}


bool GraphGenerateTool::getEdgeTypes() {
    std::string query = "SHOW EDGES;";
    cpp2::ExecutionResponse resp;
    auto code = gClients_[0]->execute(query, resp);
    if (cpp2::ErrorCode::SUCCEEDED != code) {
        LOG(ERROR) << "Do query: " << query << " failed";
        return false;
    }
    CHECK(resp.get_rows() != nullptr);
    for (auto &row : *resp.get_rows()) {
        auto &columns = row.get_columns();
        edges_.emplace_back(columns[0].get_str());
    }
    for (auto& edge : edges_) {
        query = "DESCRIBE EDGE " + edge;
        auto schema = getSchema(query);
        if (schema.empty()) return false;
        tagSchema_[edge] = std::move(schema);
    }
    return true;
}


StringSchema GraphGenerateTool::getSchema(std::string query) {
    cpp2::ExecutionResponse resp;
    auto code = gClients_[0]->execute(query, resp);
    if (cpp2::ErrorCode::SUCCEEDED != code) {
        LOG(ERROR) << "Do query: " << query << " failed";
    }
    CHECK(resp.get_rows() != nullptr);
    StringSchema schema;
    for (auto &row : *resp.get_rows()) {
        auto &columns = row.get_columns();
        schema.emplace_back(std::make_pair(columns[0].get_str(), columns[1].get_str()));
    }
    return std::move(schema);
}


std::string GraphGenerateTool::genRandomString() {
    std::string roandomString;
    auto length = folly::Random::rand32(FLAGS_max_string_prop_size);
    for (unsigned int i = 0; i < length; i++) {
        roandomString += constChars[folly::Random::rand32(sizeof(constChars) - 1)];
    }
    return roandomString;
}


std::string GraphGenerateTool::genRandomData(const std::string type) {
    if (type == "int") {
        auto random32 = folly::Random::rand32(std::numeric_limits<int32_t>::max());
        return folly::to<std::string>(random32);
    } else if (type == "string") {
        std::string str = genRandomString();
        return std::string("\"" + str + "\"");
    } else if (type == "double") {
        auto rd = folly::Random::randDouble(0, std::numeric_limits<int32_t>::max());
        return folly::to<std::string>(rd);
    } else if (type == "bigint") {
        auto random64 = folly::Random::rand64(std::numeric_limits<int64_t>::max());
        return folly::to<std::string>(random64);
    } else if (type == "bool") {
        if (folly::Random::rand32(2)) {
            return std::string("true");
        } else {
            return std::string("false");
        }
    } else if (type == "timestamp") {
        // The mainstream Linux kernel's implementation constrains this
        static const int64_t maxTimestamp = std::numeric_limits<int64_t>::max() / 1000000000;
        auto random64 = folly::Random::rand64(maxTimestamp);
        return folly::to<std::string>(random64);
    } else {
        LOG(ERROR) << "Generate " << type << " failed, Unkonwn type.";
    }
    return std::string("");
}

std::string GraphGenerateTool::genInsertVertexSentence(const std::string tag, VertexID vid) {
    auto *fmt = "INSERT VERTEX NO OVERWRITE %s(%s) VALUES %s:(%s);";
    auto scheam = tagSchema_[tag];
    std::string fields;
    std::string values;
    fields.reserve(256);
    values.reserve(256);
    for (auto& s : scheam) {
        fields += s.first + ", ";
        auto value = genRandomData(s.second);
        values += value + ", ";
    }
    if (!fields.empty() && !values.empty()) {
        fields.resize(fields.size() - 2);
        values.resize(values.size() - 2);
    }
    auto vertex = folly::to<std::string>(vid);
    auto query = folly::stringPrintf(fmt, tag.c_str(), fields.c_str(),
                                     vertex.c_str(), values.c_str());
    return query;
}

std::string GraphGenerateTool::genInsertEdgeSentence(const std::string edgeType,
                                                     VertexID srcVid,
                                                     VertexID dstVid) {
    auto *fmt = "INSERT EDGE NO OVERWRITE %s(%s) VALUES %s -> %s:(%s);";
    auto scheam = tagSchema_[edgeType];
    std::string fields;
    std::string values;
    fields.reserve(256);
    values.reserve(256);
    for (auto& s : scheam) {
        fields += s.first + ", ";
        auto value = genRandomData(s.second);
        values += value + ", ";
    }
    if (!fields.empty() && !values.empty()) {
        fields.resize(fields.size() - 2);
        values.resize(values.size() - 2);
    }
    auto src = folly::to<std::string>(srcVid);
    auto dst = folly::to<std::string>(dstVid);
    auto query = folly::stringPrintf(fmt, edgeType.c_str(), fields.c_str(),
                                     src.c_str(), dst.c_str(), values.c_str());
    return query;
}

}  // namespace graph
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::graph::GraphGenerateTool generate;
    return generate.run();
}

