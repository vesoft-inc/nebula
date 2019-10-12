/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "GraphGenerateTool.h"
#include "base/Base.h"
#include "network/NetworkUtils.h"
#include "thread/GenericThreadPool.h"
#include "time/Duration.h"
#include "graph/GraphFlags.h"

#include <algorithm>
#include <folly/gen/Base.h>

DEFINE_int32(threads, 8, "Total threads for generate data");
DEFINE_string(graph_server_addrs, "127.0.0.1:3699", "The graph server address");
DEFINE_string(space_name, "graph_pref_test", "Specify the space name");
DEFINE_int32(max_string_prop_size, 32, "The max size of string property");
DEFINE_int64(vertices, 128, "Estimated number of vertex in real scenario");
DEFINE_int64(edges, 256, "Estimated number of edge in real scenario");
DEFINE_int32(batch_size, 64, "The unit of a batch");
DEFINE_int32(batch_cross, 3, "Batch cross index, the greater value, the lower probability");
DEFINE_string(desc_file, "desc.conf", "The full path of the description file");

namespace nebula {
namespace graph {

static const char constChars[] = "!@#$%^&*0123456789"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz";

int GraphGenerateTool::run() {
    if (!init() || !getTags() || !getEdgeTypes()) {
        return EXIT_FAILURE;
    }
    if (minBatchSize_ > FLAGS_batch_size) {
        LOG(ERROR) << "The batch size should greater than " << minBatchSize_;
        return EXIT_FAILURE;
    }

    time::Duration duration;
    int64_t interval = (FLAGS_vertices + FLAGS_threads - 1) / FLAGS_threads;
    nebula::thread::GenericThreadPool pool;
    pool.start(FLAGS_threads);
    for (auto i = 0; i < FLAGS_threads; i++) {
        int64_t start = i * interval;
        int64_t end = (i + 1) * interval;
        if (end > FLAGS_vertices) {
            end = FLAGS_vertices;
        }
        LOG(INFO) << "Thread: " << i << ", start = " << start << ", end = " << end;
        pool.addTask(&GraphGenerateTool::genRatableGraph, this, start, end);
    }
    pool.stop();
    pool.wait();

    LOG(INFO) << "The total succeed vertices are " << totalSucceedVertices_
              << ", the total failure vertices are " << totalFailureVertices_
              << ", the total succeed edges are " << totalSucceedEdges_
              << ", the total failure edges are " << totalFailureEdges_;
    LOG(INFO) << "Generate the ratable graph cost " << duration.elapsedInMSec() << " ms. "
              << "The average insert vertex latency is "
              << totalInsertVertexLatency_ / totalSucceedVertices_ << " us, "
              << "the average insert edge latency is "
              << totalInsertEdgeLatency_ / totalSucceedEdges_ << " us.";
    return 0;
}

void GraphGenerateTool::genRatableGraph(int64_t start, int64_t end) {
    CHECK(start <= end);
    auto client = getGraphClient();
    DCHECK(client.get() != nullptr);

    int64_t localSucceedVertices = 0L;
    int64_t localFailureVertices = 0L;
    int64_t localSucceedEdges = 0L;
    int64_t localFailureEdges = 0L;
    uint64_t localInsertVertexLatency = 0L;
    uint64_t localInsertEdgeLatency = 0L;
    int64_t batchEdges = FLAGS_batch_size * (FLAGS_edges / FLAGS_vertices);
    for (auto& tag : tags_) {
        std::vector<VertexID> vIds;
        vIds.reserve(FLAGS_batch_size);
        tagMapVertex_[tag] = std::move(vIds);
    }

    VertexID vid = 0;
    VertexID srcVid = 0;
    VertexID dstVid = 0;
    std::default_random_engine generator;
    int64_t localInterval = end - start;
    for (int64_t i = 0; i < localInterval / FLAGS_batch_size + 1; i++) {
        // default: rand 2/3 clearing the batch vertex pools
        for (auto& tmv : tagMapVertex_) {
            if (folly::Random::rand32(FLAGS_batch_cross)) {
                tmv.second.clear();
            }
        }
        for (int64_t j = 0; j < FLAGS_batch_size && j < localInterval; j++) {
            auto number = distributionTag_(generator);
            auto tag = descVertex_[number].first;
            vid = folly::Random::rand64(i * FLAGS_batch_size, (i + 1) * FLAGS_batch_size) + start;
            tagMapVertex_[tag].emplace_back(vid);
            auto query = genInsertVertexSentence(tag, vid);
            LOG(INFO) << query;
            cpp2::ExecutionResponse resp;
            auto code = client->execute(query, resp);
            if (cpp2::ErrorCode::SUCCEEDED == code) {
                localInsertVertexLatency += resp.get_latency_in_us();
                localSucceedVertices++;
            } else {
                LOG(ERROR) << "Do insert vertex: " << query << " failed";
                localFailureVertices++;
            }
        }

        for (int64_t j = 0; j < batchEdges; j++) {
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
            auto code = client->execute(query, resp);
            if (cpp2::ErrorCode::SUCCEEDED == code) {
                localInsertEdgeLatency += resp.get_latency_in_us();
                localSucceedEdges++;
            } else {
                LOG(ERROR) << "Do insert edge: " << query << " failed";
                localFailureEdges++;
            }
        }
    }
    for (auto& tmv : tagMapVertex_) {
        tmv.second.clear();
    }

    {
        std::lock_guard<std::mutex> g(statisticsLock_);
        totalSucceedVertices_ += localSucceedVertices;
        totalFailureVertices_ += localFailureVertices;
        totalSucceedEdges_ += localSucceedEdges;
        totalFailureEdges_ += localFailureEdges;
        totalInsertVertexLatency_ += localInsertVertexLatency;
        totalInsertEdgeLatency_ += localInsertEdgeLatency;
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

std::unique_ptr<graph::GraphClient> GraphGenerateTool::getGraphClient() {
    auto graphAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_graph_server_addrs);
    if (!graphAddrsRet.ok() || graphAddrsRet.value().empty()) {
        LOG(ERROR) << "Can't get graphServer address, status:" << graphAddrsRet.status()
                   << ", FLAGS_graph_server_addrs:" << FLAGS_graph_server_addrs;
        return nullptr;
    }
    auto hosts = graphAddrsRet.value();

    auto index = folly::Random::rand32(hosts.size());
    auto ipv4 = network::NetworkUtils::intToIPv4(hosts[index].first);
    auto port = hosts[index].second;
    auto client = std::make_unique<graph::GraphClient>(ipv4, port);
    if (cpp2::ErrorCode::SUCCEEDED != client->connect("user", "password")) {
        LOG(ERROR) << "Failed connect to " << ipv4 << ":" << port;
        return nullptr;
    }
    cpp2::ExecutionResponse resp;
    std::string cmd = "USE " + FLAGS_space_name;
    auto code = client->execute(cmd, resp);
    if (cpp2::ErrorCode::SUCCEEDED != code) {
       LOG(ERROR) << "Do cmd:" << cmd << " failed";
       return nullptr;
    }
    return client;
}

bool GraphGenerateTool::getTags() {
    auto client = getGraphClient();
    DCHECK(client.get() != nullptr);

    std::string query = "SHOW TAGS;";
    cpp2::ExecutionResponse resp;
    auto code = client->execute(query, resp);
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
    auto client = getGraphClient();
    DCHECK(client.get() != nullptr);

    std::string query = "SHOW EDGES;";
    cpp2::ExecutionResponse resp;
    auto code = client->execute(query, resp);
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
    auto client = getGraphClient();
    DCHECK(client.get() != nullptr);

    cpp2::ExecutionResponse resp;
    auto code = client->execute(query, resp);
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

