/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Configuration.h"
#include "client/cpp/GraphClient.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"
#include "storage/client/StorageClient.h"
#include <random>

namespace nebula {
namespace graph {

using StringSchema = std::vector<std::pair<std::string, std::string>>;
using Arc = std::tuple<std::string, std::string, std::string>;

class GraphGenerateTool {
public:
    GraphGenerateTool() = default;
    ~GraphGenerateTool() = default;

    int run();

private:
    void genRatableGraph(int64_t start, int64_t end);

    std::unique_ptr<graph::GraphClient> getGraphClient();

    bool init();

    bool getTags();

    bool getEdgeTypes();

    StringSchema getSchema(std::string query);

    std::string genRandomString();

    std::string genRandomData(const std::string type);

    std::string genInsertVertexSentence(const std::string tag,
                                        VertexID vid);

    std::string genInsertEdgeSentence(const std::string edgeType,
                                      VertexID srcVid,
                                      VertexID dstVid);

private:
    std::vector<std::string>                                    tags_;
    std::vector<std::string>                                    edges_;
    std::unordered_map<std::string, StringSchema>               tagSchema_;
    std::unordered_map<std::string, StringSchema>               edgeSchema_;

    int64_t                                                     minBatchSize_{0L};
    std::vector<std::pair<std::string, int64_t>>                descVertex_;
    std::discrete_distribution<int64_t>                         distributionTag_;
    std::vector<std::pair<Arc, int64_t>>                        descEdge_;
    std::discrete_distribution<int64_t>                         distributionArc_;
    std::unordered_map<std::string, std::vector<VertexID>>      tagMapVertex_;

    std::mutex                                                  statisticsLock_;
    int64_t                                                     totalSucceedVertices_{0L};
    int64_t                                                     totalFailureVertices_{0L};
    int64_t                                                     totalSucceedEdges_{0L};
    int64_t                                                     totalFailureEdges_{0L};
    uint64_t                                                    totalInsertVertexLatency_{0L};
    uint64_t                                                    totalInsertEdgeLatency_{0L};
};

}  // namespace graph
}  // namespace nebula
