/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GRAPHSTATS_H
#define GRAPH_GRAPHSTATS_H

#include "stats/StatsManager.h"
#include "stats/Stats.h"

namespace nebula {
namespace graph {

class GraphStats final {
public:
    GraphStats() {
        storageClientStats_ = std::make_unique<stats::Stats>("graph", "storageCliet");
        metaClientStats_ = std::make_unique<stats::Stats>("graph", "metaClient");
        graphAllStats_ = std::make_unique<stats::Stats>("graph", "graph_all");
        parseErrorStats_ = std::make_unique<stats::Stats>("graph", "parse_error");
        insertVStats_ = std::make_unique<stats::Stats>("graph", "insert_vertex");
        insertEStats_ = std::make_unique<stats::Stats>("graph", "insert_edge");
        deleteVStats_ = std::make_unique<stats::Stats>("graph", "delete_vertex");
        deleteEStats_ = std::make_unique<stats::Stats>("graph", "delete_edge");
        updateVStats_ = std::make_unique<stats::Stats>("graph", "update_vertex");
        updateEStats_ = std::make_unique<stats::Stats>("graph", "update_edge");
        goStats_ = std::make_unique<stats::Stats>("graph", "go");
        findPathStats_ = std::make_unique<stats::Stats>("graph", "find_path");
        fetchVStats_ = std::make_unique<stats::Stats>("graph", "fetch_vertex");
        fetchEStats_ = std::make_unique<stats::Stats>("graph", "fetch_edge");
    }

    ~GraphStats() = default;

public:
    stats::Stats* getStorageClientStats() const { return storageClientStats_.get(); }
    stats::Stats* getMetaClientStats() const { return metaClientStats_.get(); }
    stats::Stats* getGraphAllStats() const { return graphAllStats_.get(); }
    stats::Stats* getParseErrorStats() const { return parseErrorStats_.get(); }
    stats::Stats* getInsertVertexStats() const { return insertVStats_.get(); }
    stats::Stats* getInsertEdgeStats() const { return insertEStats_.get(); }
    stats::Stats* getDeleteVertexStats() const { return deleteVStats_.get(); }
    stats::Stats* getDeleteEdgeStats() const { return deleteEStats_.get(); }
    stats::Stats* getUpdateVertexStats() const { return updateVStats_.get(); }
    stats::Stats* getUpdateEdgeStats() const { return updateEStats_.get(); }
    stats::Stats* getGoStats() const { return goStats_.get(); }
    stats::Stats* getFindPathStats() const { return findPathStats_.get(); }
    stats::Stats* getFetchVerticesStats() const { return fetchVStats_.get(); }
    stats::Stats* getFetchEdgesStats() const { return fetchEStats_.get(); }

private:
    std::unique_ptr<stats::Stats> storageClientStats_;         // storageClient stats
    std::unique_ptr<stats::Stats> metaClientStats_;            // metaClient stats
    std::unique_ptr<stats::Stats> graphAllStats_;              // graph stats
    std::unique_ptr<stats::Stats> parseErrorStats_;            // graph parse error stats
    std::unique_ptr<stats::Stats> insertVStats_;               // insert vertexes stats
    std::unique_ptr<stats::Stats> insertEStats_;               // insert edges stats
    std::unique_ptr<stats::Stats> deleteVStats_;               // delete vertexes stats
    std::unique_ptr<stats::Stats> deleteEStats_;               // delete edges stats
    std::unique_ptr<stats::Stats> updateVStats_;               // update vertex stats
    std::unique_ptr<stats::Stats> updateEStats_;               // update edge stats
    std::unique_ptr<stats::Stats> goStats_;                    // go stats
    std::unique_ptr<stats::Stats> findPathStats_;              // findPath stats
    std::unique_ptr<stats::Stats> fetchVStats_;                // fetch vertex stats
    std::unique_ptr<stats::Stats> fetchEStats_;                // fetch edge stats
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_GRAPHSTATS_H
