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
        storageClientStats_ = std::make_unique<stats::Stats>("storageCliet");
        metaClientStats_ = std::make_unique<stats::Stats>("metaClient");
        graphStats_ = std::make_unique<stats::Stats>("graph");
        insertVStats_ = std::make_unique<stats::Stats>("insertVertex");
        insertEStats_ = std::make_unique<stats::Stats>("insertEdge");
        goStats_ = std::make_unique<stats::Stats>("query");
    }

    ~GraphStats() = default;

public:
    stats::Stats* getStorageClientStats() { return storageClientStats_.get(); }
    stats::Stats* getMetaClientStats() { return metaClientStats_.get(); }
    stats::Stats* getGraphStats() { return graphStats_.get(); }
    stats::Stats* getInsertVertexStats() { return insertVStats_.get(); }
    stats::Stats* getInsertEdgeStats() { return insertEStats_.get(); }
    stats::Stats* getGoStats() { return goStats_.get(); }

private:
    std::unique_ptr<stats::Stats> storageClientStats_;         // storageClient stats
    std::unique_ptr<stats::Stats> metaClientStats_;            // metaClient stats
    std::unique_ptr<stats::Stats> graphStats_;                 // graphClient stats
    std::unique_ptr<stats::Stats> insertVStats_;               // insert vertexes stats
    std::unique_ptr<stats::Stats> insertEStats_;               // insert edges stats
    std::unique_ptr<stats::Stats> goStats_;                    // go stats
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_GRAPHSTATS_H
