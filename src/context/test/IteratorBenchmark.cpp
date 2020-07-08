/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include "context/Iterator.h"

// 40 edges
std::shared_ptr<nebula::Value> gDataSets1;
// 4000 edges
std::shared_ptr<nebula::Value> gDataSets2;

std::unique_ptr<nebula::graph::GetNeighborsIter> gGNIter;

namespace nebula {
namespace graph {
std::shared_ptr<nebula::Value> setUpIter(int64_t totalEdgeNum) {
    DataSet ds1;
    ds1.colNames = {kVid,
                    "_stats",
                    "_tag:tag1:prop1:prop2",
                    "_edge:+edge1:prop1:prop2:_dst:_rank",
                    "_expr"};
    for (auto i = 0; i < totalEdgeNum / 4; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));
        // _stats = empty
        row.values.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.values.emplace_back(Value(tag));
        // edges
        List edges;
        for (auto j = 0; j < 2; ++j) {
            List edge;
            for (auto k = 0; k < 2; ++k) {
                edge.values.emplace_back(k);
            }
            edge.values.emplace_back("2");
            edge.values.emplace_back(j);
            edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        // _expr = empty
        row.values.emplace_back(Value());
        ds1.rows.emplace_back(std::move(row));
    }

    DataSet ds2;
    ds2.colNames = {kVid,
                    "_stats",
                    "_tag:tag2:prop1:prop2",
                    "_edge:-edge2:prop1:prop2:_dst:_rank",
                    "_expr"};
    for (auto i = totalEdgeNum / 4; i < totalEdgeNum / 2; ++i) {
        Row row;
        // _vid
        row.values.emplace_back(folly::to<std::string>(i));
        // _stats = empty
        row.values.emplace_back(Value());
        // tag
        List tag;
        tag.values.emplace_back(0);
        tag.values.emplace_back(1);
        row.values.emplace_back(Value(tag));
        // edges
        List edges;
        for (auto j = 0; j < 2; ++j) {
            List edge;
            for (auto k = 0; k < 2; ++k) {
                edge.values.emplace_back(k);
            }
            edge.values.emplace_back("2");
            edge.values.emplace_back(j);
            edges.values.emplace_back(std::move(edge));
        }
        row.values.emplace_back(edges);
        // _expr = empty
        row.values.emplace_back(Value());
        ds2.rows.emplace_back(std::move(row));
    }

    List datasets;
    datasets.values.emplace_back(std::move(ds1));
    datasets.values.emplace_back(std::move(ds2));
    return std::make_shared<Value>(std::move(datasets));
}

size_t getNeighborsIterCtor(size_t iters, std::shared_ptr<nebula::Value> val) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        GetNeighborsIter iter(val);
        folly::doNotOptimizeAway(iter);
    }
    return iters * ops;
}

size_t getColumnForGetNeighborsIter(size_t iters) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        auto& val = gGNIter->getColumn(kVid);
        folly::doNotOptimizeAway(val);
    }
    return iters * ops;
}

size_t getTagProp(size_t iters) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        auto& val = gGNIter->getTagProp("tag1", "prop1");
        folly::doNotOptimizeAway(val);
    }
    return iters * ops;
}

size_t getTagProps(size_t iters) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        for (; gGNIter->valid(); gGNIter->next()) {
            auto& val = gGNIter->getTagProp("tag1", "prop1");
            folly::doNotOptimizeAway(val);
        }
        gGNIter->reset();
    }
    return iters * ops;
}

size_t getEdgeProp(size_t iters) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        auto& val = gGNIter->getEdgeProp("edge1", "prop1");
        folly::doNotOptimizeAway(val);
    }
    return iters * ops;
}

size_t getVertex(size_t iters) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        auto val = gGNIter->getVertex();
        folly::doNotOptimizeAway(val);
    }
    return iters * ops;
}

size_t getEdge(size_t iters) {
    constexpr size_t ops = 100000UL;
    for (size_t i = 0; i < iters * ops; ++i) {
        auto val = gGNIter->getEdge();
        folly::doNotOptimizeAway(val);
    }
    return iters * ops;
}

BENCHMARK_NAMED_PARAM_MULTI(getNeighborsIterCtor, get_neighbors_ctor_40_edges, gDataSets1)
BENCHMARK_NAMED_PARAM_MULTI(getNeighborsIterCtor, get_neighbors_ctor_4000_edges, gDataSets2)
BENCHMARK_NAMED_PARAM_MULTI(getColumnForGetNeighborsIter, get_column_1)
BENCHMARK_NAMED_PARAM_MULTI(getTagProp, get_tag_prop)
BENCHMARK_NAMED_PARAM_MULTI(getEdgeProp, get_edge_prop)
BENCHMARK_NAMED_PARAM_MULTI(getVertex, get_vertex)
BENCHMARK_NAMED_PARAM_MULTI(getEdge, get_edge)
BENCHMARK_NAMED_PARAM_MULTI(getTagProps, get_tag_4000)
}  // namespace graph
}  // namespace nebula


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    gDataSets1 = nebula::graph::setUpIter(40);
    gGNIter = std::make_unique<nebula::graph::GetNeighborsIter>(gDataSets1);
    gDataSets2 = nebula::graph::setUpIter(4000);
    folly::runBenchmarks();
    return 0;
}

/*
============================================================================
IteratorBenchmark.cpprelative                               time/iter  iters/s
============================================================================
getNeighborsIterCtor(get_neighbors_ctor_40_edge              7.44us  134.35K
getNeighborsIterCtor(get_neighbors_ctor_4000_ed            172.39us    5.80K
getColumnForGetNeighborsIter(get_column_1)                  34.17ns   29.27M
getTagProp(get_tag_prop)                                    71.12ns   14.06M
getEdgeProp(get_edge_prop)                                  74.98ns   13.34M
getVertex(get_vertex)                                      329.12ns    3.04M
getEdge(get_edge)                                          770.82ns    1.30M
getTagProps(get_tag_4000)                                    2.29us  437.26K
============================================================================
*/
