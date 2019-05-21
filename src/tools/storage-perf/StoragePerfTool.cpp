/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "thread/GenericWorker.h"
#include "time/Duration.h"
#include "storage/client/StorageClient.h"
#include "dataman/RowWriter.h"

DEFINE_int32(threads, 2, "Total threads for perf");
DEFINE_int32(qps, 1000, "Total qps for the perf tool");
DEFINE_int32(totalReqs, 10000, "Total requests during this perf test");
DEFINE_int32(io_threads, 3, "Client io threads");
DEFINE_bool(mock_server, true, "Test server was started in mock server mode");
DEFINE_int32(mock_server_port, 44500, "The mock server port");
DEFINE_string(method, "getNeighbors", "method type being tested,"
                                      "such as getNeighbors, addVertices, addEdges, getVertices");

DEFINE_int32(min_vertex_id, 1, "The smallest vertex Id");
DEFINE_int32(max_vertex_id, 10000, "The biggest vertex Id");

namespace nebula {
namespace storage {

class Perf {
public:
    int run() {
        uint32_t qpsPerThread = FLAGS_qps / FLAGS_threads;
        uint32_t interval = 1000 / qpsPerThread;
        CHECK_GT(interval, 0) << "qpsPerThread should not large than 1000, interval " << interval;
        LOG(INFO) << "Total threads " << FLAGS_threads
                  << ", qpsPerThread " << qpsPerThread
                  << ", task interval ms " << interval;
        std::vector<std::unique_ptr<thread::GenericWorker>> threads;
        for (auto i = 0; i < FLAGS_threads; i++) {
            auto t = std::make_unique<thread::GenericWorker>();
            threads.emplace_back(std::move(t));
        }
        threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);
        client_ = std::make_unique<StorageClient>(threadPool_);
        time::Duration duration;
        for (auto& t : threads) {
            CHECK(t->start("TaskThread"));
            if (FLAGS_method == "getNeighbors") {
                t->addRepeatTask(interval, &Perf::getNeighborsTask, this);
            } else if (FLAGS_method == "addVertices") {
                t->addRepeatTask(interval, &Perf::addVerticesTask, this);
            } else if (FLAGS_method == "addEdges") {
                t->addRepeatTask(interval, &Perf::addEdgesTask, this);
            } else if (FLAGS_method == "getVertices") {
                t->addRepeatTask(interval, &Perf::getVerticesTask, this);
            } else {
                LOG(FATAL) << "Unknown method name " << FLAGS_method;
            }
        }
        while (finishedRequests_ < FLAGS_totalReqs) {
            if (finishedRequests_ % 1000 == 0) {
                LOG(INFO) << "Total " << FLAGS_totalReqs << ", finished " << finishedRequests_;
            }
            usleep(1000 * 30);
        }
        for (auto& t : threads) {
            t->stop();
        }
        for (auto& t : threads) {
            t->wait();
        }
        LOG(INFO) << "Total time cost " << duration.elapsedInMSec() << "ms, "
                  << "total requests " << finishedRequests_;
        return 0;
    }

private:
    std::vector<VertexID> randomVertices() {
        return {FLAGS_min_vertex_id
                + folly::Random::rand32(FLAGS_max_vertex_id - FLAGS_min_vertex_id)};
    }

    std::vector<storage::cpp2::PropDef> randomCols() {
        std::vector<storage::cpp2::PropDef> props;
        {
            storage::cpp2::PropDef prop;
            prop.set_name(folly::stringPrintf("tag_%d_col_1", defaultTagId_));
            prop.set_owner(storage::cpp2::PropOwner::SOURCE);
            prop.set_tag_id(defaultTagId_);
            props.emplace_back(std::move(prop));
        }
        {
            storage::cpp2::PropDef prop;
            prop.set_name("col_1");
            prop.set_owner(storage::cpp2::PropOwner::EDGE);
            props.emplace_back(std::move(prop));
        }
        return props;
    }

    std::vector<storage::cpp2::Vertex> genVertices() {
        std::vector<storage::cpp2::Vertex> vertices;
        static VertexID vId = FLAGS_min_vertex_id;
        storage::cpp2::Vertex v;
        v.set_id(vId++);
        decltype(v.tags) tags;
        storage::cpp2::Tag tag;
        tag.set_tag_id(defaultTagId_);
        std::vector<std::string> names;
        std::vector<cpp2::PropValue> values;
        values.resize(6);
        for (auto i = 0; i < 3; i++) {
            names.emplace_back(folly::stringPrintf("col_%d", i));
            values[i].set_int_val(i);
        }
        for (auto i = 3; i < 6; i++) {
            names.emplace_back(folly::stringPrintf("col_%d", i));
            values[i].set_string_val(folly::stringPrintf("tag_string_col_%d", i));
        }
        tag.set_props_name(std::move(names));
        tag.set_props_value(std::move(values));
        tags.emplace_back(std::move(tag));
        v.set_tags(std::move(tags));
        vertices.emplace_back(std::move(v));
        return vertices;
    }

    std::vector<storage::cpp2::Edge> genEdges() {
        std::vector<storage::cpp2::Edge> edges;
        static VertexID vId = FLAGS_min_vertex_id;
        storage::cpp2::Edge edge;
        storage::cpp2::EdgeKey eKey;
        eKey.set_src(vId);
        eKey.set_edge_type(defaultEdgeType_);
        eKey.set_dst(vId + 1);
        eKey.set_ranking(0);
        edge.set_key(std::move(eKey));
        std::vector<std::string> names;
        std::vector<cpp2::PropValue> values;
        values.resize(6);
        for (auto i = 0; i < 10; i++) {
            names.emplace_back(folly::stringPrintf("col_%d", i));
            values[i].set_int_val(i);
        }
        for (auto i = 10; i < 20; i++) {
            names.emplace_back(folly::stringPrintf("col_%d", i));
            values[i].set_string_val(folly::stringPrintf("tag_string_col_%d", i));
        }
        edge.set_props_name(std::move(names));
        edge.set_props_value(std::move(values));
        edges.emplace_back(std::move(edge));
        return edges;
    }

    void getNeighborsTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->getNeighbors(defaultSpaceId_, randomVertices(),
                                       defaultEdgeType_, true, "", randomCols())
                            .via(evb).then([this]() {
                                this->finishedRequests_++;
                                VLOG(3) << "request successed!";
                             }).onError([](folly::FutureException&) {
                                LOG(ERROR) << "request failed!";
                             });
    }

    void addVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->addVertices(defaultSpaceId_, genVertices(), true)
                    .via(evb).then([this]() {
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).onError([](folly::FutureException&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

    void addEdgesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->addEdges(defaultSpaceId_, genEdges(), true)
                    .via(evb).then([this]() {
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).onError([](folly::FutureException&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

    void getVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->getVertexProps(defaultSpaceId_, randomVertices(), randomCols())
                    .via(evb).then([this]() {
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).onError([](folly::FutureException&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

private:
    std::atomic_long finishedRequests_{0};
    std::unique_ptr<StorageClient> client_;
    std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
    GraphSpaceID defaultSpaceId_ = 0;
    EdgeType     defaultEdgeType_ = 101;
    TagID        defaultTagId_    = 3001;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::Perf perf;
    return perf.run();
}

