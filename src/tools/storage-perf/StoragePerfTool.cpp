/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "thread/GenericWorker.h"
#include "time/Duration.h"
#include "storage/client/StorageClient.h"
#include <folly/TokenBucket.h>
#include <folly/stats/TimeseriesHistogram.h>
#include <folly/stats/BucketedTimeSeries.h>

DEFINE_int32(threads, 1, "Total threads for perf");
DEFINE_double(qps, 1000, "Total qps for the perf tool");
DEFINE_int32(totalReqs, 10000, "Total requests during this perf test");
DEFINE_int32(io_threads, 10, "Client io threads");
DEFINE_string(method, "getNeighbors", "method type being tested,"
                                      "such as getNeighbors, addVertices, addEdges, getVertices");
DEFINE_string(meta_server_addrs, "", "meta server address");
DEFINE_int32(min_vertex_id, 1, "The smallest vertex Id");
DEFINE_int32(max_vertex_id, 10000, "The biggest vertex Id");
DEFINE_int32(size, 1000, "The data's size per request");
DEFINE_string(space_name, "test", "Specify the space name");
DEFINE_string(tag_name, "test_tag", "Specify the tag name");
DEFINE_string(edge_name, "test_edge", "Specify the edge name");
DEFINE_bool(random_message, false, "Whether to write random message to storage service");
DEFINE_int32(concurrency, 50, "concurrent requests");
DEFINE_int32(batch_num, 1, "batch vertices for one request");


namespace nebula {
namespace storage {

thread_local uint32_t position = 1;

class Perf {
public:
    Perf()
        : latencies_(10, 0, 3000, {10, {std::chrono::seconds(20)}})
        , qps_(10, 0, 1000000, {10, {std::chrono::seconds(20)}}) {}

    int run() {
        LOG(INFO) << "Total threads " << FLAGS_threads << ", qps " << FLAGS_qps;
        auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
        if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
            LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
                       << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
            return EXIT_FAILURE;
        }

        threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);
        meta::MetaClientOptions options;
        options.skipConfig_ = true;
        mClient_ = std::make_unique<meta::MetaClient>(threadPool_, metaAddrsRet.value(), options);
        CHECK(mClient_->waitForMetadReady());

        auto spaceResult = mClient_->getSpaceIdByNameFromCache(FLAGS_space_name);
        if (!spaceResult.ok()) {
            LOG(ERROR) << "Get SpaceID Failed: " << spaceResult.status();
            return EXIT_FAILURE;
        }
        spaceId_ = spaceResult.value();

        auto tagResult = mClient_->getTagIDByNameFromCache(spaceId_, FLAGS_tag_name);
        if (!tagResult.ok()) {
            LOG(ERROR) << "TagID not exist: " << tagResult.status();
            return EXIT_FAILURE;
        }
        tagId_ = tagResult.value();

        auto edgeResult = mClient_->getEdgeTypeByNameFromCache(spaceId_, FLAGS_edge_name);
        if (!edgeResult.ok()) {
            LOG(ERROR) << "EdgeType not exist: " << edgeResult.status();
            return EXIT_FAILURE;
        }
        edgeType_ = edgeResult.value();

        client_ = std::make_unique<StorageClient>(threadPool_, mClient_.get());
        time::Duration duration;

        std::vector<std::thread> threads;
        threads.reserve(FLAGS_threads);
        for (int i = 0; i < FLAGS_threads; i++) {
            threads.emplace_back(std::bind(&Perf::runInternal, this));
        }
        for (auto& t : threads) {
            t.join();
        }
        threadPool_->stop();
        LOG(INFO) << "Total time cost " << duration.elapsedInMSec() << "ms, "
                  << "total requests " << finishedRequests_;
        return 0;
    }


    void runInternal() {
        while (finishedRequests_ < FLAGS_totalReqs) {
            if (FLAGS_method == "getNeighbors") {
                getNeighborsTask();
            } else if (FLAGS_method == "addVertices") {
                addVerticesTask();
            } else if (FLAGS_method == "addEdges") {
                addEdgesTask();
            } else {
                getVerticesTask();
            }
            PLOG_EVERY_N(INFO, 2000)
                << "Progress "
                << finishedRequests_/static_cast<double>(FLAGS_totalReqs) * 100 << "%"
                << ", qps=" << qps_.rate(0)
                << ", latency(us) median = " << latencies_.getPercentileEstimate(0.5, 0)
                << ", p90 = " << latencies_.getPercentileEstimate(0.9, 0)
                << ", p99 = " << latencies_.getPercentileEstimate(0.99, 0);
            usleep(500);
        }
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
            prop.set_name(folly::stringPrintf("tag_%d_col_1", tagId_));
            prop.set_owner(storage::cpp2::PropOwner::SOURCE);
            prop.id.set_tag_id(tagId_);
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

    std::string genData(int32_t size) {
        if (FLAGS_random_message) {
            auto randchar = []() -> char {
                const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
                const size_t maxIndex = (sizeof(charset) - 1);
                return charset[folly::Random::rand32(maxIndex)];
            };
            std::string str(size, 0);
            std::generate_n(str.begin(), size, randchar);
            return str;
        } else {
            return std::string(size, ' ');
        }
    }

    std::vector<storage::cpp2::Vertex> genVertices() {
        std::vector<storage::cpp2::Vertex> vertices;
        static VertexID vId = FLAGS_min_vertex_id;
        for (int i = 0; i < FLAGS_batch_num; i++) {
            storage::cpp2::Vertex v;
            v.set_id(vId++);
            decltype(v.tags) tags;
            storage::cpp2::Tag tag;
            tag.set_tag_id(tagId_);
            auto props = genData(FLAGS_size);
            tag.set_props(std::move(props));
            tags.emplace_back(std::move(tag));
            v.set_tags(std::move(tags));
            vertices.emplace_back(std::move(v));
        }
        return vertices;
    }

    std::vector<storage::cpp2::Edge> genEdges() {
        std::vector<storage::cpp2::Edge> edges;
        static VertexID vId = FLAGS_min_vertex_id;
        storage::cpp2::Edge edge;
        storage::cpp2::EdgeKey eKey;
        eKey.set_src(vId);
        eKey.set_edge_type(edgeType_);
        eKey.set_dst(vId + 1);
        eKey.set_ranking(0);
        edge.set_key(std::move(eKey));
        auto props = genData(FLAGS_size);
        edge.set_props(std::move(props));
        edges.emplace_back(std::move(edge));
        vId++;
        return edges;
    }

    void getNeighborsTask() {
        auto* evb = threadPool_->getEventBase();
        auto tokens = tokenBucket_.consumeOrDrain(FLAGS_concurrency, FLAGS_qps, FLAGS_concurrency);
        for (auto i = 0; i < tokens; i++) {
            auto start = time::WallClock::fastNowInMicroSec();
            client_->getNeighbors(spaceId_, randomVertices(), {edgeType_}, "", randomCols())
                .via(evb)
                .thenValue([this, start](auto&& resps) {
                    if (!resps.succeeded()) {
                        LOG(ERROR) << "Request failed!";
                    } else {
                        VLOG(3) << "request successed!";
                    }
                    this->finishedRequests_++;
                    auto now = time::WallClock::fastNowInMicroSec();
                    latencies_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()),
                                        now - start);
                    qps_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()), 1);
                })
                .thenError([this](auto&& e) {
                    LOG(ERROR) << "request failed, e = " << e.what();
                    this->finishedRequests_++;
                });
        }
    }

    void addVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto tokens = tokenBucket_.consumeOrDrain(FLAGS_concurrency, FLAGS_qps, FLAGS_concurrency);
        for (auto i = 0; i < tokens; i++) {
            auto vertices = genVertices();
            auto start = time::WallClock::fastNowInMicroSec();
            client_->addVertices(spaceId_, std::move(vertices), true)
                    .via(evb).thenValue([this, start](auto&& resps) {
                        if (!resps.succeeded()) {
                            for (auto& entry : resps.failedParts()) {
                                LOG(ERROR) << "Request failed, part " << entry.first
                                           << ", error " << static_cast<int32_t>(entry.second);
                            }
                        } else {
                            VLOG(1) << "request successed!";
                        }
                        this->finishedRequests_++;
                        auto now = time::WallClock::fastNowInMicroSec();
                        latencies_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()),
                                            now - start);
                        qps_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()), 1);
                     }).thenError([this](auto&& e) {
                        LOG(ERROR) << "Request failed, e = " << e.what();
                        this->finishedRequests_++;
                     });
        }
    }

    void addEdgesTask() {
        auto* evb = threadPool_->getEventBase();
        auto tokens = tokenBucket_.consumeOrDrain(FLAGS_concurrency, FLAGS_qps, FLAGS_concurrency);
        for (auto i = 0; i < tokens; i++) {
            auto start = time::WallClock::fastNowInMicroSec();
            client_->addEdges(spaceId_, genEdges(), true)
                    .via(evb).thenValue([this, start](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        auto now = time::WallClock::fastNowInMicroSec();
                        latencies_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()),
                                            now - start);
                        qps_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()), 1);
                     }).thenError([this](auto&&) {
                        LOG(ERROR) << "Request failed!";
                        this->finishedRequests_++;
                     });
        }
    }

    void getVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto tokens = tokenBucket_.consumeOrDrain(FLAGS_concurrency, FLAGS_qps, FLAGS_concurrency);
        for (auto i = 0; i < tokens; i++) {
            auto start = time::WallClock::fastNowInMicroSec();
            client_->getVertexProps(spaceId_, randomVertices(), randomCols())
                    .via(evb).thenValue([this, start](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        auto now = time::WallClock::fastNowInMicroSec();
                        latencies_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()),
                                            now - start);
                        qps_.addValue(std::chrono::seconds(time::WallClock::fastNowInSec()), 1);
                     }).thenError([this](auto&&) {
                        this->finishedRequests_++;
                        LOG(ERROR) << "Request failed!";
                     });
        }
    }

private:
    std::atomic_long finishedRequests_{0};
    std::unique_ptr<StorageClient> client_;
    std::unique_ptr<meta::MetaClient> mClient_;
    std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
    GraphSpaceID spaceId_;
    TagID tagId_;
    EdgeType edgeType_;
    folly::DynamicTokenBucket tokenBucket_;
    folly::TimeseriesHistogram<int64_t> latencies_;
    folly::TimeseriesHistogram<int64_t> qps_;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::Perf perf;
    return perf.run();
}
