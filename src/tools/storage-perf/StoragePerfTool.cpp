/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "thread/GenericWorker.h"
#include "time/Duration.h"
#include "storage/client/StorageClient.h"

DEFINE_int32(threads, 2, "Total threads for perf");
DEFINE_int32(qps, 1000, "Total qps for the perf tool");
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

namespace nebula {
namespace storage {

thread_local uint32_t position = 1;

class Perf {
public:
    int run() {
        uint32_t qpsPerThread = FLAGS_qps / FLAGS_threads;
        int32_t radix = qpsPerThread / 1000;
        int32_t slotSize = sizeof(slots_) / sizeof(int32_t);
        std::fill(slots_, slots_ + slotSize, radix);

        int32_t remained = qpsPerThread % 1000 / 100;
        if (remained != 0) {
            int32_t step = slotSize / remained - 1;
            if (step == 0) {
                step = 1;
            }
            for (int32_t i = 0; i < remained; i++) {
                int32_t p = (i + i * step) % slotSize;
                if (slots_[p] == radix) {
                    slots_[p] += 1;
                } else {
                    while (slots_[++p] == (radix + 1)) {}
                    slots_[p] += 1;
                }
            }
        }

        std::stringstream stream;
        for (int32_t slot : slots_) {
            stream << slot << " ";
        }
        LOG(INFO) << "Total threads " << FLAGS_threads
                  << ", qpsPerThread " << qpsPerThread
                  << ", slots " << stream.str();
        auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
        if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
            LOG(ERROR) << "Can't get metaServer address, status:" << metaAddrsRet.status()
                       << ", FLAGS_meta_server_addrs:" << FLAGS_meta_server_addrs;
            return EXIT_FAILURE;
        }

        std::vector<std::unique_ptr<thread::GenericWorker>> threads;
        for (int32_t i = 0; i < FLAGS_threads; i++) {
            auto t = std::make_unique<thread::GenericWorker>();
            threads.emplace_back(std::move(t));
        }
        threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);
        mClient_ = std::make_unique<meta::MetaClient>(threadPool_, metaAddrsRet.value());
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
        static uint32_t interval = 1;
        for (auto& t : threads) {
            CHECK(t->start("TaskThread"));
            if (FLAGS_method == "getNeighbors") {
                t->addRepeatTask(interval, &Perf::getNeighborsTask, this);
            } else if (FLAGS_method == "addVertices") {
                t->addRepeatTask(interval, &Perf::addVerticesTask, this);
            } else if (FLAGS_method == "addEdges") {
                t->addRepeatTask(interval, &Perf::addEdgesTask, this);
            } else {
                t->addRepeatTask(interval, &Perf::getVerticesTask, this);
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
            std::stringstream stream;
            for (int32_t index = 0; index < size;) {
                const char *array = folly::to<std::string>(folly::Random::rand32(128)).c_str();
                int32_t length = sizeof(array) / sizeof(char);
                for (int32_t i = 0; i < length; i++) {
                    stream << array[i];
                    index++;
                }
            }
            return stream.str();
        } else {
            return std::string(size, ' ');
        }
    }

    std::vector<storage::cpp2::Vertex> genVertices() {
        static int32_t size = sizeof(slots_) / sizeof(int32_t);
        std::vector<storage::cpp2::Vertex> vertices;
        static VertexID vId = FLAGS_min_vertex_id;
        position = (position + 1) % size;
        for (int32_t i = 0; i < slots_[position]; i++) {
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
        static int32_t size = sizeof(slots_) / sizeof(int32_t);
        std::vector<storage::cpp2::Edge> edges;
        static VertexID vId = FLAGS_min_vertex_id;
        position = (position + 1) % size;
        for (int32_t i = 0; i< slots_[position]; i++) {
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
        }
        return edges;
    }

    void getNeighborsTask() {
        auto* evb = threadPool_->getEventBase();
        std::vector<EdgeType> e(edgeType_);
        auto f =
          client_->getNeighbors(spaceId_, randomVertices(), std::move(e), "", randomCols())
                .via(evb)
                .thenValue([this](auto&& resps) {
                    if (!resps.succeeded()) {
                        LOG(ERROR) << "Request failed!";
                    } else {
                        VLOG(3) << "request successed!";
                    }
                    this->finishedRequests_++;
                    VLOG(3) << "request successed!";
                })
                .thenError([](auto&&) { LOG(ERROR) << "request failed!"; });
    }

    void addVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->addVertices(spaceId_, genVertices(), true)
                    .via(evb).thenValue([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            for (auto& entry : resps.failedParts()) {
                                LOG(ERROR) << "Request failed, part " << entry.first
                                           << ", error " << static_cast<int32_t>(entry.second);
                            }
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                     }).thenError([](auto&&) {
//                        LOG(ERROR) << "Request failed, e = " << e.what();
                     });
    }

    void addEdgesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->addEdges(spaceId_, genEdges(), true)
                    .via(evb).thenValue([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).thenError([](auto&&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

    void getVerticesTask() {
        auto* evb = threadPool_->getEventBase();
        auto f = client_->getVertexProps(spaceId_, randomVertices(), randomCols())
                    .via(evb).thenValue([this](auto&& resps) {
                        if (!resps.succeeded()) {
                            LOG(ERROR) << "Request failed!";
                        } else {
                            VLOG(3) << "request successed!";
                        }
                        this->finishedRequests_++;
                        VLOG(3) << "request successed!";
                     }).thenError([](auto&&) {
                        LOG(ERROR) << "Request failed!";
                     });
    }

private:
    std::atomic_long finishedRequests_{0};
    std::unique_ptr<StorageClient> client_;
    std::unique_ptr<meta::MetaClient> mClient_;
    std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
    GraphSpaceID spaceId_;
    TagID tagId_;
    EdgeType edgeType_;
    int32_t slots_[10];
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::Perf perf;
    return perf.run();
}
