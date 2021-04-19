/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/time/Duration.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "codec/RowReader.h"
#include <thrift/lib/cpp/util/EnumUtils.h>


DEFINE_string(meta_server_addrs, "", "meta server address");
DEFINE_int32(io_threads, 10, "client io threads");

DEFINE_int32(partition_num, 1024, "partititon for space");
DEFINE_string(space_name, "test_space", "the space name");
DEFINE_string(tag_name, "test_tag", "the tag name");
DEFINE_string(prop_name, "test_prop", "the property name");

DEFINE_string(first_vertex_id, "1", "The smallest vertex id");
DEFINE_uint64(width, 100, "width of matrix");
DEFINE_uint64(height, 1000, "height of matrix");

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

/**
 * We generate a big circle of data, all node is the vertex, and the vertex have only one
 * property of the next vertex, so we can validate them by traversing.
 *
 * There are some gflags we need to pay attention:
 * 1. The space's replica must be 1, because we don't have retry in StorageClient, we will
 *    update it after we suppport preheat. The tag must have only one int property,
 *    which is prop_name.
 * 2. If the space and tag doesn't exists, it will try to create one, maybe you need to set
 *    heartbeat_interval_secs to make sure the storage service has load meta.
 * 3. The width and height is the size of the big linked list, you can refer to the graph below.
 *    As expected, we can traverse the big linked list after width * height steps starting
 *    from any node in the list.
 */
class IntegrityTest {
public:
    IntegrityTest()
        : propName_(FLAGS_prop_name)
        , width_{FLAGS_width}
        , height_{FLAGS_height}
        , firstVertexId_{FLAGS_first_vertex_id} {}

    int run() {
        if (!init()) {
            return EXIT_FAILURE;
        }
        prepareData();
        if (!validate(firstVertexId_, width_ * height_)) {
            LOG(INFO) << "Integrity test failed";
            return EXIT_FAILURE;
        }
        LOG(INFO) << "Integrity test passed";
        return 0;
    }

private:
    bool init() {
        FLAGS_heartbeat_interval_secs = 10;
        auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
        if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
            LOG(ERROR) << "Can't get metaServer address, status: " << metaAddrsRet.status()
                       << ", FLAGS_meta_server_addrs: " << FLAGS_meta_server_addrs;
            return false;
        }
        threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);

        meta::MetaClientOptions options;
        options.skipConfig_ = true;
        mClient_ = std::make_unique<meta::MetaClient>(threadPool_,
                                                      metaAddrsRet.value(),
                                                      options);
        if (!mClient_->waitForMetadReady()) {
            LOG(ERROR) << "Init meta client failed";
            return false;
        }

        auto spaceResult = mClient_->getSpaceIdByNameFromCache(FLAGS_space_name);
        if (!spaceResult.ok()) {
            LOG(ERROR) << "Get spaceId failed, try to create one";
            meta::cpp2::SpaceDesc spaceDesc;
            spaceDesc.set_space_name(FLAGS_space_name);
            spaceDesc.set_partition_num(FLAGS_partition_num);
            spaceDesc.set_replica_factor(1);
            auto ret = mClient_->createSpace(spaceDesc).get();
            if (!ret.ok()) {
                LOG(ERROR) << "Create space failed: " << ret.status();
                return false;
            }
            spaceId_ = ret.value();
        } else {
            spaceId_ = spaceResult.value();
        }

        auto tagResult = mClient_->getTagIDByNameFromCache(spaceId_, FLAGS_tag_name);
        if (!tagResult.ok()) {
            sleep(FLAGS_heartbeat_interval_secs + 1);
            LOG(ERROR) << "Get tagId failed, try to create one: " << tagResult.status();
            nebula::meta::cpp2::Schema schema;
            nebula::meta::cpp2::ColumnDef column;
            column.name = FLAGS_prop_name;
            column.type.set_type(meta::cpp2::PropertyType::INT64);
            (*schema.columns_ref()).emplace_back(std::move(column));
            auto ret = mClient_->createTagSchema(spaceId_, FLAGS_tag_name, schema).get();
            if (!ret.ok()) {
                LOG(ERROR) << "Create tag failed: " << ret.status();
                return false;
            }
            tagId_ = ret.value();
        } else {
            tagId_ = tagResult.value();
        }

        client_ = std::make_unique<GraphStorageClient>(threadPool_, mClient_.get());
        return true;
    }

    /**
     * [ . . . ] represents one batch of random longs of length WIDTH
     *                _________________________
     *               |                  ______ |
     *               |                 |      ||
     *             .-+-----------------+-----.||
     *             | |                 |     |||
     * first   = [ . . . . . . . . . . . ]   |||
     *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
     *             | | | | | | | | | | |     |||
     * prev    = [ . . . . . . . . . . . ]   |||
     *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^     |||
     *             | | | | | | | | | | |     |||
     * current = [ . . . . . . . . . . . ]   |||
     *                                       |||
     * ...                                   |||
     *                                       |||
     * last    = [ . . . . . . . . . . . ]   |||
     *             ^ ^ ^ ^ ^ ^ ^ ^ ^ ^ ^_____|||
     *             |                 |________||
     *             |___________________________|
     */
    void prepareData() {
        std::vector<VertexID> first;
        std::vector<VertexID> prev;
        std::vector<VertexID> cur;

        LOG(INFO) << "Start insert vertex";
        for (size_t i = 0; i < width_; i++) {
            prev.emplace_back(std::to_string(std::atol(firstVertexId_.c_str()) + i));
        }
        // leave alone the first line, generate other lines
        for (size_t i = 1; i < height_; i++) {
            addVertex(prev, cur, std::to_string(std::atol(firstVertexId_.c_str() + i * width_)));
            prev = std::move(cur);
        }
        // shift the last line
        std::rotate(prev.begin(), prev.end() - 1, prev.end());
        // generate first line, each node in first line will points to a node in rotated last line,
        // which will make the matrix a big linked list
        addVertex(prev, first, firstVertexId_);
        LOG(INFO) << "Prepare data ok";
    }

    void addVertex(std::vector<VertexID>& prev, std::vector<VertexID>& cur, VertexID startId) {
        std::unordered_map<TagID, std::vector<std::string>> propNames;
        propNames[tagId_].emplace_back(propName_);
        auto future =
            client_->addVertices(spaceId_, genVertices(prev, cur, startId), propNames, true);
        auto resp = std::move(future).get();
        if (!resp.succeeded()) {
            for (auto& err : resp.failedParts()) {
                VLOG(2) << "Partition " << err.first
                        << " failed: " << apache::thrift::util::enumNameSafe(err.second);
            }
        }
    }

    std::vector<storage::cpp2::NewVertex> genVertices(std::vector<VertexID>& prev,
                                                   std::vector<VertexID>& cur,
                                                   VertexID startId) {
        // We insert add vertices of a row once a time
        std::vector<storage::cpp2::NewVertex> newVertices;
        for (size_t i = 0; i < width_; i++) {
            VertexID vId;
            vId = std::to_string(std::atol(startId.c_str()) + i);
            cur.emplace_back(vId);

            storage::cpp2::NewVertex v;
            v.set_id(vId);
            std::vector<nebula::storage::cpp2::NewTag> tags;

            storage::cpp2::NewTag tag;
            tag.set_tag_id(tagId_);

            std::vector<nebula::Value> props;
            Value val(prev[i]);
            props.emplace_back(val);
            tag.set_props(props);
            tags.emplace_back(std::move(tag));

            v.set_tags(std::move(tags));
            newVertices.emplace_back(std::move(v));
            VLOG(2) << "Build " << cur[i] << " -> " << prev[i];
            PLOG_EVERY_N(INFO, 10000) << "We have inserted "
                << std::atol(vId.c_str()) - std::atol(firstVertexId_.c_str()) - width_
                << " vertices so far, total: " << width_ * height_;
        }
        return newVertices;
    }

    bool validate(VertexID startId, int64_t queryTimes) {
        int64_t count = 0;
        VertexID nextId = startId;
        while (count < queryTimes) {
            PLOG_EVERY_N(INFO, 1000) << "We have gone " << count << " steps so far";
            // TODO support getProps
            std::vector<cpp2::VertexProp> props;
            cpp2::VertexProp tagProp;
            tagProp.set_tag(tagId_);
            (*tagProp.props_ref()).emplace_back(propName_);
            DataSet dataset({kVid});
            auto future = client_->getProps(spaceId_, dataset, &props, nullptr, nullptr);
            auto resp = std::move(future).get();
            if (!resp.succeeded()) {
                LOG(ERROR) << "Failed to fetch props of vertex " << nextId;
                return false;
            }
            // TODO
            #if 0
            auto& results = resp.responses();
            // get tag schema
            auto* vschema = results[0].get_vertex_schema();
            DCHECK(vschema != nullptr);
            auto tagIter = vschema->find(tagId_);
            DCHECK(tagIter != vschema->end());
            auto tagProvider = std::make_shared<ResultSchemaProvider>(tagIter->second);

            for (auto& vdata : results[0].vertices) {
                auto iter = std::find_if(vdata.tag_data.begin(), vdata.tag_data.end(),
                    [this] (const auto& tagData) {
                        return tagData.tag_id == tagId_;
                    });
                if (iter == vdata.tag_data.end()) {
                    return false;
                }
                auto tagReader = RowReaderWrapper::getRowReader(iter->data, tagProvider);
                auto ret = RowReader::getPropByName(tagReader.get(), propName_);
                CHECK(ok(ret));
                nextId = boost::get<int64_t>(value(ret));
            }
            #endif
            count++;
        }
        // after go to next node for width * height times, it should go back to where it starts
        if (nextId != startId) {
            return false;
        }
        return true;
    }

private:
    std::unique_ptr<GraphStorageClient>          client_;
    std::unique_ptr<meta::MetaClient>            mClient_;
    std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
    GraphSpaceID                                 spaceId_;
    TagID                                        tagId_;
    std::string                                  propName_;
    size_t                                       width_;
    size_t                                       height_;
    VertexID                                     firstVertexId_;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    nebula::storage::IntegrityTest integrity;
    return integrity.run();
}
