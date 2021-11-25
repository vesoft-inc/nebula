/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "clients/storage/StorageClient.h"
#include "codec/RowReader.h"
#include "common/base/Base.h"
#include "common/time/Duration.h"

DEFINE_string(meta_server_addrs, "", "meta server address");
DEFINE_int32(io_threads, 10, "client io threads");

DEFINE_string(space_name, "test_space", "the space name");

DEFINE_string(first_key, "1", "the smallest key");
DEFINE_uint32(width, 100, "width of matrix");
DEFINE_uint32(height, 1000, "height of matrix");

namespace nebula {
namespace storage {

/**
 * We generate a big circle of data, all node are key/values, the value is the next node's key
 * , so we can validate them by traversing.
 *
 * The width and height is the size of the big linked list, you can refer to the graph below. As
 * expected, we can traverse the big linked list after width * height steps starting from any node
 * in the list.
 */
class IntegrityTest {
 public:
  IntegrityTest() : width_{FLAGS_width}, height_{FLAGS_height}, firstKey_{FLAGS_first_key} {}

  int run() {
    if (!init()) {
      return EXIT_FAILURE;
    }
    prepareData();
    if (!validate(firstKey_, width_ * height_)) {
      LOG(INFO) << "Integrity test failed";
      return EXIT_FAILURE;
    }
    LOG(INFO) << "Integrity test passed";
    return 0;
  }

 private:
  bool init() {
    if (static_cast<int64_t>(width_) * static_cast<int64_t>(height_) >
        std::numeric_limits<int32_t>::max()) {
      LOG(ERROR) << "Width * Height is out of range";
      return false;
    }

    auto metaAddrsRet = nebula::network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    if (!metaAddrsRet.ok() || metaAddrsRet.value().empty()) {
      LOG(ERROR) << "Can't get metaServer address, status: " << metaAddrsRet.status()
                 << ", FLAGS_meta_server_addrs: " << FLAGS_meta_server_addrs;
      return false;
    }
    threadPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_io_threads);

    meta::MetaClientOptions options;
    options.skipConfig_ = true;
    mClient_ = std::make_unique<meta::MetaClient>(threadPool_, metaAddrsRet.value(), options);
    if (!mClient_->waitForMetadReady()) {
      LOG(ERROR) << "Init meta client failed";
      return false;
    }

    auto spaceResult = mClient_->getSpaceIdByNameFromCache(FLAGS_space_name);
    if (!spaceResult.ok()) {
      LOG(ERROR) << "Get spaceId failed";
      return false;
    } else {
      spaceId_ = spaceResult.value();
    }

    client_ = std::make_unique<StorageClient>(threadPool_, mClient_.get());
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
    std::vector<std::string> first;
    std::vector<std::string> prev;
    std::vector<std::string> cur;

    LOG(INFO) << "Start insert kvs";
    for (size_t i = 0; i < width_; i++) {
      prev.emplace_back(std::to_string(std::atoi(firstKey_.c_str()) + i));
    }
    // leave alone the first line, generate other lines
    for (size_t i = 1; i < height_; i++) {
      insertRow(prev, cur, std::to_string(std::atoi(firstKey_.c_str()) + i * width_));
      prev = std::move(cur);
    }
    // shift the last line
    std::rotate(prev.begin(), prev.end() - 1, prev.end());
    // generate first line, each node in first line will points to a node in
    // rotated last line, which will make the matrix a big linked list
    insertRow(prev, first, firstKey_);
    LOG(INFO) << "Prepare data ok";
  }

  void insertRow(const std::vector<std::string>& prev,
                 std::vector<std::string>& cur,
                 const std::string& startId) {
    auto future = client_->put(spaceId_, genKeyValue(prev, cur, startId));
    auto resp = std::move(future).get();
    if (!resp.succeeded()) {
      for (auto& err : resp.failedParts()) {
        VLOG(2) << "Partition " << err.first
                << " failed: " << apache::thrift::util::enumNameSafe(err.second);
      }
    }
  }

  std::vector<KeyValue> genKeyValue(const std::vector<std::string>& prev,
                                    std::vector<std::string>& cur,
                                    const std::string& startId) {
    // We insert key-values of a row once a time
    std::vector<KeyValue> kvs;
    for (size_t i = 0; i < width_; i++) {
      auto key = std::to_string(std::atoi(startId.c_str()) + i);
      cur.emplace_back(key);
      kvs.emplace_back(std::make_pair(cur[i], prev[i]));

      VLOG(2) << "Build " << cur[i] << " -> " << prev[i];
      LOG_EVERY_N(INFO, 10000) << "We have inserted "
                               << std::atoi(key.c_str()) - std::atoi(firstKey_.c_str()) - width_
                               << " key-value so far, total: " << width_ * height_;
    }
    return kvs;
  }

  bool validate(const std::string& startId, int64_t queryTimes) {
    int64_t count = 0;
    std::string nextId = startId;
    while (count < queryTimes) {
      LOG_EVERY_N(INFO, 1000) << "We have gone " << count << " steps so far";
      auto future = client_->get(spaceId_, {nextId});
      auto resp = std::move(future).get();
      if (!resp.succeeded()) {
        LOG(ERROR) << "Failed to get value of " << nextId;
        return false;
      }

      const auto& results = resp.responses();
      DCHECK_EQ(results.size(), 1UL);
      auto kvs = results[0].get_key_values();
      auto iter = kvs.find(nextId);
      if (iter == kvs.end()) {
        LOG(ERROR) << "Value of " << nextId << " not found";
        return false;
      }
      nextId = iter->second;
      count++;
    }
    // after go to next node for width * height times, it should go back to
    // where it starts
    if (nextId != startId) {
      return false;
    }
    return true;
  }

 private:
  std::unique_ptr<StorageClient> client_;
  std::unique_ptr<meta::MetaClient> mClient_;
  std::shared_ptr<folly::IOThreadPoolExecutor> threadPool_;
  GraphSpaceID spaceId_;
  size_t width_;
  size_t height_;
  std::string firstKey_;
};

}  // namespace storage
}  // namespace nebula

int main(int argc, char* argv[]) {
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  nebula::storage::IntegrityTest integrity;
  return integrity.run();
}
