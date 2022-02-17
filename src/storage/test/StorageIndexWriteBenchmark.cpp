/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Benchmark.h>           // for BenchmarkSuspender
#include <folly/String.h>              // for stringPrintf
#include <folly/futures/Future.h>      // for Future::get
#include <folly/init/Init.h>           // for init
#include <gflags/gflags.h>             // for clstring, DEFINE_int32
#include <stddef.h>                    // for size_t
#include <stdint.h>                    // for int32_t, uint8_t
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>      // for max
#include <memory>         // for unique_ptr, make_un...
#include <string>         // for string, basic_string
#include <type_traits>    // for remove_reference<>:...
#include <unordered_map>  // for unordered_map<>::ma...
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/Logging.h"                  // for LOG, LogMessage
#include "common/datatypes/HostAddr.h"            // for HostAddr
#include "common/datatypes/Value.h"               // for Value
#include "common/fs/FileUtils.h"                  // for FileUtils
#include "common/meta/Common.h"                   // for PartHosts
#include "common/meta/IndexManager.h"             // for IndexManager
#include "common/meta/NebulaSchemaProvider.h"     // for NebulaSchemaProvider
#include "common/meta/SchemaManager.h"            // for SchemaManager
#include "common/thrift/ThriftTypes.h"            // for PartitionID, GraphS...
#include "common/utils/IndexKeyUtils.h"           // for IndexKeyUtils
#include "common/utils/NebulaKeyUtils.h"          // for NebulaKeyUtils
#include "common/utils/Types.h"                   // for IndexID
#include "interface/gen-cpp2/common_types.h"      // for ErrorCode, ErrorCod...
#include "interface/gen-cpp2/meta_types.h"        // for ColumnDef, ColumnTy...
#include "interface/gen-cpp2/storage_types.h"     // for NewTag, AddVertices...
#include "kvstore/KVIterator.h"                   // for KVIterator
#include "kvstore/KVStore.h"                      // for KVOptions, KVStore
#include "kvstore/NebulaStore.h"                  // for NebulaStore
#include "kvstore/PartManager.h"                  // for MemPartManager, Par...
#include "mock/AdHocIndexManager.h"               // for AdHocIndexManager
#include "mock/AdHocSchemaManager.h"              // for AdHocSchemaManager
#include "mock/MockCluster.h"                     // for MockCluster
#include "storage/CommonUtils.h"                  // for IndexGuard, StorageEnv
#include "storage/mutate/AddVerticesProcessor.h"  // for AddVerticesProcessor

DEFINE_int32(bulk_insert_size, 10000, "The number of vertices by bulk insert");
DEFINE_int32(total_vertices_size, 1000000, "The number of vertices");
DEFINE_string(root_data_path, "/tmp/IndexWritePref", "Engine data path");

namespace nebula {
namespace storage {

using NewVertex = nebula::storage::cpp2::NewVertex;
using NewTag = nebula::storage::cpp2::NewTag;

enum class IndexENV : uint8_t {
  NO_INDEX = 1,
  ONE_INDEX = 2,
  MULTIPLE_INDEX = 3,
  INVALID_INDEX = 4,
};

GraphSpaceID spaceId = 1;
TagID tagId = 1;
IndexID indexId = 1;

std::string toVertexId(size_t vIdLen, int32_t vId) {
  std::string id;
  id.reserve(vIdLen);
  id.append(reinterpret_cast<const char*>(&vId), sizeof(vId)).append(vIdLen - sizeof(vId), '\0');
  return id;
}

std::unique_ptr<kvstore::MemPartManager> memPartMan(const std::vector<PartitionID>& parts) {
  auto memPartMan = std::make_unique<kvstore::MemPartManager>();
  // GraphSpaceID =>  {PartitionIDs}
  auto& partsMap = memPartMan->partsMap();
  for (auto partId : parts) {
    partsMap[spaceId][partId] = meta::PartHosts();
  }
  return memPartMan;
}

std::vector<nebula::meta::cpp2::ColumnDef> mockTagIndexColumns() {
  std::vector<nebula::meta::cpp2::ColumnDef> cols;
  for (int32_t i = 0; i < 3; i++) {
    nebula::meta::cpp2::ColumnDef col;
    col.name = folly::stringPrintf("col_%d", i);
    col.type.type = nebula::cpp2::PropertyType::INT64;
    cols.emplace_back(std::move(col));
  }
  return cols;
}

std::shared_ptr<meta::NebulaSchemaProvider> mockTagSchema() {
  std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
  for (int32_t i = 0; i < 3; i++) {
    nebula::meta::cpp2::ColumnDef col;
    col.name = folly::stringPrintf("col_%d", i);
    col.type.type = nebula::cpp2::PropertyType::INT64;
    schema->addField(col.name, col.type.type);
  }
  return schema;
}

std::unique_ptr<meta::SchemaManager> memSchemaMan() {
  auto schemaMan = std::make_unique<mock::AdHocSchemaManager>();
  schemaMan->addTagSchema(spaceId, tagId, mockTagSchema());
  return schemaMan;
}

std::unique_ptr<meta::IndexManager> memIndexMan(IndexENV type) {
  auto indexMan = std::make_unique<mock::AdHocIndexManager>();
  switch (type) {
    case IndexENV::NO_INDEX:
      break;
    case IndexENV::ONE_INDEX: {
      indexMan->addTagIndex(spaceId, tagId, indexId, mockTagIndexColumns());
      break;
    }
    case IndexENV::INVALID_INDEX: {
      indexMan->addTagIndex(spaceId, -1, indexId, mockTagIndexColumns());
      break;
    }
    case IndexENV::MULTIPLE_INDEX: {
      indexMan->addTagIndex(spaceId, tagId, indexId, mockTagIndexColumns());
      indexMan->addTagIndex(spaceId, tagId, indexId + 1, mockTagIndexColumns());
      indexMan->addTagIndex(spaceId, tagId, indexId + 2, mockTagIndexColumns());
      break;
    }
  }
  return indexMan;
}

std::vector<NewVertex> genVertices(size_t vLen, int32_t& vId) {
  std::vector<NewVertex> vertices;
  for (auto i = 0; i < FLAGS_bulk_insert_size; i++) {
    NewVertex newVertex;
    NewTag newTag;
    newTag.tag_id_ref() = tagId;
    std::vector<Value> props;
    props.emplace_back(Value(1L + i));
    props.emplace_back(Value(2L + i));
    props.emplace_back(Value(3L + i));
    newTag.props_ref() = std::move(props);
    std::vector<nebula::storage::cpp2::NewTag> newTags;
    newTags.push_back(std::move(newTag));
    newVertex.id_ref() = toVertexId(vLen, vId++);
    newVertex.tags_ref() = std::move(newTags);
    vertices.emplace_back(std::move(newVertex));
  }
  return vertices;
}

bool processVertices(StorageEnv* env, int32_t& vId) {
  cpp2::AddVerticesRequest req;
  BENCHMARK_SUSPEND {
    req.space_id_ref() = 1;
    req.if_not_exists_ref() = true;
    auto newVertex = genVertices(32, vId);
    (*req.parts_ref())[1] = std::move(newVertex);
  };

  auto* processor = AddVerticesProcessor::instance(env, nullptr);
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  BENCHMARK_SUSPEND {
    if (!resp.result.failed_parts.empty()) {
      return false;
    }
  };
  return true;
}

void initEnv(IndexENV type,
             const std::string& dataPath,
             std::unique_ptr<storage::StorageEnv>& env,
             std::unique_ptr<kvstore::NebulaStore>& kv,
             std::unique_ptr<meta::SchemaManager>& sm,
             std::unique_ptr<meta::IndexManager>& im) {
  env = std::make_unique<StorageEnv>();
  const std::vector<PartitionID> parts{1};
  kvstore::KVOptions options;
  HostAddr localHost = HostAddr("0", 0);
  LOG(INFO) << "Use meta in memory!";
  options.partMan_ = memPartMan(parts);
  std::vector<std::string> paths;
  paths.emplace_back(folly::stringPrintf("%s/disk1", dataPath.c_str()));
  paths.emplace_back(folly::stringPrintf("%s/disk2", dataPath.c_str()));
  options.dataPaths_ = std::move(paths);
  kv = mock::MockCluster::initKV(std::move(options), localHost);
  mock::MockCluster::waitUntilAllElected(kv.get(), 1, parts);
  sm = memSchemaMan();
  im = memIndexMan(type);
  env->schemaMan_ = std::move(sm).get();
  env->indexMan_ = std::move(im).get();
  env->kvstore_ = std::move(kv).get();
  env->rebuildIndexGuard_ = std::make_unique<IndexGuard>();
  env->verticesML_ = std::make_unique<storage::VerticesMemLock>();
}

void verifyDataCount(storage::StorageEnv* env, int32_t expected) {
  auto prefix = NebulaKeyUtils::tagPrefix(1);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
  DCHECK(nebula::cpp2::ErrorCode::SUCCEEDED == status);
  int32_t cnt = 0;
  while (iter->valid()) {
    cnt++;
    iter->next();
  }
  DCHECK(expected == cnt);
}

void verifyIndexCount(storage::StorageEnv* env, int32_t expected) {
  auto prefix = IndexKeyUtils::indexPrefix(1);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto status = env->kvstore_->prefix(spaceId, 1, prefix, &iter);
  DCHECK(nebula::cpp2::ErrorCode::SUCCEEDED == status);
  int32_t cnt = 0;
  while (iter->valid()) {
    cnt++;
    iter->next();
  }
  DCHECK(expected == cnt);
}

void insertVertices(bool withoutIndex) {
  std::unique_ptr<storage::StorageEnv> env;
  std::unique_ptr<kvstore::NebulaStore> kv;
  std::unique_ptr<meta::SchemaManager> sm;
  std::unique_ptr<meta::IndexManager> im;
  int32_t vId = 0;
  BENCHMARK_SUSPEND {
    std::string dataPath =
        withoutIndex ? folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "withoutIndex")
                     : folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "attachIndex");
    auto type = withoutIndex ? IndexENV::NO_INDEX : IndexENV::ONE_INDEX;
    initEnv(type, dataPath, env, kv, sm, im);
  };

  while (vId < FLAGS_total_vertices_size) {
    if (!processVertices(env.get(), vId)) {
      LOG(ERROR) << "Vertices bulk insert error";
      return;
    }
  }
  BENCHMARK_SUSPEND {
    verifyDataCount(env.get(), FLAGS_total_vertices_size);
    if (!withoutIndex) {
      verifyIndexCount(env.get(), FLAGS_total_vertices_size);
    }
  };
  BENCHMARK_SUSPEND {
    im.reset();
    kv.reset();
    sm.reset();
    env.reset();
    fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
  };
}

void insertUnmatchIndex() {
  std::unique_ptr<storage::StorageEnv> env;
  std::unique_ptr<kvstore::NebulaStore> kv;
  std::unique_ptr<meta::SchemaManager> sm;
  std::unique_ptr<meta::IndexManager> im;
  int32_t vId = 0;
  BENCHMARK_SUSPEND {
    std::string dataPath =
        folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "unmatchIndex");
    initEnv(IndexENV::INVALID_INDEX, dataPath, env, kv, sm, im);
  };

  while (vId < FLAGS_total_vertices_size) {
    if (!processVertices(env.get(), vId)) {
      LOG(ERROR) << "Vertices bulk insert error";
      return;
    }
  }
  BENCHMARK_SUSPEND {
    verifyDataCount(env.get(), FLAGS_total_vertices_size);
    verifyIndexCount(env.get(), 0);
  };
  BENCHMARK_SUSPEND {
    im.reset();
    kv.reset();
    sm.reset();
    env.reset();
    fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
  };
}

void insertDupVertices() {
  std::unique_ptr<storage::StorageEnv> env;
  std::unique_ptr<kvstore::NebulaStore> kv;
  std::unique_ptr<meta::SchemaManager> sm;
  std::unique_ptr<meta::IndexManager> im;
  int32_t vId = 0;
  BENCHMARK_SUSPEND {
    std::string dataPath =
        folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "duplicateIndex");
    initEnv(IndexENV::ONE_INDEX, dataPath, env, kv, sm, im);
  };

  while (vId < FLAGS_total_vertices_size) {
    if (!processVertices(env.get(), vId)) {
      LOG(ERROR) << "Vertices bulk insert error";
      return;
    }
  }

  vId = 0;
  while (vId < FLAGS_total_vertices_size) {
    if (!processVertices(env.get(), vId)) {
      LOG(ERROR) << "Vertices bulk insert error";
      return;
    }
  }

  BENCHMARK_SUSPEND {
    verifyDataCount(env.get(), FLAGS_total_vertices_size);
    verifyIndexCount(env.get(), FLAGS_total_vertices_size);
  };
  BENCHMARK_SUSPEND {
    im.reset();
    kv.reset();
    sm.reset();
    env.reset();
    fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
  };
}

void insertVerticesMultIndex() {
  std::unique_ptr<storage::StorageEnv> env;
  std::unique_ptr<kvstore::NebulaStore> kv;
  std::unique_ptr<meta::SchemaManager> sm;
  std::unique_ptr<meta::IndexManager> im;
  int32_t vId = 0;
  BENCHMARK_SUSPEND {
    std::string dataPath = folly::stringPrintf("%s/%s", FLAGS_root_data_path.c_str(), "multIndex");
    initEnv(IndexENV::MULTIPLE_INDEX, dataPath, env, kv, sm, im);
  };

  while (vId < FLAGS_total_vertices_size) {
    if (!processVertices(env.get(), vId)) {
      LOG(ERROR) << "Vertices bulk insert error";
      return;
    }
  }

  BENCHMARK_SUSPEND {
    verifyDataCount(env.get(), FLAGS_total_vertices_size);
    verifyIndexCount(env.get(), FLAGS_total_vertices_size * 3);
  };
  BENCHMARK_SUSPEND {
    im.reset();
    kv.reset();
    sm.reset();
    env.reset();
    fs::FileUtils::remove(FLAGS_root_data_path.c_str(), true);
  };
}

BENCHMARK(withoutIndex) {
  insertVertices(true);
}

BENCHMARK(unmatchIndex) {
  insertUnmatchIndex();
}

BENCHMARK(attachIndex) {
  insertVertices(false);
}

BENCHMARK(duplicateVerticesIndex) {
  insertDupVertices();
}

BENCHMARK(multipleIndex) {
  insertVerticesMultIndex();
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  folly::init(&argc, &argv, true);
  folly::runBenchmarks();
  return 0;
}

/**
 * withoutIndex: Without index, insert data only.
 * unmatchIndex: There are no matched indexes.
 * attachIndex: One index, the index contains all the columns of tag.
 * duplicateVerticesIndex: One index, and insert duplicate vertices.
 * multipleIndex: Three indexes by one tag.
 *
 * 56 processors, Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz
 *
 * --bulk_insert_size = 10000
 * --total_vertices_size = 1000000
 *
 * ============================================================================
 * src/storage/test/StorageIndexWriteBenchmark.cpprelative  time/iter  iters/s
 * ============================================================================
 * withoutIndex                                                10.05ms    99.47
 * unmatchIndex                                                  5.42s  184.56m
 * attachIndex                                                   9.23s  108.40m
 * duplicateVerticesIndex                                       15.59s   64.14m
 * multipleIndex                                                14.88s   67.22m
 * ============================================================================
 **/
