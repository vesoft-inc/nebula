/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef TOOLS_DBDUMP_DBDUMPER_H_
#define TOOLS_DBDUMP_DBDUMPER_H_

#include <folly/Range.h>            // for StringPiece
#include <gflags/gflags_declare.h>  // for DECLARE_string
#include <rocksdb/db.h>             // for DB
#include <rocksdb/options.h>        // for Options

#include <cstdint>        // for uint32_t, int64_t
#include <functional>     // for function
#include <memory>         // for unique_ptr
#include <string>         // for string, hash, basi...
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"  // for MetaClient
#include "codec/RowReaderWrapper.h"
#include "common/base/Base.h"
#include "common/base/Status.h"                    // for Status
#include "common/datatypes/Value.h"                // for Value
#include "common/meta/ServerBasedSchemaManager.h"  // for ServerBasedSchemaM...
#include "common/thrift/ThriftTypes.h"             // for VertexID, EdgeType
#include "interface/gen-cpp2/common_types.h"       // for PropertyType
#include "kvstore/RocksEngine.h"

namespace nebula {
class RowReader;
namespace kvstore {
class RocksPrefixIter;
}  // namespace kvstore

class RowReader;
namespace kvstore {
class RocksPrefixIter;
}  // namespace kvstore
}  // namespace nebula

DECLARE_string(space_name);
DECLARE_string(db_path);
DECLARE_string(meta_server);
DECLARE_string(parts);
DECLARE_string(mode);
DECLARE_string(vids);
DECLARE_string(tags);
DECLARE_string(edges);
DECLARE_int64(limit);

namespace nebula {
namespace storage {

class DbDumper {
 public:
  DbDumper() = default;

  ~DbDumper() = default;

  Status init();

  void run();

 private:
  Status initMeta();

  Status initSpace();

  Status initParams();

  Status openDb();

  void seekToFirst();

  void seek(std::string& prefix);

  void iterates(kvstore::RocksPrefixIter* it);

  inline void printTagKey(const folly::StringPiece& key);

  inline void printEdgeKey(const folly::StringPiece& key);

  std::string getTagName(const TagID tagId);

  std::string getEdgeName(const EdgeType edgeType);

  void printValue(const RowReader* reader);

  bool isValidVidLen(VertexID vid);

  Value getVertexId(const folly::StringPiece& vidStr);

 private:
  std::unique_ptr<rocksdb::DB> db_;
  rocksdb::Options options_;
  std::unique_ptr<meta::MetaClient> metaClient_;
  std::unique_ptr<meta::ServerBasedSchemaManager> schemaMng_;
  GraphSpaceID spaceId_;
  int32_t spaceVidLen_;
  nebula::cpp2::PropertyType spaceVidType_;
  int32_t partNum_;
  std::unordered_set<PartitionID> parts_;
  std::unordered_set<VertexID> vids_;
  std::unordered_set<TagID> tagIds_;
  std::unordered_set<EdgeType> edgeTypes_;
  std::vector<std::function<bool(const folly::StringPiece&)>> beforePrintVertex_;
  std::vector<std::function<bool(const folly::StringPiece&)>> beforePrintEdge_;

  // For statistics
  std::unordered_map<TagID, uint32_t> tagStat_;
  std::unordered_map<EdgeType, uint32_t> edgeStat_;
  int64_t count_{0};
  int64_t vertexCount_{0};
  int64_t edgeCount_{0};
};

}  // namespace storage
}  // namespace nebula
#endif  // TOOLS_DBDUMP_DBDUMPER_H_
