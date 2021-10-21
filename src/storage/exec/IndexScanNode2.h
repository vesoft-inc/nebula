/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include <gtest/gtest_prod.h>

#include <cstring>
#include <functional>

#include "common/base/Base.h"
#include "common/datatypes/DataSet.h"
#include "common/utils/IndexKeyUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "storage/CommonUtils.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {

class Path;
class RangePath;
class PrefixPath;
class IndexScanNode : public IndexNode {
  FRIEND_TEST(IndexScanTest, Base);
  FRIEND_TEST(IndexScanTest, Vertex);
  FRIEND_TEST(IndexScanTest, Edge);
  friend class IndexScanTestHelper;

 public:
  IndexScanNode(const IndexScanNode& node);
  IndexScanNode(RuntimeContext* context,
                const std::string& name,
                IndexID indexId,
                const std::vector<cpp2::IndexColumnHint>& columnHints,
                ::nebula::kvstore::KVStore* kvstore)
      : IndexNode(context, name), indexId_(indexId), columnHints_(columnHints), kvstore_(kvstore) {}
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override;
  std::string identify() override;

 protected:
  nebula::cpp2::ErrorCode doExecute(PartitionID partId) final;
  ErrorOr<Row> doNext(bool& hasNext) final;
  void decodePropFromIndex(folly::StringPiece key,
                           const Map<std::string, size_t>& colPosMap,
                           std::vector<Value>& values);
  virtual Row decodeFromIndex(folly::StringPiece key) = 0;
  virtual nebula::cpp2::ErrorCode getBaseData(folly::StringPiece key,
                                              std::pair<std::string, std::string>& kv) = 0;
  virtual Map<std::string, Value> decodeFromBase(const std::string& key,
                                                 const std::string& value) = 0;
  virtual const meta::SchemaProviderIf* getSchema() = 0;
  bool checkTTL();
  nebula::cpp2::ErrorCode resetIter(PartitionID partId);
  PartitionID partId_;
  const IndexID indexId_;
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  bool indexNullable_ = false;
  const std::vector<cpp2::IndexColumnHint>& columnHints_;
  std::unique_ptr<Path> path_;
  std::unique_ptr<kvstore::KVIterator> iter_;
  nebula::kvstore::KVStore* kvstore_;
  std::vector<std::string> requiredColumns_;
  Set<std::string> requiredAndHintColumns_;
  std::pair<bool, std::pair<int64_t, std::string>> ttlProps_;
  bool needAccessBase_{false};
  bool fatalOnBaseNotFound_{false};
};
/**
 * Path
 *
 * Path表示一个Index查询范围（range或prefix）。
 */
class Path {
 public:
  enum class Qualified : int16_t { INCOMPATIBLE = 0, UNCERTAIN = 1, COMPATIBLE = 2 };
  using QualifiedFunction = std::function<Qualified(const std::string&)>;
  using ColumnTypeDef = ::nebula::meta::cpp2::ColumnTypeDef;
  Path(nebula::meta::cpp2::IndexItem* index,
       const meta::SchemaProviderIf* schema,
       const std::vector<cpp2::IndexColumnHint>& hints,
       int64_t vidLen);
  virtual ~Path() = default;

  static std::unique_ptr<Path> make(::nebula::meta::cpp2::IndexItem* index,
                                    const meta::SchemaProviderIf* schema,
                                    const std::vector<cpp2::IndexColumnHint>& hints,
                                    int64_t vidLen);
  Qualified qualified(const folly::StringPiece& key);
  virtual bool isRange() { return false; }

  virtual Qualified qualified(const Map<std::string, Value>& rowData) = 0;
  virtual void resetPart(PartitionID partId) = 0;
  const std::string& toString();

 protected:
  std::string encodeValue(const Value& value,
                          const ColumnTypeDef& colDef,
                          size_t index,
                          std::string& key);
  std::vector<QualifiedFunction> QFList_;
  ::nebula::meta::cpp2::IndexItem* index_;
  const meta::SchemaProviderIf* schema_;
  const std::vector<cpp2::IndexColumnHint> hints_;
  std::vector<bool> nullable_;
  int64_t index_nullable_offset_{8};
  int64_t totalKeyLength_{8};
  int64_t suffixLength_;
  std::string serializeString_;
};
class PrefixPath : public Path {
 public:
  PrefixPath(nebula::meta::cpp2::IndexItem* index,
             const meta::SchemaProviderIf* schema,
             const std::vector<cpp2::IndexColumnHint>& hints,
             int64_t vidLen);
  // Override
  Qualified qualified(const Map<std::string, Value>& rowData) override;
  void resetPart(PartitionID partId) override;

  const std::string& getPrefixKey() { return prefix_; }

 private:
  std::string prefix_;
  void buildKey();
};
class RangePath : public Path {
 public:
  RangePath(nebula::meta::cpp2::IndexItem* index,
            const meta::SchemaProviderIf* schema,
            const std::vector<cpp2::IndexColumnHint>& hints,
            int64_t vidLen);
  // Override
  Qualified qualified(const Map<std::string, Value>& rowData) override;
  void resetPart(PartitionID partId) override;

  inline bool includeStart() { return includeStart_; }
  inline bool includeEnd() { return includeEnd_; }
  inline const std::string& getStartKey() { return startKey_; }
  inline const std::string& getEndKey() { return endKey_; }
  virtual bool isRange() { return true; }

 private:
  std::string startKey_, endKey_;
  bool includeStart_ = true;
  bool includeEnd_ = false;

  void buildKey();
  std::tuple<std::string, std::string> encodeRange(
      const cpp2::IndexColumnHint& hint,
      const nebula::meta::cpp2::ColumnTypeDef& colTypeDef,
      size_t colIndex,
      size_t offset);
  inline std::string encodeString(const Value& value, size_t len, bool& truncated);
  inline std::string encodeFloat(const Value& value, bool& isNaN);
  std::string encodeBeginValue(const Value& value,
                               const ColumnTypeDef& colDef,
                               std::string& key,
                               size_t offset);
  std::string encodeEndValue(const Value& value,
                             const ColumnTypeDef& colDef,
                             std::string& key,
                             size_t offset);
};

/* define inline functions */

}  // namespace storage

}  // namespace nebula
