/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
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
 public:
  IndexScanNode(const IndexScanNode& node);
  IndexScanNode(RuntimeContext* context,
                const std::string& name,
                IndexID indexId,
                const std::vector<cpp2::IndexColumnHint>& columnHints)
      : IndexNode(context, name), indexId_(indexId), columnHints_(columnHints) {}
  ::nebula::cpp2::ErrorCode init(InitContext& ctx) override {
    for (auto& col : ctx.requiredColumns) {
      requiredColumns_.push_back(col);
    }
    ctx.returnColumns = requiredColumns_;
    for (size_t i = 0; i < ctx.returnColumns.size(); i++) {
      ctx.retColMap[ctx.returnColumns[i]] = i;
    }
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

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
  std::pair<bool, std::pair<int64_t, std::string>> ttlProps_;
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
  Path(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
       std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
       const std::vector<cpp2::IndexColumnHint>& hints)
      : index_(index), schema_(schema), hints_(hints) {}
  virtual ~Path() = default;

  static std::unique_ptr<Path> make(
      std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
      std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
      const std::vector<cpp2::IndexColumnHint>& hints);
  Qualified qualified(const folly::StringPiece& key);
  virtual bool isRange() { return false; }

  virtual Qualified qualified(const Map<std::string, Value>& rowData) = 0;
  virtual void resetPart(PartitionID partId) = 0;

 protected:
  std::string encodeValue(const Value& value, const ColumnTypeDef& colDef, std::string& key);
  std::vector<QualifiedFunction> QFList_;
  std::shared_ptr<nebula::meta::cpp2::IndexItem> index_;
  std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema_;
  const std::vector<cpp2::IndexColumnHint>& hints_;
};
class PrefixPath : public Path {
 public:
  PrefixPath(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
             std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
             const std::vector<cpp2::IndexColumnHint>& hints);
  // Override
  Qualified qualified(const Map<std::string, Value>& rowData) override;
  void resetPart(PartitionID partId) override;

  const std::string& getPrefixKey();

 private:
  std::string prefix_;
  void buildKey();
};
class RangePath : public Path {
 public:
  RangePath(std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
            std::shared_ptr<const nebula::meta::NebulaSchemaProvider> schema,
            const std::vector<cpp2::IndexColumnHint>& hints);
  // Override
  Qualified qualified(const Map<std::string, Value>& rowData) override;
  void resetPart(PartitionID partId) override;

  inline bool eqToStart() { return eqToStart_; }
  inline bool eqToEnd() { return eqToEnd_; }
  inline const std::string& getStartKey() { return startKey_; }
  inline const std::string& getEndKey() { return endKey_; }
  virtual bool isRange() { return true; }

 private:
  std::string startKey_, endKey_;
  bool eqToStart_, eqToEnd_;
  void buildKey();
  std::string encodeBeginValue(const Value& value, const ColumnTypeDef& colDef, std::string& key);
  std::string encodeEndValue(const Value& value, const ColumnTypeDef& colDef, std::string& key);
};

/* define inline functions */
}  // namespace storage

}  // namespace nebula
