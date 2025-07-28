/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TEST_INDEXTESTUTIL_H
#define STORAGE_TEST_INDEXTESTUTIL_H
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <regex>
#include <string>

#include "common/datatypes/DataSet.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "folly/Conv.h"
#include "folly/String.h"
#include "interface/gen-cpp2/common_types.h"
#include "kvstore/KVIterator.h"
#include "kvstore/KVStore.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
using ::nebula::kvstore::KVIterator;
class MockKVIterator : public KVIterator {
  using KVMap = std::map<std::string, std::string>;

 public:
  MockKVIterator(const KVMap& kv, KVMap::iterator&& iter) : kv_(kv), iter_(std::move(iter)) {}
  bool valid() const {
    return iter_ != kv_.end() && validFunc_(iter_);
  }
  void next() {
    iter_++;
  }
  void prev() {
    iter_--;
  }
  folly::StringPiece key() const {
    return folly::StringPiece(iter_->first);
  }
  folly::StringPiece val() const {
    return folly::StringPiece(iter_->second);
  }
  void setValidFunc(const std::function<bool(const KVMap::iterator& iter)> validFunc) {
    validFunc_ = validFunc;
  }

 private:
  const KVMap& kv_;
  KVMap::iterator iter_;
  std::function<bool(const KVMap::iterator& iter)> validFunc_;
};
class MockKVStore : public ::nebula::kvstore::KVStore {
 private:
  GraphSpaceID spaceId_{0};
  std::map<std::string, std::string> kv_;

 public:
  MockKVStore() {}
  // Return bit-OR of StoreCapability values;
  uint32_t capability() const override {
    CHECK(false);
    return 0;
  };
  void stop() override {}
  ErrorOr<nebula::cpp2::ErrorCode, HostAddr> partLeader(GraphSpaceID, PartitionID) override {
    CHECK(false);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  const void* GetSnapshot(GraphSpaceID, PartitionID) override {
    return nullptr;
  }
  void ReleaseSnapshot(GraphSpaceID, PartitionID, const void*) override {}
  // Read a single key
  nebula::cpp2::ErrorCode get(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& key,
                              std::string* value,
                              bool canReadFromFollower = false,
                              const void* snapshot = nullptr) override {
    UNUSED(canReadFromFollower);
    UNUSED(partId);
    UNUSED(snapshot);
    CHECK_EQ(spaceId, spaceId_);
    auto iter = kv_.lower_bound(key);
    if (iter != kv_.end() && iter->first == key) {
      *value = iter->second;
      return ::nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
      return ::nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
    }
  }

  // Read multiple keys, if error occurs a cpp2::ErrorCode is returned,
  // If key[i] does not exist, the i-th value in return value would be
  // Status::KeyNotFound
  std::pair<nebula::cpp2::ErrorCode, std::vector<Status>> multiGet(
      GraphSpaceID,
      PartitionID,
      const std::vector<std::string>& keys,
      std::vector<std::string>* values,
      bool) override {
    std::vector<Status> status;
    nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    for (auto& key : keys) {
      auto iter = kv_.lower_bound(key);
      if (iter != kv_.end() && iter->first == key) {
        values->push_back(iter->second);
        status.push_back(Status::OK());
      } else {
        values->push_back("");
        status.push_back(Status::KeyNotFound());
        ret = nebula::cpp2::ErrorCode::E_PARTIAL_RESULT;
      }
    }
    return {ret, std::move(status)};
  }

  // Get all results in range [start, end)
  nebula::cpp2::ErrorCode range(GraphSpaceID spaceId,
                                PartitionID,
                                const std::string& start,
                                const std::string& end,
                                std::unique_ptr<KVIterator>* iter,
                                bool) override {
    CHECK_EQ(spaceId, spaceId_);
    std::unique_ptr<MockKVIterator> mockIter;
    mockIter = std::make_unique<MockKVIterator>(kv_, kv_.lower_bound(start));
    mockIter->setValidFunc([end](const decltype(kv_)::iterator& it) { return it->first < end; });
    (*iter) = std::move(mockIter);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode prefix(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 const std::string& prefix,
                                 std::unique_ptr<KVIterator>* iter,
                                 bool canReadFromFollower = false,
                                 const void* snapshot = nullptr) override {
    UNUSED(canReadFromFollower);
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(snapshot);  // Pity that mock kv don't have snap.
    CHECK_EQ(spaceId, spaceId_);
    auto mockIter = std::make_unique<MockKVIterator>(kv_, kv_.lower_bound(prefix));
    mockIter->setValidFunc([prefix](const decltype(kv_)::iterator& it) {
      if (it->first.size() < prefix.size()) {
        return false;
      }
      for (size_t i = 0; i < prefix.size(); i++) {
        if (prefix[i] != it->first[i]) {
          return false;
        }
      }
      return true;
    });
    (*iter) = std::move(mockIter);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  // Get all results with prefix starting from start
  nebula::cpp2::ErrorCode rangeWithPrefix(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          const std::string& start,
                                          const std::string& prefix,
                                          std::unique_ptr<KVIterator>* iter,
                                          bool canReadFromFollower = false) override {
    UNUSED(canReadFromFollower);
    UNUSED(spaceId);
    UNUSED(partId);
    CHECK_EQ(spaceId, spaceId_);
    auto mockIter = std::make_unique<MockKVIterator>(kv_, kv_.lower_bound(start));
    mockIter->setValidFunc([prefix](const decltype(kv_)::iterator& it) {
      if (it->first.size() < prefix.size()) {
        return false;
      }
      for (size_t i = 0; i < prefix.size(); i++) {
        if (prefix[i] != it->first[i]) {
          return false;
        }
      }
      return true;
    });
    (*iter) = std::move(mockIter);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode sync(GraphSpaceID, PartitionID) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void asyncMultiPut(GraphSpaceID,
                     PartitionID,
                     std::vector<::nebula::kvstore::KV>&& keyValues,
                     ::nebula::kvstore::KVCallback) override {
    for (size_t i = 0; i < keyValues.size(); i++) {
      kv_.emplace(std::move(keyValues[i]));
    }
  }

  // Asynchronous version of remove methods
  void asyncRemove(GraphSpaceID,
                   PartitionID,
                   const std::string& key,
                   ::nebula::kvstore::KVCallback) override {
    kv_.erase(key);
  }

  void asyncMultiRemove(GraphSpaceID,
                        PartitionID,
                        std::vector<std::string>&& keys,
                        ::nebula::kvstore::KVCallback) override {
    for (size_t i = 0; i < keys.size(); i++) {
      kv_.erase(keys[i]);
    }
  }

  void asyncRemoveRange(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::string& start,
                        const std::string& end,
                        ::nebula::kvstore::KVCallback cb) override {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(cb);
    for (auto iter = kv_.lower_bound(start); iter != kv_.end();) {
      if (iter->first < end) {
        iter = kv_.erase(iter);
      } else {
        iter++;
      }
    }
  }

  void asyncAtomicOp(GraphSpaceID spaceId,
                     PartitionID partId,
                     ::nebula::kvstore::MergeableAtomicOp op,
                     ::nebula::kvstore::KVCallback cb) override {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(cb);
    UNUSED(op);
    LOG(FATAL) << "Unexpect";
  }
  void asyncAppendBatch(GraphSpaceID,
                        PartitionID,
                        std::string&&,
                        ::nebula::kvstore::KVCallback) override {
    LOG(FATAL) << "Unexpect";
  }
  nebula::cpp2::ErrorCode ingest(GraphSpaceID) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  int32_t allLeader(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>&) override {
    LOG(FATAL) << "Unexpect";
    return 0;
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<::nebula::kvstore::Part>> part(
      GraphSpaceID, PartitionID) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  nebula::cpp2::ErrorCode compact(GraphSpaceID) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  nebula::cpp2::ErrorCode flush(GraphSpaceID) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<::nebula::cpp2::CheckpointInfo>> createCheckpoint(
      GraphSpaceID, const std::string&) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  nebula::cpp2::ErrorCode dropCheckpoint(GraphSpaceID, const std::string&) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode setWriteBlocking(GraphSpaceID, bool) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupTable(
      GraphSpaceID,
      const std::string&,
      const std::string&,
      std::function<bool(const folly::StringPiece& key)>) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  // for meta BR
  nebula::cpp2::ErrorCode restoreFromFiles(GraphSpaceID, const std::vector<std::string>&) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  std::unique_ptr<nebula::kvstore::WriteBatch> startBatchWrite() override {
    LOG(FATAL) << "Unexpect";
    return nullptr;
  }
  nebula::cpp2::ErrorCode batchWriteWithoutReplicator(
      GraphSpaceID, std::unique_ptr<nebula::kvstore::WriteBatch>) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  std::vector<std::string> getDataRoot() const override {
    LOG(FATAL) << "Unexpect";
    return {};
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::string> getProperty(GraphSpaceID,
                                                            const std::string&) override {
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  void put(const std::string& key, const std::string& value) {
    kv_[key] = value;
  }

 private:
  using ::nebula::kvstore::KVStore::prefix;
  using ::nebula::kvstore::KVStore::range;
  using ::nebula::kvstore::KVStore::rangeWithPrefix;
};
class MockIndexNode : public IndexNode {
 public:
  explicit MockIndexNode(RuntimeContext* context) : IndexNode(context, "MockIndexNode") {}
  ::nebula::cpp2::ErrorCode init(InitContext& initCtx) override {
    return initFunc(initCtx);
  }
  std::unique_ptr<IndexNode> copy() override {
    LOG(FATAL) << "Unexpect";
    return nullptr;
  }
  std::function<Result()> nextFunc;
  std::function<::nebula::cpp2::ErrorCode(PartitionID)> executeFunc;
  std::function<::nebula::cpp2::ErrorCode(InitContext& initCtx)> initFunc;
  std::string identify() override {
    return "MockIndexNode";
  }

 private:
  Result doNext() override {
    return nextFunc();
  }
  ::nebula::cpp2::ErrorCode doExecute(PartitionID partId) override {
    return executeFunc(partId);
  };
};

class RowParser {
 public:
  explicit RowParser(const std::string& str) {
    ss = std::stringstream(folly::trimWhitespace(folly::StringPiece(str)).toString());
    parseHeader();
    parseRow();
  }
  void parseHeader() {
    std::string line;
    std::getline(ss, line);
    std::vector<std::string> types;
    folly::split("|", line, types);
    for (size_t i = 0; i < types.size(); i++) {
      types[i] = folly::trimWhitespace(folly::StringPiece(types[i])).toString();
    }
    typeList_ = std::move(types);
  }
  void parseRow() {
    std::string line;
    while (std::getline(ss, line)) {
      std::vector<std::string> values;
      folly::split("|", line, values);
      for (size_t i = 0; i < values.size(); i++) {
        values[i] = folly::trimWhitespace(folly::StringPiece(values[i])).toString();
      }
      Row row;
      for (size_t i = 0; i < values.size(); i++) {
        if (values[i] == "<null>") {
          row.emplace_back(Value::null());
        } else if (values[i] == "<INF>") {
          row.emplace_back(std::numeric_limits<double>::infinity());
        } else if (values[i] == "<-INF>") {
          row.emplace_back(-std::numeric_limits<double>::infinity());
        } else if (values[i] == "<NaN>") {
          row.emplace_back(std::numeric_limits<double>::quiet_NaN());
        } else if (values[i] == "<-NaN>") {
          row.emplace_back(-std::numeric_limits<double>::quiet_NaN());
        } else if (values[i] == "<now>") {
          row.emplace_back(Value(time::WallClock::fastNowInSec()));
        } else {
          row.emplace_back(transformMap[typeList_[i]](values[i]));
        }
      }
      rowList_.emplace_back(std::move(row));
    }
  }
  const std::vector<Row>& getResult() {
    return rowList_;
  }

 private:
  Time stringToTime(const std::string& str) {
    uint32_t hour;
    uint32_t minute;
    uint32_t sec;
    uint32_t microsec;
    EXPECT_EQ(4, std::sscanf(str.c_str(), "%u:%u:%u.%u", &hour, &minute, &sec, &microsec));
    return Time(hour, minute, sec, microsec);
  }

  Date stringToDate(const std::string& str) {
    uint32_t year;
    uint32_t month;
    uint32_t day;
    EXPECT_EQ(3, std::sscanf(str.c_str(), "%u-%u-%u", &year, &month, &day));
    return Date(year, month, day);
  }

  DateTime stringToDateTime(const std::string& str) {
    uint32_t year;
    uint32_t month;
    uint32_t day;
    uint32_t hour;
    uint32_t minute;
    uint32_t sec;
    uint32_t microsec;
    EXPECT_EQ(7,
              std::sscanf(str.c_str(),
                          "%u-%u-%uT%u:%u:%u.%u",
                          &year,
                          &month,
                          &day,
                          &hour,
                          &minute,
                          &sec,
                          &microsec));
    return DateTime(year, month, day, hour, minute, sec, microsec);
  }

 private:
  std::stringstream ss;
  std::vector<std::string> typeList_;
  std::vector<Row> rowList_;
  std::map<std::string, std::function<Value(const std::string& str)>> transformMap{
      {"int", [](const std::string& str) { return Value(std::stol(str)); }},
      {"string", [](const std::string& str) { return Value(str); }},
      {"float", [](const std::string& str) { return Value(folly::to<double>(str)); }},
      {"bool", [](const std::string& str) { return Value(str == "true" ? true : false); }},
      {"fixed_string", [](const std::string& str) { return Value(str); }},
      {"date", [this](const std::string& str) { return Value(stringToDate(str)); }},
      {"time", [this](const std::string& str) { return Value(stringToTime(str)); }},
      {"datetime", [this](const std::string& str) { return Value(stringToDateTime(str)); }},
      {"geography", [](const std::string& str) { return Value(Geography::fromWKT(str).value()); }}};
};

/**
 * define a schema
 *
 * format:
 *    name | type | length | nullable
 * example:
 * std::string str=R"(
 *    a | int        |    |
 *    b | string     |    | true
 *    c | double | 10 |
 * )"_schema
 */
class SchemaParser {
 public:
  explicit SchemaParser(const std::string& str) {
    schema = std::make_shared<::nebula::meta::NebulaSchemaProvider>(0);
    ss = std::stringstream(folly::trimWhitespace(folly::StringPiece(str)).toString());
    parse();
  }
  void parse() {
    std::string line;
    while (std::getline(ss, line)) {
      std::vector<std::string> values;
      folly::split("|", line, values);
      std::string name = folly::trimWhitespace(folly::StringPiece(values[0])).toString();
      auto type = typeMap[folly::trimWhitespace(folly::StringPiece(values[1])).toString()];
      int length = 0;
      {
        std::string lenStr = folly::trimWhitespace(folly::StringPiece(values[2])).toString();
        if (lenStr != "") {
          length = std::stoi(lenStr);
        }
      }
      bool nullable = false;
      {
        std::string nullableStr = folly::trimWhitespace(folly::StringPiece(values[3])).toString();
        if (nullableStr == "true") {
          nullable = true;
        }
      }
      schema->addField(name, type, length, nullable);
    }
  }
  std::shared_ptr<::nebula::meta::NebulaSchemaProvider> getResult() {
    return schema;
  }

 private:
  std::stringstream ss;
  std::shared_ptr<::nebula::meta::NebulaSchemaProvider> schema;
  std::map<std::string, ::nebula::cpp2::PropertyType> typeMap{
      {"int", ::nebula::cpp2::PropertyType::INT64},
      {"double", ::nebula::cpp2::PropertyType::DOUBLE},
      {"string", ::nebula::cpp2::PropertyType::STRING},
      {"fixed_string", ::nebula::cpp2::PropertyType::FIXED_STRING},
      {"bool", ::nebula::cpp2::PropertyType::BOOL},
      {"date", ::nebula::cpp2::PropertyType::DATE},
      {"time", ::nebula::cpp2::PropertyType::TIME},
      {"datetime", ::nebula::cpp2::PropertyType::DATETIME},
      {"geography", ::nebula::cpp2::PropertyType::GEOGRAPHY}};
};

/**
 * define index of a schema
 *
 * format:
 * (Tag|Edge)(name,id)
 * example
 * std::string str=R"(
 * Tag(name,id)
 * (i1,1): a,b(10),c
 * (i2,2): b(5),c
 * )"_index(schema)
 */
class IndexParser {
 public:
  using IndexItem = ::nebula::meta::cpp2::IndexItem;
  using SchemaProvider = ::nebula::meta::NebulaSchemaProvider;
  explicit IndexParser(const std::string& str) {
    ss = std::stringstream(folly::trimWhitespace(folly::StringPiece(str)).toString());
    parseSchema();
  }
  void parseSchema() {
    static std::regex pattern(R"((TAG|EDGE)\((.+),(\d+)\))");
    std::smatch match;
    std::string line;
    std::getline(ss, line);
    CHECK(std::regex_match(line, match, pattern));
    std::string name = match.str(2);
    int32_t id = std::stoi(match.str(3));
    schemaName_ = name;
    if (match.str(1) == "TAG") {
      schemaId_.tag_id_ref() = id;
    } else {
      schemaId_.edge_type_ref() = id;
    }
  }
  std::vector<std::shared_ptr<IndexItem>> operator()(std::shared_ptr<SchemaProvider> schema) {
    schema_ = schema;
    std::vector<std::shared_ptr<IndexItem>> ret;
    std::string line;
    while (std::getline(ss, line)) {
      auto index = parse(folly::trimWhitespace(folly::StringPiece(line)).toString());
      ret.push_back(index);
    }
    return ret;
  }
  std::shared_ptr<IndexItem> parse(const std::string& line) {
    auto ret = std::make_shared<IndexItem>();
    ret->schema_id_ref() = schemaId_;
    ret->schema_name_ref() = schemaName_;
    static std::regex pattern(R"(\((.+),(\d+)\):(.+))");
    std::smatch match;
    CHECK(std::regex_match(line, match, pattern));
    ret->index_name_ref() = folly::trimWhitespace(folly::StringPiece(match.str(1)).toString());
    ret->index_id_ref() = std::stoi(match.str(2));
    std::string columnStr = match.str(3);
    std::vector<std::string> columns;
    folly::split(",", columnStr, columns);
    for (size_t i = 0; i < columns.size(); i++) {
      columns[i] = folly::trimWhitespace(folly::StringPiece(columns[i])).toString();
    }
    std::vector<::nebula::meta::cpp2::ColumnDef> fields;
    for (auto& column : columns) {
      std::string name;
      int length;
      std::smatch m;
      std::regex p(R"((.+)\((\d+)\))");
      if (std::regex_match(column, m, p)) {
        name = m.str(1);
        length = std::stoi(m.str(2));
      } else {
        name = column;
        length = 0;
      }
      ::nebula::meta::cpp2::ColumnDef col;
      auto field = schema_->field(name);
      col.name_ref() = name;
      ::nebula::meta::cpp2::ColumnTypeDef type;
      if (length > 0) {
        type.type_length_ref() = length;
        type.type_ref() = ::nebula::cpp2::PropertyType::FIXED_STRING;
      } else {
        type.type_ref() = field->type();
      }
      col.type_ref() = type;
      col.nullable_ref() = field->nullable();
      fields.emplace_back(std::move(col));
    }
    ret->fields_ref() = fields;
    return ret;
  }

 private:
  std::stringstream ss;
  std::string schemaName_;
  ::nebula::cpp2::SchemaID schemaId_;
  std::shared_ptr<SchemaProvider> schema_;
  std::shared_ptr<::nebula::meta::cpp2::IndexItem> index_;
};

// Definition of UDL
std::vector<Row> operator""_row(const char* str, std::size_t len) {
  auto ret = RowParser(std::string(str, len)).getResult();
  return ret;
}
std::shared_ptr<::nebula::meta::NebulaSchemaProvider> operator"" _schema(const char* str,
                                                                         std::size_t) {
  return SchemaParser(std::string(str)).getResult();
}

IndexParser operator"" _index(const char* str, std::size_t) {
  return IndexParser(std::string(str));
}

}  // namespace storage
}  // namespace nebula
#endif
