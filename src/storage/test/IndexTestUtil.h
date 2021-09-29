/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <iostream>
#include <map>
#include <regex>

#include "common/datatypes/DataSet.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "folly/String.h"
#include "kvstore/KVIterator.h"
#include "kvstore/KVStore.h"
#include "storage/exec/IndexNode.h"
namespace nebula {
namespace storage {
using ::nebula::kvstore::KVIterator;
class MockKVIterator : public KVIterator {
  using KVMap = std::map<std::string, std::string>;

 public:
  MockKVIterator(const KVMap& kv_, KVMap::iterator&& iter);
  bool valid() const { return iter_ != kv_.end(); }
  void next() { iter_++; }
  void prev() { iter_--; }
  folly::StringPiece key() const { return folly::StringPiece(iter_->first); }
  folly::StringPiece val() const { return folly::StringPiece(iter_->second); }
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
  GraphSpaceID spaceId_;
  std::map<std::string, std::string> kv_;

 public:
  // Return bit-OR of StoreCapability values;
  uint32_t capability() const override {
    assert(false);
    return 0;
  };
  void stop() {}
  ErrorOr<nebula::cpp2::ErrorCode, HostAddr> partLeader(GraphSpaceID spaceId, PartitionID partID) {
    UNUSED(spaceId), UNUSED(partID);
    assert(false);
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  // Read a single key
  nebula::cpp2::ErrorCode get(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& key,
                              std::string* value,
                              bool canReadFromFollower = false) override {
    UNUSED(canReadFromFollower);
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
      GraphSpaceID spaceId,
      PartitionID partId,
      const std::vector<std::string>& keys,
      std::vector<std::string>* values,
      bool canReadFromFollower = false) override {
    UNUSED(canReadFromFollower);
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
                                PartitionID partId,
                                const std::string& start,
                                const std::string& end,
                                std::unique_ptr<KVIterator>* iter,
                                bool canReadFromFollower = false) override {
    CHECK_EQ(spaceId, spaceId_);
    auto it = kv_.lower_bound(start);
    auto mockIter = std::make_unique<MockKVIterator>(kv_, std::move(it));
    mockIter->setValidFunc([start, end](const decltype(kv_)::iterator& iter) {
      if (start <= iter->first && iter->first < end) {
        return true;
      } else {
        return false;
      }
    });
    (*iter) = std::move(mockIter);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode prefix(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 const std::string& prefix,
                                 std::unique_ptr<KVIterator>* iter,
                                 bool canReadFromFollower = false) override {
    UNUSED(canReadFromFollower);
    CHECK_EQ(spaceId, spaceId_);
    auto it = kv_.lower_bound(prefix);
    auto mockIter = std::make_unique<MockKVIterator>(kv_, std::move(it));
    mockIter->setValidFunc([prefix](const decltype(kv_)::iterator& iter) {
      if (iter->first.size() < prefix.size()) {
        return false;
      }
      for (size_t i = 0; i < prefix.size(); i++) {
        if (prefix[i] != iter->first[i]) {
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
    CHECK_EQ(spaceId, spaceId_);
    auto it = kv_.lower_bound(start);
    auto mockIter = std::make_unique<MockKVIterator>(kv_, std::move(it));
    mockIter->setValidFunc([prefix](const decltype(kv_)::iterator& iter) {
      if (iter->first.size() < prefix.size()) {
        return false;
      }
      for (size_t i = 0; i < prefix.size(); i++) {
        if (prefix[i] != iter->first[i]) {
          return false;
        }
      }
      return true;
    });
    (*iter) = std::move(mockIter);
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode sync(GraphSpaceID spaceId, PartitionID partId) {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  void asyncMultiPut(GraphSpaceID spaceId,
                     PartitionID partId,
                     std::vector<::nebula::kvstore::KV>&& keyValues,
                     ::nebula::kvstore::KVCallback cb) override {
    for (size_t i = 0; i < keyValues.size(); i++) {
      kv_.emplace(std::move(keyValues[i]));
    }
  }

  // Asynchronous version of remove methods
  void asyncRemove(GraphSpaceID spaceId,
                   PartitionID partId,
                   const std::string& key,
                   ::nebula::kvstore::KVCallback cb) override {
    kv_.erase(key);
  }

  void asyncMultiRemove(GraphSpaceID spaceId,
                        PartitionID partId,
                        std::vector<std::string>&& keys,
                        ::nebula::kvstore::KVCallback cb) override {
    for (size_t i = 0; i < keys.size(); i++) {
      kv_.erase(keys[i]);
    }
  }

  void asyncRemoveRange(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::string& start,
                        const std::string& end,
                        ::nebula::kvstore::KVCallback cb) override {
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
                     raftex::AtomicOp op,
                     ::nebula::kvstore::KVCallback cb) override {
    LOG(FATAL) << "Unexpect";
  }
  void asyncAppendBatch(GraphSpaceID spaceId,
                        PartitionID partId,
                        std::string&& batch,
                        ::nebula::kvstore::KVCallback cb) override {
    LOG(FATAL) << "Unexpect";
  }
  nebula::cpp2::ErrorCode ingest(GraphSpaceID spaceId) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  int32_t allLeader(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderIds) override {
    LOG(FATAL) << "Unexpect";
    return 0;
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<::nebula::kvstore::Part>> part(
      GraphSpaceID spaceId, PartitionID partId) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  nebula::cpp2::ErrorCode compact(GraphSpaceID spaceId) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  nebula::cpp2::ErrorCode flush(GraphSpaceID spaceId) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<::nebula::cpp2::CheckpointInfo>> createCheckpoint(
      GraphSpaceID spaceId, const std::string& name) override {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  };
  nebula::cpp2::ErrorCode dropCheckpoint(GraphSpaceID spaceId, const std::string& name) {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  nebula::cpp2::ErrorCode setWriteBlocking(GraphSpaceID spaceId, bool sign) {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupTable(
      GraphSpaceID spaceId,
      const std::string& name,
      const std::string& tablePrefix,
      std::function<bool(const folly::StringPiece& key)> filter) {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  // for meta BR
  nebula::cpp2::ErrorCode restoreFromFiles(GraphSpaceID spaceId,
                                           const std::vector<std::string>& files) {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  nebula::cpp2::ErrorCode multiPutWithoutReplicator(GraphSpaceID spaceId,
                                                    std::vector<::nebula::kvstore::KV> keyValues) {
    LOG(FATAL) << "Unexpect";
    return ::nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  std::vector<std::string> getDataRoot() const {
    LOG(FATAL) << "Unexpect";
    return {};
  }
};
class MockIndexNode : public IndexNode {
 public:
  ::nebula::cpp2::ErrorCode init(InitContext& initCtx) { return initFunc(initCtx); }
  std::unique_ptr<IndexNode> copy() { LOG(FATAL) << "Unexpect"; }
  std::function<ErrorOr<Row>(bool&)> nextFunc;
  std::function<::nebula::cpp2::ErrorCode(PartitionID)> executeFunc;
  std::function<::nebula::cpp2::ErrorCode(InitContext& initCtx)> initFunc;

 private:
  ErrorOr<Row> doNext(bool& hasNext) override { return nextFunc(hasNext); }
  ::nebula::cpp2::ErrorCode doExecute(PartitionID partId) override { return executeFunc(partId); };
};

class RowParser {
 public:
  explicit RowParser(const std::string& str) {
    ss = std::stringstream(folly::stripLeftMargin(str));
    ss = std::stringstream(str);
    parseHeader();
    parseRow();
  }
  void parseHeader() {
    std::string line;
    std::getline(ss, line);
    std::vector<std::string> types;
    folly::split("|", line, types);
    for (size_t i = 0; i < types.size(); i++) {
      types[i] = folly::stripLeftMargin(types[i]);
    }
    typeList_ = std::move(types);
  }
  void parseRow() {
    std::string line;
    while (std::getline(ss, line)) {
      std::vector<std::string> values;
      folly::split("|", line, values);
      for (size_t i = 0; i < values.size(); i++) {
        values[i] = folly::stripLeftMargin(values[i]);
      }
      Row row;
      for (size_t i = 0; i < values.size(); i++) {
        if (values[i] == "<null>") {
          row.emplace_back(Value::null());
        } else {
          row.emplace_back(transformMap[typeList_[i]](values[i]));
        }
      }
      rowList_.emplace_back(std::move(row));
    }
  }
  const std::vector<Row>& getResult() { return rowList_; }

 private:
  std::stringstream ss;
  std::vector<std::string> typeList_;
  std::vector<Row> rowList_;
  std::map<std::string, std::function<Value(const std::string& str)>> transformMap{
      {"int64", [](const std::string& str) { return Value(std::stoi(str)); }},
      {"string", [](const std::string& str) { return Value(str); }},
      {"double", [](const std::string& str) { return Value(std::stof(str)); }}};
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
 *    c | fix_string | 10 |
 * )"_schema
 */
class SchemaParser {
 public:
  explicit SchemaParser(const std::string& str) {
    ss = std::stringstream(folly::stripLeftMargin(str));
    parse();
  }
  void parse() {
    std::string line;
    while (std::getline(ss, line)) {
      std::vector<std::string> values;
      folly::split("|", line, values);
      std::string name = folly::stripLeftMargin(values[0]);
      auto type = typeMap[folly::stripLeftMargin(values[1])];
      int length = 0;
      {
        std::string lenStr = folly::stripLeftMargin(values[2]);
        if (lenStr != "") {
          length = std::stoi(lenStr);
        }
      }
      bool nullable = false;
      {
        std::string nullable = folly::stripLeftMargin(values[3]);
        if (nullable == "true") {
          nullable = true;
        }
      }
      schema->addField(name, type, length, nullable);
    }
  }
  std::shared_ptr<::nebula::meta::NebulaSchemaProvider> getResult() { return schema; }

 private:
  std::stringstream ss;
  std::shared_ptr<::nebula::meta::NebulaSchemaProvider> schema;
  std::map<std::string, ::nebula::meta::cpp2::PropertyType> typeMap;
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
    ss = std::stringstream(folly::stripLeftMargin(str));
    parseSchema();
  }
  void parseSchema() {
    static std::regex pattern(R"((TAG|EDGE)\((.+),(\d+)\))");
    std::smatch match;
    std::string line;
    std::getline(ss, line);
    assert(std::regex_match(line, match, pattern));
    std::string name = match.str(2);
    int32_t id = std::stoi(match.str(3));
    schemaName_ = name;
    if (match.str(1) == "TAG") {
      schemaId_.set_tag_id(id);
    } else {
      schemaId_.set_edge_type(id);
    }
  }
  std::map<IndexID, std::shared_ptr<IndexItem>> operator()(std::shared_ptr<SchemaProvider> schema) {
    schema_ = schema;
    std::map<IndexID, std::shared_ptr<IndexItem>> ret;
    std::string line;
    while (std::getline(ss, line)) {
      auto index = parse(line);
      ret[index->get_index_id()] = index;
    }
    return ret;
  }
  std::shared_ptr<IndexItem> parse(const std::string& line) {
    auto ret = std::make_shared<IndexItem>();
    ret->set_schema_id(schemaId_);
    ret->set_schema_name(schemaName_);
    static std::regex pattern(R"(\((.+),(\d+)\):(.+))");
    std::smatch match;
    assert(std::regex_match(line, match, pattern));
    ret->set_index_name(folly::stripLeftMargin(match.str(1)));
    ret->set_index_id(std::stoi(match.str(2)));
    std::string columnStr = match.str(3);
    std::vector<std::string> columns;
    folly::split(",", columnStr, columns);
    for (size_t i = 0; i < columns.size(); i++) {
      columns[i] = folly::stripLeftMargin(columns[i]);
    }
    std::vector<::nebula::meta::cpp2::ColumnDef> fields;
    for (auto& column : columns) {
      std::string name;
      int length;
      std::smatch match;
      std::regex pattern(R"((.+)\((\d+)\))");
      if (std::regex_match(column, match, pattern)) {
        name = match.str(1);
        length = std::stoi(match.str(2));
      } else {
        name = column;
        length = 0;
      }
      ::nebula::meta::cpp2::ColumnDef col;
      auto field = schema_->field(name);
      col.set_name(name);
      ::nebula::meta::cpp2::ColumnTypeDef type;
      if (length > 0) {
        type.set_type_length(length);
      }
      type.set_type(field->type());
      col.set_type(type);
      col.set_nullable(field->nullable());
      fields.emplace_back(std::move(col));
    }
    ret->set_fields(fields);
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
std::vector<Row> operator""_row(const char* str, std::size_t) {
  auto ret = RowParser(std::string(str)).getResult();
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
