/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef TOOLS_DATA_INSPECTOR_INSPECTOR_H_
#define TOOLS_DATA_INSPECTOR_INSPECTOR_H_

#include "common/base/Base.h"
#include "rocksdb/db.h"

namespace nebula {

class Inspector {
public:
  explicit Inspector(rocksdb::DB* db) : db_(db) {}
  virtual ~Inspector();

  virtual void info() const = 0;
  virtual void stats() const = 0;
  virtual void dump() const = 0;

  static std::unique_ptr<Inspector> getInspector();

protected:
  rocksdb::DB* db_;

  static rocksdb::DB* openDB();

  std::string decodeHexStr(const char* start, size_t len) const;

  std::string getPrefix() const;

  // field_name => value
  std::unordered_map<std::string, std::string> preparePrefix() const;

  std::string processPrefixInt8(const std::string& valStr, int8_t& val) const;
  std::string processPrefixInt32(const std::string& valStr, int32_t& val) const;
  std::string processPrefixInt64(const std::string& valStr, int64_t& val) const;
  std::string processPrefixHex(const std::string& valStr) const;

  // Each version will implement its own parsing logic
  virtual std::string parseKey(rocksdb::Slice key) const = 0;
};

}  // namespace nebula
#endif  // TOOLS_DATA_INSPECTOR_INSPECTOR_H_

