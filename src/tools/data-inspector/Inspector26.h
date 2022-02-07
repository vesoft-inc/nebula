/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef TOOLS_DATA_INSPECTOR_INSPECTOR26_H_
#define TOOLS_DATA_INSPECTOR_INSPECTOR26_H_

#include "tools/data-inspector/Inspector.h"

namespace nebula {

class Inspector26 : public Inspector {
public:
  explicit Inspector26(rocksdb::DB* db) : Inspector(db) {}

  void info() const override;
  void stats() const override;
  void dump() const override;

protected:
  std::string parseKey(rocksdb::Slice key) const override;

private:
  std::string parseVertexKey(rocksdb::Slice key) const;
  std::string parseEdgeKey(rocksdb::Slice key) const;
  std::string parseSystemKey(rocksdb::Slice key) const;
  std::string parseKVKey(rocksdb::Slice key) const;
};

}  // namespace nebula
#endif  // TOOLS_DATA_INSPECTOR_INSPECTOR26_H_

