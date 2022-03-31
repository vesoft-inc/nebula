// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/CharsetExecutor.h"

namespace nebula {
namespace graph {
folly::Future<Status> ShowCharsetExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  DataSet dataSet({"Charset", "Description", "Default collation", "Maxlen"});

  auto charsetDesc = qctx()->getCharsetInfo()->getCharsetDesc();

  for (auto &charSet : charsetDesc) {
    Row row;
    row.values.resize(4);
    row.values[0].setStr(charSet.second.charsetName_);
    row.values[1].setStr(charSet.second.desc_);
    row.values[2].setStr(charSet.second.defaultColl_);
    row.values[3].setInt(charSet.second.maxLen_);
    dataSet.emplace_back(std::move(row));
  }

  return finish(ResultBuilder().value(Value(std::move(dataSet))).build());
}

folly::Future<Status> ShowCollationExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  DataSet dataSet({"Collation", "Charset"});

  auto charsetDesc = qctx()->getCharsetInfo()->getCharsetDesc();

  for (auto &charSet : charsetDesc) {
    for (auto &coll : charSet.second.supportColls_) {
      Row row;
      row.values.resize(2);
      row.values[0].setStr(coll);
      row.values[1].setStr(charSet.second.charsetName_);
      dataSet.emplace_back(std::move(row));
    }
  }

  return finish(ResultBuilder().value(Value(std::move(dataSet))).build());
}
}  // namespace graph
}  // namespace nebula
