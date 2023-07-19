/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TEST_TOSSTESTUTILS_H
#define STORAGE_TEST_TOSSTESTUTILS_H

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>

#include "clients/storage/InternalStorageClient.h"
#include "clients/storage/StorageClient.h"
#include "codec/RowWriterV2.h"
#include "common/base/Base.h"
#include "common/expression/ConstantExpression.h"
#include "common/meta/SchemaManager.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "storage/transaction/TransactionUtils.h"

namespace nebula {
namespace storage {

struct TossTestUtils {
  static std::vector<nebula::Value> genSingleVal(int n) {
    boost::uuids::random_generator gen;
    std::vector<nebula::Value> ret(2);
    ret[0].setInt(n);
    ret[1].setStr(boost::uuids::to_string(gen()));
    return ret;
  }

  static std::vector<std::vector<nebula::Value>> genValues(size_t num) {
    boost::uuids::random_generator gen;
    std::vector<std::vector<nebula::Value>> ret(num);
    for (auto i = 0U; i != num; ++i) {
      ret[i].resize(2);
      int32_t n = 1024 * (1 + i);
      ret[i][0].setInt(n);
      ret[i][1].setStr(boost::uuids::to_string(gen()));
    }
    return ret;
  }

  static std::string dumpDataSet(const DataSet& ds) {
    std::stringstream oss;
    for (auto&& it : folly::enumerate(ds.colNames)) {
      oss << "\ncolNames[" << it.index << "]=" << *it;
    }
    oss << "\n";

    oss << dumpRows(ds.rows);
    return oss.str();
  }

  static std::string concatValues(const std::vector<nebula::Value>& vals) {
    if (vals.empty()) {
      return "";
    }
    std::ostringstream oss;
    for (auto& val : vals) {
      oss << val << ',';
    }
    std::string ret = oss.str();
    return ret.substr(0, ret.size() - 1);
  }

  static std::string dumpValues(const std::vector<Value>& vals) {
    std::stringstream oss;
    oss << "vals.size() = " << vals.size() << "\n";
    for (auto& val : vals) {
      oss << val.toString() << "\n";
    }
    return oss.str();
  }

  static std::string dumpRows(const std::vector<Row>& rows) {
    std::stringstream oss;
    oss << "rows.size() = " << rows.size() << "\n";
    for (auto& row : rows) {
      oss << "row.size()=" << row.size() << "\n";
      oss << row.toString() << "\n";
    }
    return oss.str();
  }

  static std::string hexVid(int64_t vid) {
    std::string str(reinterpret_cast<char*>(&vid), sizeof(int64_t));
    return folly::hexlify(str);
  }

  static std::string hexEdgeId(const cpp2::EdgeKey& ek) {
    return hexVid(ek.get_src().getInt()) + hexVid(ek.get_dst().getInt());
  }

  static std::vector<std::string> splitNeiResults(std::vector<std::string>& svec) {
    std::vector<std::string> ret;
    for (auto& str : svec) {
      auto sub = splitNeiResult(str);
      ret.insert(ret.end(), sub.begin(), sub.end());
    }
    return ret;
  }

  static bool compareSize(const std::vector<std::string>& svec, size_t expect) {
    auto equalSize = svec.size() == expect;
    if (!equalSize) {
      LOG(INFO) << "compareSize failed: expect=" << expect;
      print_svec(svec);
    }
    return equalSize;
  }

  static std::vector<std::string> splitNeiResult(folly::StringPiece str) {
    std::vector<std::string> ret;
    auto begin = str.begin();
    auto end = str.end();
    if (str.startsWith("[[")) {
      begin++;
      begin++;
    }
    if (str.endsWith("]]")) {
      end--;
      end--;
    }
    str.assign(begin, end);
    folly::split("],[", str, ret);
    if (ret.size() == 1U && ret.back() == "__EMPTY__") {
      ret.clear();
    }
    return ret;
  }

  static void print_svec(const std::vector<std::string>& svec) {
    LOG(INFO) << "svec.size()=" << svec.size();
    for (auto& str : svec) {
      LOG(INFO) << str;
    }
  }

  static cpp2::NewEdge toVertexIdEdge(const cpp2::NewEdge& e) {
    cpp2::NewEdge ret(e);
    (*ret.key_ref())
        .set_src(std::string(reinterpret_cast<const char*>(&e.get_key().get_src().getInt()), 8));
    (*ret.key_ref())
        .set_dst(std::string(reinterpret_cast<const char*>(&e.get_key().get_dst().getInt()), 8));
    return ret;
  }

  static cpp2::EdgeKey makeEdgeKeyS(const cpp2::EdgeKey& input) {
    if (input.src.type() == Value::Type::STRING) {
      return input;
    }
    cpp2::EdgeKey ret(input);
    ret.set_src(std::string(reinterpret_cast<const char*>(&input.get_src().getInt()), 8));
    ret.set_dst(std::string(reinterpret_cast<const char*>(&input.get_dst().getInt()), 8));
    return ret;
  }

  static std::vector<nebula::Value> makeISValue(int64_t iVal) {
    boost::uuids::random_generator gen;
    std::vector<nebula::Value> vals(2);
    vals[0].setInt(iVal);
    vals[1].setStr(boost::uuids::to_string(gen()));
    return vals;
  }

  // generate a vector of values, 1st is ant i64, 2nd is a random string.
  static std::vector<std::vector<nebula::Value>> genISValues(size_t num) {
    std::vector<std::vector<nebula::Value>> ret;
    for (auto i = 0U; i != num; ++i) {
      int32_t n = 1024 * (1 + i);
      ret.emplace_back(makeISValue(n));
    }
    return ret;
  }

  // generate num different edges with same dst
  // the first src is dst + 1, and increase 1 for each
  static std::vector<cpp2::NewEdge> makeNeighborEdges(int64_t dst, int edgeType, size_t num) {
    auto values = genISValues(num);
    std::vector<cpp2::NewEdge> edges;
    auto rank = 0;
    for (auto i = 0U; i < num; ++i) {
      auto src = dst + i + 1;
      auto ekey = makeEdgeKeyI(src, edgeType, rank, dst);
      edges.emplace_back();
      edges.back().set_key(std::move(ekey));
      edges.back().set_props(std::move(values[i]));
    }
    return edges;
  }

  static cpp2::NewEdge makeEdge(int64_t src, int edgeType) {
    cpp2::NewEdge edge;
    edge.set_key(makeEdgeKeyI(src, edgeType, 0, src + 1));
    edge.set_props(makeISValue(1024));
    return edge;
  }

  static cpp2::NewEdge makeEdgeS(int64_t src, int edgeType) {
    cpp2::NewEdge edge = makeEdge(src, edgeType);
    edge.key = makeEdgeKeyS(edge.key);
    return edge;
  }

  static cpp2::NewEdge makeTwinEdge(const cpp2::NewEdge& oldEdge) {
    cpp2::NewEdge newEdge(oldEdge);
    auto newVal = makeISValue(newEdge.props[0].getInt() + 1024);
    newEdge.set_props(newVal);
    return newEdge;
  }

  static std::vector<std::string> makeColNames(size_t n) {
    std::vector<std::string> colNames;
    for (auto i = 0U; i < n; ++i) {
      colNames.emplace_back(folly::sformat("c{}", i + 1));
    }
    return colNames;
  }

  static std::vector<meta::cpp2::ColumnDef> makeColDefs(
      const std::vector<nebula::cpp2::PropertyType>& types) {
    auto N = types.size();
    auto colNames = makeColNames(N);
    std::vector<meta::cpp2::ColumnDef> columnDefs(N);
    for (auto i = 0U; i != N; ++i) {
      columnDefs[i].set_name(colNames[i]);
      meta::cpp2::ColumnTypeDef colTypeDef;
      colTypeDef.set_type(types[i]);
      columnDefs[i].set_type(colTypeDef);
      columnDefs[i].set_nullable(true);
    }
    return columnDefs;
  }
};  // end TossTestUtils

}  // namespace storage
}  // namespace nebula
#endif
