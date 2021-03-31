/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <chrono>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>

#include "codec/RowWriterV2.h"
#include "common/base/Base.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/clients/storage/InternalStorageClient.h"
#include "common/expression/ConstantExpression.h"
#include "common/meta/SchemaManager.h"
#include "storage/transaction/TransactionUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

struct TossTestUtils {
    static std::vector<nebula::Value> genSingleVal(int n) {
        boost::uuids::random_generator          gen;
        std::vector<nebula::Value> ret(2);
        ret[0].setInt(n);
        ret[1].setStr(boost::uuids::to_string(gen()));
        return ret;
    }

    static std::vector<std::vector<nebula::Value>> genValues(size_t num) {
        boost::uuids::random_generator          gen;
        std::vector<std::vector<nebula::Value>> ret(num);
        for (auto i = 0U; i != num; ++i) {
            ret[i].resize(2);
            int32_t n = 1024*(1+i);
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
        return ret.substr(0, ret.size()-1);
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
        (*ret.key_ref()).set_src(
                std::string(reinterpret_cast<const char*>(&e.get_key().get_src().getInt()), 8));
        (*ret.key_ref()).set_dst(
                std::string(reinterpret_cast<const char*>(&e.get_key().get_dst().getInt()), 8));
        return ret;
    }

    static cpp2::EdgeKey toVidKey(const cpp2::EdgeKey& input) {
        cpp2::EdgeKey ret(input);
        ret.set_src(std::string(reinterpret_cast<const char*>(&input.get_src().getInt()), 8));
        ret.set_dst(std::string(reinterpret_cast<const char*>(&input.get_dst().getInt()), 8));
        return ret;
    }
};  // end TossTestUtils

}  // namespace storage
}  // namespace nebula
