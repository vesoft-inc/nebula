
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

#define FLOG_FMT(...) LOG(INFO) << folly::sformat(__VA_ARGS__)

DECLARE_int32(heartbeat_interval_secs);
DEFINE_string(meta_server, "192.168.8.5:6500", "Meta servers' address.");

namespace nebula {
namespace storage {

const std::string kMetaName = "hp-server";  // NOLINT
constexpr int32_t kMetaPort = 6500;
const std::string kSpaceName = "test";   // NOLINT
constexpr int32_t kPart = 5;
constexpr int32_t kReplica = 3;
constexpr int32_t kSum = 10000 * 10000;

using StorageClient = storage::GraphStorageClient;

struct TossTestUtils {
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
        return hexVid(ek.src.getInt()) + hexVid(ek.dst.getInt());
    }

    static std::vector<std::string> splitNeiResults(std::vector<std::string>& svec) {
        std::vector<std::string> ret;
        for (auto& str : svec) {
            auto sub = splitNeiResult(str);
            ret.insert(ret.end(), sub.begin(), sub.end());
        }
        return ret;
    }

    static void logIfSizeNotAsExpect(const std::vector<std::string>& svec, size_t expect) {
        if (svec.size() != expect) {
            print_svec(svec);
        }
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
};  // end TossTestUtils

struct TossEnvironment {
    static TossEnvironment* getInstance(const std::string& metaName, int32_t metaPort) {
        static TossEnvironment inst(metaName, metaPort);
        return &inst;
    }

    TossEnvironment(const std::string& metaName, int32_t metaPort) {
        executor_ = std::make_shared<folly::IOThreadPoolExecutor>(20);
        mClient_ = setupMetaClient(metaName, metaPort);
        sClient_ = std::make_unique<StorageClient>(executor_, mClient_.get());
        interClient_ = std::make_unique<storage::InternalStorageClient>(executor_, mClient_.get());
        schemaMan_ = meta::SchemaManager::create(mClient_.get());
    }

    std::unique_ptr<meta::MetaClient>
    setupMetaClient(const std::string& metaName, uint32_t metaPort) {
        std::vector<HostAddr> metas;
        metas.emplace_back(HostAddr(metaName, metaPort));
        meta::MetaClientOptions options;
        auto client = std::make_unique<meta::MetaClient>(executor_, metas, options);
        if (!client->waitForMetadReady()) {
            LOG(FATAL) << "!client->waitForMetadReady()";
        }
        return client;
    }

    /*
     * setup space and edge type, then describe the cluster
     * */
    void init(const std::string& spaceName,
              size_t part,
              int replica,
              std::vector<meta::cpp2::ColumnDef>& colDefs) {
        if (spaceId_ != 0) {
            return;
        }
        spaceId_ = setupSpace(spaceName, part, replica);
        edgeType_ = setupEdgeSchema("test_edge", colDefs);

        int sleepSecs = FLAGS_heartbeat_interval_secs + 2;
        while (sleepSecs) {
            LOG(INFO) << "sleep for " << sleepSecs-- << " sec";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        bool leaderLoaded = false;
        while (!leaderLoaded) {
            auto statusOrLeaderMap = mClient_->loadLeader();
            if (!statusOrLeaderMap.ok()) {
                LOG(FATAL) << "mClient_->loadLeader() failed!!!!!!";
            }

            for (auto& leader : statusOrLeaderMap.value()) {
                LOG(INFO) << "spaceId=" << leader.first.first
                            << ", part=" << leader.first.second
                            << ", host=" << leader.second;
                if (leader.first.first == spaceId_) {
                    leaderLoaded = true;
                }
            }
        }
    }

    int32_t getSpaceId() { return spaceId_; }

    int setupSpace(const std::string& spaceName, int nPart, int nReplica) {
        auto fDropSpace = mClient_->dropSpace(spaceName, true);
        fDropSpace.wait();
        LOG(INFO) << "drop space " << spaceName;

        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name(spaceName);
        spaceDesc.set_partition_num(nPart);
        spaceDesc.set_replica_factor(nReplica);
        meta::cpp2::ColumnTypeDef colType;
        colType.set_type(meta::cpp2::PropertyType::INT64);
        spaceDesc.set_vid_type(colType);
        spaceDesc.set_isolation_level(meta::cpp2::IsolationLevel::TOSS);

        auto fCreateSpace = mClient_->createSpace(spaceDesc, true);
        fCreateSpace.wait();
        if (!fCreateSpace.valid()) {
            LOG(FATAL) << "!fCreateSpace.valid()";
        }
        if (!fCreateSpace.value().ok()) {
            LOG(FATAL) << "!fCreateSpace.value().ok(): "
                       << fCreateSpace.value().status().toString();
        }
        auto spaceId = fCreateSpace.value().value();
        LOG(INFO) << folly::sformat("spaceId = {}", spaceId);
        return spaceId;
    }

    EdgeType setupEdgeSchema(const std::string& edgeName,
                             std::vector<meta::cpp2::ColumnDef> columns) {
        meta::cpp2::Schema schema;
        schema.set_columns(std::move(columns));

        auto fCreateEdgeSchema = mClient_->createEdgeSchema(spaceId_, edgeName, schema, true);
        fCreateEdgeSchema.wait();

        if (!fCreateEdgeSchema.valid() || !fCreateEdgeSchema.value().ok()) {
            LOG(FATAL) << "createEdgeSchema failed";
        }
        return fCreateEdgeSchema.value().value();
    }

    cpp2::EdgeKey generateEdgeKey(int64_t srcId, int rank, int dstId = 0) {
        cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);

        edgeKey.set_edge_type(edgeType_);
        edgeKey.set_ranking(rank);
        if (dstId == 0) {
            dstId = kSum - srcId;
        }
        edgeKey.set_dst(dstId);
        return edgeKey;
    }

    cpp2::NewEdge generateEdge(int srcId,
                               int rank,
                               std::vector<nebula::Value> values,
                               int dstId = 0) {
        cpp2::NewEdge newEdge;

        cpp2::EdgeKey edgeKey = generateEdgeKey(srcId, rank, dstId);
        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(values));

        return newEdge;
    }

    cpp2::NewEdge reverseEdge(const cpp2::NewEdge& e0) {
        cpp2::NewEdge e(e0);
        std::swap(e.key.src, e.key.dst);
        e.key.edge_type *= -1;
        return e;
    }

    cpp2::ErrorCode syncAddMultiEdges(std::vector<cpp2::NewEdge>& edges, bool useToss) {
        bool retLeaderChange = false;
        int32_t retry = 0;
        int32_t retryMax = 10;
        do {
            if (retry > 0 && retry != retryMax) {
                LOG(INFO) << "\n leader changed, retry=" << retry;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                retLeaderChange = false;
            }
            auto f = addEdgesAsync(edges, useToss);
            f.wait();
            if (!f.valid()) {
                return cpp2::ErrorCode::E_UNKNOWN;
            }
            if (!f.value().succeeded()) {
                LOG(INFO) << "addEdgeAsync() !f.value().succeeded()";
            }

            std::vector<cpp2::ExecResponse>& execResps = f.value().responses();
            for (auto& execResp : execResps) {
                // ResponseCommon
                auto& respComn = execResp.result;
                auto& failedParts = respComn.get_failed_parts();
                for (auto& part : failedParts) {
                    if (part.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                        retLeaderChange = true;
                    }
                }
            }

            auto parts = f.value().failedParts();
            for (auto& part : parts) {
                if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    retLeaderChange = true;
                    LOG(INFO) << "E_LEADER_CHANGED retry";
                    break;
                } else {
                    auto icode = static_cast<int32_t>(part.second);
                    FLOG_FMT("failed: (space,part)=({},{}), code {}", spaceId_, part.first, icode);
                }
            }
            if (++retry == retryMax) {
                break;
            }
        } while (retLeaderChange);
        return cpp2::ErrorCode::SUCCEEDED;
    }

    cpp2::ErrorCode syncAddEdge(const cpp2::NewEdge& edge, bool useToss = true) {
        std::vector<cpp2::NewEdge> edges{edge};
        return syncAddMultiEdges(edges, useToss);
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
    addEdgesAsync(const std::vector<cpp2::NewEdge>& edges, bool useToss = true) {
        auto propNames = makeColNames(edges.back().props.size());
        return sClient_->addEdges(spaceId_, edges, propNames, true, nullptr, useToss);
    }

    static std::vector<std::string> makeColNames(size_t n) {
        std::vector<std::string> colNames;
        for (auto i = 0U; i < n; ++i) {
            colNames.emplace_back(folly::sformat("c{}", i+1));
        }
        return colNames;
    }

    std::vector<Value> getProps(cpp2::NewEdge edge) {
        // nebula::DataSet ds;  ===> will crash if not set
        nebula::DataSet ds({kSrc, kType, kRank, kDst});
        std::vector<Value> edgeInfo{edge.key.src,
                                    edge.key.edge_type,
                                    edge.key.ranking,
                                    edge.key.dst};
        List row(std::move(edgeInfo));
        ds.rows.emplace_back(std::move(row));

        std::vector<cpp2::EdgeProp> props;
        cpp2::EdgeProp oneProp;
        oneProp.type = edge.key.edge_type;
        props.emplace_back(oneProp);

        auto frpc = sClient_->getProps(spaceId_,
                                       std::move(ds), /*DataSet*/
                                       nullptr, /*vector<cpp2::VertexProp>*/
                                       &props, /*vector<cpp2::EdgeProp>*/
                                       nullptr /*expressions*/).via(executor_.get());
        frpc.wait();
        if (!frpc.valid()) {
            LOG(FATAL) << "getProps rpc invalid()";
        }
        StorageRpcResponse<cpp2::GetPropResponse>& rpcResp = frpc.value();

        LOG(INFO) << "rpcResp.succeeded()=" << rpcResp.succeeded()
                  << ", responses().size()=" << rpcResp.responses().size()
                  << ", failedParts().size()=" << rpcResp.failedParts().size();

        auto resps = rpcResp.responses();
        if (resps.empty()) {
            LOG(FATAL) << "getProps() resps.empty())";
        }
        cpp2::GetPropResponse& propResp = resps.front();
        cpp2::ResponseCommon result = propResp.result;
        std::vector<cpp2::PartitionResult>& fparts = result.failed_parts;
        if (!fparts.empty()) {
            for (cpp2::PartitionResult& res : fparts) {
                LOG(INFO) << "part_id: " << res.part_id
                          << ", part leader " << res.leader
                          << ", code " << static_cast<int>(res.code);
            }
            LOG(FATAL) << "getProps() !failed_parts.empty())";
        }
        nebula::DataSet& dataSet = propResp.props;
        std::vector<Row>& rows = dataSet.rows;
        if (rows.empty()) {
            LOG(FATAL) << "getProps() dataSet.rows.empty())";
        }
        std::vector<Value>& values = rows[0].values;
        if (values.empty()) {
            LOG(FATAL) << "getProps() values.empty())";
        }
        return values;
    }

    folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>
    getNeighborsWrapper(const std::vector<cpp2::NewEdge>& edges,
                        int64_t limit = std::numeric_limits<int64_t>::max()) {
        // para3
        std::vector<Row> vertices;
        std::set<Value> vids;
        for (auto& e : edges) {
            vids.insert(e.key.src);
        }
        for (auto& vid : vids) {
            Row row;
            row.emplace_back(vid);
            vertices.emplace_back(row);
        }
        // para 4
        std::vector<EdgeType> edgeTypes;
        // para 5
        cpp2::EdgeDirection edgeDirection = cpp2::EdgeDirection::BOTH;
        // para 6
        std::vector<cpp2::StatProp>* statProps = nullptr;
        // para 7
        std::vector<cpp2::VertexProp>* vertexProps = nullptr;
        // para 8
        const std::vector<cpp2::EdgeProp> edgeProps;
        // para 9
        const std::vector<cpp2::Expr>* expressions = nullptr;
        // para 10
        bool dedup = false;
        // para 11
        bool random = false;
        // para 12
        const std::vector<cpp2::OrderBy> orderBy = std::vector<cpp2::OrderBy>();

        auto colNames = makeColNames(edges.back().props.size());

        return sClient_->getNeighbors(spaceId_,
                                      colNames,
                                      vertices,
                                      edgeTypes,
                                      edgeDirection,
                                      statProps,
                                      vertexProps,
                                      &edgeProps,
                                      expressions,
                                      dedup,
                                      random,
                                      orderBy,
                                      limit);
    }

    std::vector<std::string> getNeiProps(const std::vector<cpp2::NewEdge>& edges,
                                         int64_t limit = std::numeric_limits<int64_t>::max()) {
        bool retLeaderChange = false;
        do {
            auto f = getNeighborsWrapper(edges, limit);
            f.wait();
            if (!f.valid()) {
                LOG(ERROR) << "!f.valid()";
                break;
            }
            if (f.value().succeeded()) {
                return extractMultiRpcResp(f.value());
            } else {
                LOG(ERROR) << "!f.value().succeeded()";
            }
            auto parts = f.value().failedParts();
            for (auto& part : parts) {
                if (part.second == cpp2::ErrorCode::E_LEADER_CHANGED) {
                    retLeaderChange = true;
                    break;
                }
            }
        } while (retLeaderChange);
        LOG(ERROR) << "getOutNeighborsProps failed";
        std::vector<std::string> ret;
        return ret;
    }

    using RpcResp = StorageRpcResponse<cpp2::GetNeighborsResponse>;

    static std::vector<std::string> extractMultiRpcResp(RpcResp& rpc) {
        std::vector<std::string> ret;
        LOG(INFO) << "rpc.responses().size()=" << rpc.responses().size();
        for (auto& resp : rpc.responses()) {
            auto sub = extractNeiProps(resp);
            ret.insert(ret.end(), sub.begin(), sub.end());
        }
        return ret;
    }


    static std::vector<std::string> extractNeiProps(cpp2::GetNeighborsResponse& resp) {
        std::vector<std::string> ret;
        auto& ds = resp.vertices;
        LOG(INFO) << "ds.rows.size()=" << ds.rows.size();
        for (auto& row : ds.rows) {
            if (row.values.size() < 4) {
                continue;
            }
            LOG(INFO) << "row.values.size()=" << row.values.size();
            ret.emplace_back(row.values[3].toString());
        }
        return ret;
    }

    int32_t getCountOfNeighbors(const std::vector<cpp2::NewEdge>& edges) {
        int32_t ret = 0;
        auto neiPropsVec = getNeiProps(edges);
        for (auto& prop : neiPropsVec) {
            ret += countSquareBrackets(prop);
        }
        return ret;
    }

    static int32_t countSquareBrackets(const std::vector<std::string>& str) {
        int32_t ret = 0;
        for (auto& s : str) {
            ret += countSquareBrackets(s);
        }
        return ret;
    }

    static int32_t countSquareBrackets(const std::string& str) {
        auto ret = TossTestUtils::splitNeiResult(str);
        return ret.size();
    }

    static std::vector<std::string> extractProps(const std::string& props) {
        std::vector<std::string> ret;
        if (props.find('[') == std::string::npos) {
            return ret;
        }
        // props string shoule be [[...]], trim the []
        auto props1 = props.substr(2, props.size() - 4);
        folly::split("],[", props1, ret, true);
        std::sort(ret.begin(), ret.end(), [](const std::string& a, const std::string& b) {
            std::vector<std::string> svec1;
            std::vector<std::string> svec2;
            folly::split(",", a, svec1);
            folly::split(",", b, svec2);
            auto i1 = std::atoi(svec1[4].data());
            auto i2 = std::atoi(svec2[4].data());
            return i1 < i2;
        });
        return ret;
    }

    int64_t getSrc() const {
        return src_;
    }

    void setSrc(int64_t src) {
        src_ = src;
    }

    std::vector<cpp2::NewEdge> generateNEdges(size_t cnt) {
        auto vals = TossTestUtils::genValues(cnt);
        std::vector<cpp2::NewEdge> edges;
        for (auto i = 0U; i != cnt; ++i) {
            edges.emplace_back(generateEdge(src_, 0, vals[i], src_ + i));
        }
        return edges;
    }

    void insertMutliLocks(const std::vector<cpp2::NewEdge>& edges) {
        UNUSED(edges);
        // auto lockKey = ;
    }


    std::string insertLock(const cpp2::NewEdge& edge, size_t vIdLen = 8) {
        auto& edgeKey = edge.key;
        auto stPart = mClient_->partId(spaceId_, edgeKey.src.getStr());
        if (!stPart.ok()) {
            LOG(FATAL) << "partId(spaceId_, edgeKey.src.getStr()) not ok";
        }
        auto partId = stPart.value();
        auto rawKey = TransactionUtils::edgeKey(vIdLen, partId, edgeKey, 0);
        auto lockKey = NebulaKeyUtils::toLockKey(rawKey);
        auto cpLockKey = lockKey;

        auto props = edge.props;
        auto edgeType = edgeKey.get_edge_type();
        auto pSchema = schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType)).get();
        if (!pSchema) {
            LOG(FATAL) << "Space " << spaceId_ << ", Edge " << edgeType << " invalid";
        }

        auto propNames = makeColNames(edge.props.size());
        auto lockVal = encodeRowVal(pSchema, propNames, edge.props);
        kvstore::BatchHolder bat;
        bat.put(std::move(lockKey), std::move(lockVal));
        auto batch = encodeBatchValue(bat.getBatch());

        auto sf = interClient_->forwardTransaction(0, spaceId_, partId, std::move(batch)).wait();
        LOG_IF(FATAL, !sf.hasValue()) << "!sf.hasValue()";

        auto& stExec = sf.value();
        LOG_IF(FATAL, !stExec.ok()) << "!stExec.ok()";

        auto& respCommon = stExec.value().result;
        LOG_IF(FATAL, !respCommon.failed_parts.empty()) << "!respCommon.failed_parts.empty()";

        return cpLockKey;
    }

    // simple copy of Storage::BaseProcessor::encodeRowVal
    std::string encodeRowVal(const meta::NebulaSchemaProvider* schema,
                             const std::vector<std::string>& propNames,
                             const std::vector<Value>& props) {
        RowWriterV2 rowWrite(schema);
        WriteResult wRet;
        if (!propNames.empty()) {
            for (size_t i = 0; i < propNames.size(); i++) {
                wRet = rowWrite.setValue(propNames[i], props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        } else {
            for (size_t i = 0; i < props.size(); i++) {
                wRet = rowWrite.setValue(i, props[i]);
                if (wRet != WriteResult::SUCCEEDED) {
                    LOG(FATAL) << "Add field faild";
                }
            }
        }
        wRet = rowWrite.finish();
        if (wRet != WriteResult::SUCCEEDED) {
            LOG(FATAL) << "Add field faild";
        }

        return std::move(rowWrite).moveEncodedStr();
    }

    std::shared_ptr<folly::IOThreadPoolExecutor>        executor_;
    std::unique_ptr<meta::MetaClient>                   mClient_;
    std::unique_ptr<StorageClient>                      sClient_;
    std::unique_ptr<storage::InternalStorageClient>     interClient_;
    std::unique_ptr<meta::SchemaManager>                schemaMan_;

    int32_t                                             spaceId_{0};
    int32_t                                             edgeType_{0};
    int64_t                                             src_{0};
};

}  // namespace storage
}  // namespace nebula
