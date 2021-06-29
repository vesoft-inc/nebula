/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_MOCKDATA_H_
#define MOCK_MOCKDATA_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/meta/NebulaSchemaProvider.h"

DECLARE_bool(mock_ttl_col);
DECLARE_int32(mock_ttl_duration);

namespace nebula {
namespace mock {

struct VertexData {
    VertexID            vId_;
    TagID               tId_;
    std::vector<Value>  props_;
};

struct EdgeData {
    VertexID            srcId_;
    EdgeType            type_;
    EdgeRanking         rank_;
    VertexID            dstId_;
    std::vector<Value>  props_;
};

struct Player {
    std::string         name_;
    int                 age_;
    bool                playing_;
    int                 career_;
    int                 startYear_;
    int                 endYear_;
    int                 games_;
    double              avgScore_;
    int                 serveTeams_;
    std::string         country_{""};
    int                 champions_{0};
};

struct Serve {
    std::string         playerName_;
    std::string         teamName_;
    int                 startYear_;
    int                 endYear_;
    int                 teamCareer_;
    int                 teamGames_;
    double              teamAvgScore_;
    std::string         type_{""};
    int                 champions_{0};
};

struct Teammate {
    std::string         player1_;
    std::string         player2_;
    std::string         teamName_;
    int                 startYear_;
    int                 endYear_;
};

class MockData {
public:
    /*
     * Mock schema
     */
    static std::shared_ptr<meta::NebulaSchemaProvider>
    mockPlayerTagSchema(ObjectPool* pool = nullptr, SchemaVer ver = 0, bool hasProp = true);

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTeamTagSchema(SchemaVer ver = 0,
                                                                         bool hasProp = true);

    static std::shared_ptr<meta::NebulaSchemaProvider>
    mockServeEdgeSchema(ObjectPool* pool = nullptr, SchemaVer ver = 0, bool hasProp = true);

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTeammateEdgeSchema(SchemaVer ver = 0,
                                                                              bool hasProp = true);

    static std::vector<nebula::meta::cpp2::ColumnDef> mockGeneralTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockPlayerTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockTeamTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockSimpleTagIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockServeEdgeIndexColumns();

    static std::vector<nebula::meta::cpp2::ColumnDef> mockTeammateEdgeIndexColumns();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockGeneralTagSchemaV1();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockGeneralTagSchemaV2();

    static std::shared_ptr<meta::NebulaSchemaProvider> mockTypicaSchemaV2(ObjectPool* pool);

    static std::vector<nebula::meta::cpp2::ColumnDef> mockTypicaIndexColumns();

    static std::string encodeFixedStr(const std::string& v, size_t len);
    /*
     * Mock data
     */
    // Construct data in the order of schema
    // generate player and team tag
    static std::vector<VertexData> mockVertices(bool upper = false);

    static std::vector<std::pair<PartitionID, std::string>>
    mockPlayerIndexKeys(bool upper = false);

    // generate serve edge
    static std::vector<EdgeData> mockEdges(bool upper = false);

    static std::vector<std::pair<PartitionID, std::string>> mockServeIndexKeys();

    // generate serve and teammate edge
    static std::vector<EdgeData> mockMultiEdges();

    static std::vector<VertexID> mockVerticeIds();

    static std::vector<VertexID> mockPlayerVerticeIds();

    // generate serve edge with different rank
    static std::unordered_map<VertexID, std::vector<EdgeData>> mockmMultiRankServes(
            EdgeRanking rankCount = 1);

    // generate player -> list<Serve> according to players_;
    static std::unordered_map<std::string, std::vector<Serve>> playerServes() {
        std::unordered_map<std::string, std::vector<Serve>> result;
        for (const auto& serve : serves_) {
            result[serve.playerName_].emplace_back(serve);
        }
        return result;
    }

    // generate team -> list<Serve> according to serves_;
    static std::unordered_map<std::string, std::vector<Serve>> teamServes() {
        std::unordered_map<std::string, std::vector<Serve>> result;
        for (const auto& serve : serves_) {
            result[serve.teamName_].emplace_back(serve);
        }
        return result;
    }

    // Only has EdgeKey data, not props
    static std::vector<EdgeData> mockEdgeKeys();

    // Construct data in the specified order
    // For convenience, here is the reverse order
    static std::vector<VertexData> mockVerticesSpecifiedOrder();

    static std::vector<EdgeData> mockEdgesSpecifiedOrder();

    static std::unordered_map<PartitionID, std::vector<VertexData>>
    mockVerticesofPart(int32_t parts = 6);

    static std::unordered_map<PartitionID, std::vector<EdgeData>>
    mockEdgesofPart(int32_t parts = 6);

    /*
     * Mock request
     */
    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesReq(bool upper = false, int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesReq(bool upper = false, int32_t parts = 6);

    static nebula::storage::cpp2::DeleteVerticesRequest
    mockDeleteVerticesReq(int32_t parts = 6);

    static nebula::storage::cpp2::DeleteEdgesRequest
    mockDeleteEdgesReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesSpecifiedOrderReq(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesSpecifiedOrderReq(int32_t parts = 6);

    /*
     * Mock KV data
     */
    static nebula::storage::cpp2::KVPutRequest mockKVPut();

    static nebula::storage::cpp2::KVGetRequest mockKVGet();

    static nebula::storage::cpp2::KVRemoveRequest mockKVRemove();

public:
    static std::vector<std::string> teams_;

    static std::vector<Player> players_;

    static std::vector<Serve> serves_;

    static std::vector<Teammate> teammates_;

    // player name -> list<Serve>
    static std::unordered_map<std::string, std::vector<Serve>> playerServes_;

    // team name -> list<Serve>
    static std::unordered_map<std::string, std::vector<Serve>> teamServes_;

    static EdgeData getReverseEdge(const EdgeData& edge);
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKDATA_H_
