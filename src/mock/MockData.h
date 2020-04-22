/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_MOCKDATA_H_
#define MOCK_MOCKDATA_H_

#include "base/Base.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "meta/SchemaProviderIf.h"

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
    bool                starting_{false};
    int                 champions_{0};;
};

class MockData {
public:
    /*
     * Mock schema
     */
    static std::shared_ptr<meta::SchemaProviderIf> mockPlayerTagSchema();

    static std::shared_ptr<meta::SchemaProviderIf> mockTeamTagSchema();

    static std::shared_ptr<meta::SchemaProviderIf> mockEdgeSchema();

    /*
     * Mock data
     */
    // Construct data and request in the order of schema
    static std::vector<VertexData> mockVertices();

    static std::vector<EdgeData> mockEdges();

    static nebula::storage::cpp2::AddVerticesRequest mockAddVertices(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest mockAddEdges(int32_t parts = 6);

    // Construct data and requests in the specified order
    // For convenience, here is the reverse order
    static std::vector<VertexData> mockVerticesSpecifiedOrder();

    static std::vector<EdgeData> mockEdgesSpecifiedOrder();

    static nebula::storage::cpp2::AddVerticesRequest
    mockAddVerticesSpecifiedOrder(int32_t parts = 6);

    static nebula::storage::cpp2::AddEdgesRequest
    mockAddEdgesSpecifiedOrder(int32_t parts = 6);

private:
    static std::vector<std::string> teams_;

    static std::vector<Player> players_;

    static std::vector<Serve> serve_;
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKDATA_H_
