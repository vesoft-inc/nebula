/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

/*
 * CREATE SPACE sky(partition_num=3, replica_factor=1);
 *
 * USE sky;
 *
 * CREATE TAG tag1(tag1_col1 string, tag1_col2 string);
 *
 * CREATE TAG tag2(tag2_col1 string, tag2_col2 string);
 *
 * CREATE EDGE edge1(edge1_col1 string, edge1_col2 string);
 *
 * CREATE EDGE edge2(edge2_col1 string, edge2_col2 string);
 *
 * CREATE TAG INDEX i_tag1 ON tag1(tag1_col1, tag1_col2);
 *
 * CREATE EDGE INDEX i_edge1 ON edge1(edge1_col1, edge1_col2);
 *
 * INSERT VERTEX tag1(tag1_col1, tag1_col2) VALUES
 * 100:("tag1_col1_100", "tag1_col2_100"),
 * 200:("tag1_col1_200", "tag1_col2_200"),
 * 300:("tag1_col1_300", "tag1_col2_300");
 *
 * INSERT VERTEX tag2(tag2_col1, tag2_col2) VALUES
 * 100:("tag2_col1_100", "tag2_col2_100"),
 * 200:("tag2_col1_200", "tag2_col2_200"),
 * 300:("tag2_col1_300", "tag2_col2_300");
 *
 * INSERT EDGE edge1(edge1_col1 , edge1_col2) VALUES
 * 100 -> 200@0:("edge1_col1_100_200", "edge1_col2_100_200"),
 * 100 -> 300@0:("edge1_col1_100_300", "edge1_col2_100_300");
 *
 * INSERT EDGE edge2(edge2_col1 , edge2_col2) VALUES
 * 100 -> 200@0:("edge2_col1_100_200", "edge2_col2_100_200"),
 * 100 -> 300@0:("edge2_col1_100_300", "edge2_col2_100_300")
 *
 * INSERT EDGE edge1(edge1_col1 , edge1_col2) VALUES
 * 200 -> 100@0:("edge1_col1_200_100", "edge1_col2_200_100"),
 * 200 -> 300@0:("edge1_col1_200_300", "edge1_col2_200_300")
 *
 */
#include "tools/storage-client/src/NebulaStorageClient.h"

nebula::ResultCode lookupTagIndex(nebula::NebulaStorageClient *client);

nebula::ResultCode lookupEdgeIndex(nebula::NebulaStorageClient *client);

nebula::ResultCode getNeighbors(nebula::NebulaStorageClient *client);

nebula::ResultCode getVertexProps(nebula::NebulaStorageClient *client);

int main(int argc, char** argv) {
    // step 1 : Declare NebulaStorageClient
    auto client = std::make_unique<nebula::NebulaStorageClient> ("127.0.0.1:78833");

    // step 2 : initialize args, optional.
    auto ret = client->initArgs(argc, argv);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }

    // step 3 : set the logging path, optional.
    ret = client->setLogging("/tmp/nebula_storage_client/logs/");
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }

    // step 4 : initialize NebulaStorageClient.
    ret = client->init("cbs");
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }

    // step 5 : execute query and fetch result set.
    ret = lookupTagIndex(client.get());
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }

    ret = lookupEdgeIndex(client.get());
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return -1;
    }

    ret = getNeighbors(client.get());
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return -1;
    }

    ret = getVertexProps(client.get());
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return -1;
    }
    return 0;
}

nebula::ResultCode lookupTagIndex(nebula::NebulaStorageClient *client) {
    // LOOKUP ON tag1 WHERE tag1.tag1_col1 != "a" YIELD tag1.tag1_col1, tag1.tag1_col2;
    //    ==============================================
    //    | VertexID | tag1.tag1_col1 | tag1.tag1_col2 |
    //    ==============================================
    //    | 300      | tag1_col1_300  | tag1_col2_300  |
    //    ----------------------------------------------
    //    | 100      | tag1_col1_100  | tag1_col2_100  |
    //    ----------------------------------------------
    //    | 200      | tag1_col1_200  | tag1_col2_200  |
    //    ----------------------------------------------

    nebula::ResultSet rst;
    auto filter = "tag1.tag1_col1 != \"a\"";
    nebula::LookupContext input("i_tag1", filter, {"tag1_col1", "tag1_col2"}, false);
    auto ret = client->lookup(input, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 3) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }

    // LOOKUP ON tag1 WHERE tag1.tag1_col1 == "tag1_col1_300" YIELD tag1.tag1_col1, tag1.tag1_col2;
    //    ==============================================
    //    | VertexID | tag1.tag1_col1 | tag1.tag1_col2 |
    //    ==============================================
    //    | 300      | tag1_col1_300  | tag1_col2_300  |
    //    ----------------------------------------------

    filter = "tag1.tag1_col1 == \"tag1_col1_300\"";
    nebula::LookupContext input2("i_tag1", filter, {"tag1_col1", "tag1_col2"}, false);
    ret = client->lookup(input2, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 1) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }
    return nebula::ResultCode::SUCCEEDED;
}

nebula::ResultCode lookupEdgeIndex(nebula::NebulaStorageClient *client) {
    // LOOKUP ON edge1 WHERE edge1.edge1_col1 != "a" YIELD edge1.edge1_col1, edge1.edge1_col2;
    //    =======================================================================
    //    | SrcVID | DstVID | Ranking | edge1.edge1_col1   | edge1.edge1_col2   |
    //    =======================================================================
    //    | 100    | 200    | 0       | edge1_col1_100_200 | edge1_col2_100_200 |
    //    -----------------------------------------------------------------------
    //    | 100    | 300    | 0       | edge1_col1_100_300 | edge1_col2_100_300 |
    //    -----------------------------------------------------------------------
    //    | 200    | 100    | 0       | edge1_col1_200_100 | edge1_col2_200_100 |
    //    -----------------------------------------------------------------------
    //    | 200    | 300    | 0       | edge1_col1_200_300 | edge1_col2_200_300 |
    //    -----------------------------------------------------------------------

    nebula::ResultSet rst;
    auto filter = "edge1.edge1_col1 != \"a\"";
    nebula::LookupContext input("i_edge1", filter, {"edge1_col1", "edge1_col2"}, true);
    auto ret = client->lookup(input, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 4) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }

    // LOOKUP ON edge1 WHERE edge1.edge1_col1 == "edge1_col1_100_300"
    // YIELD edge1.edge1_col1, edge1.edge1_col2;
    //    =======================================================================
    //    | SrcVID | DstVID | Ranking | edge1.edge1_col1   | edge1.edge1_col2   |
    //    =======================================================================
    //    | 100    | 300    | 0       | edge1_col1_100_300 | edge1_col2_100_300 |
    //    -----------------------------------------------------------------------

    filter = "edge1.edge1_col1 == \"edge1_col1_100_300\"";
    nebula::LookupContext input2("i_edge1", filter, {"edge1_col1", "edge1_col2"}, true);
    ret = client->lookup(input2, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 1) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }
    return nebula::ResultCode::SUCCEEDED;
}

nebula::ResultCode getNeighbors(nebula::NebulaStorageClient *client) {
    //    GO FROM 100 OVER edge1,edge2
    //    WHERE edge1.edge1_col1 == "edge1_col1_100_200"
    //    YIELD edge1._dst, edge2._dst, $^.tag1.tag1_col1, edge1._src, edge2._src,
    //    $^.tag2.tag2_col1, edge1.edge1_col1, edge2.edge2_col1;
    //    =====================================================================================================================================    // NOLINT
    //    | edge1._dst | edge2._dst | $^.tag1.tag1_col1 | edge1._src | edge2._src | $^.tag2.tag2_col1 | edge1.edge1_col1   | edge2.edge2_col1 |    // NOLINT
    //    =====================================================================================================================================    // NOLINT
    //    | 200        |            | tag1_col1_100     | 100        |            | tag2_col1_100     | edge1_col1_100_200 |                  |    // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------    // NOLINT

    nebula::ResultSet rst;
    std::vector<nebula::PropDef> returns;
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::EDGE, "edge1", "_dst"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::EDGE, "edge2", "_dst"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::TAG, "tag1", "tag1_col1"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::EDGE, "edge1", "_src"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::EDGE, "edge2", "_src"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::TAG, "tag2", "tag2_col1"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::EDGE, "edge1", "edge1_col1"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::EDGE, "edge2", "edge2_col1"));
    auto filter = "edge1.edge1_col1 == \"edge1_col1_100_200\"";
    nebula::NeighborsContext input({100}, {"edge1", "edge2"}, filter, returns);
    auto ret = client->getNeighbors(input, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 1) {
        return nebula::ResultCode::E_PARTIAL_RESULT;;
    }

    //    GO FROM 100 OVER edge1,edge2
    //    WHERE edge1.edge1_col1 != "a"
    //    YIELD edge1._dst, edge2._dst, $^.tag1.tag1_col1, edge1._src, edge2._src,
    //    $^.tag2.tag2_col1, edge1.edge1_col1, edge2.edge2_col1;
    //    =====================================================================================================================================   // NOLINT
    //    | edge1._dst | edge2._dst | $^.tag1.tag1_col1 | edge1._src | edge2._src | $^.tag2.tag2_col1 | edge1.edge1_col1   | edge2.edge2_col1 |   // NOLINT
    //    =====================================================================================================================================   // NOLINT
    //    | 300        |            | tag1_col1_100     | 100        |            | tag2_col1_100     | edge1_col1_100_300 |                  |   // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------   // NOLINT
    //    | 200        |            | tag1_col1_100     | 100        |            | tag2_col1_100     | edge1_col1_100_200 |                  |   // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------   // NOLINT

    filter = "edge1.edge1_col1 != \"a\"";
    nebula::NeighborsContext input2({100}, {"edge1", "edge2"}, filter, returns);
    ret = client->getNeighbors(input2, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 2) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }

    //    GO FROM 100, 200 OVER edge1,edge2
    //    WHERE edge2.edge2_col1 != "a"
    //    YIELD edge1._dst, edge2._dst, $^.tag1.tag1_col1, edge1._src, edge2._src,
    //    $^.tag2.tag2_col1, edge1.edge1_col1, edge2.edge2_col1;
    //    =====================================================================================================================================  // NOLINT
    //    | edge1._dst | edge2._dst | $^.tag1.tag1_col1 | edge1._src | edge2._src | $^.tag2.tag2_col1 | edge1.edge1_col1 | edge2.edge2_col1   |  // NOLINT
    //    =====================================================================================================================================  // NOLINT
    //    |            | 300        | tag1_col1_100     |            | 100        | tag2_col1_100     |                  | edge2_col1_100_300 |  // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------  // NOLINT
    //    |            | 200        | tag1_col1_100     |            | 100        | tag2_col1_100     |                  | edge2_col1_100_200 |  // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------  // NOLINT

    filter = "edge2.edge2_col1 != \"a\"";
    nebula::NeighborsContext input3({100}, {"edge1", "edge2"}, filter, returns);
    ret = client->getNeighbors(input3, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 2) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }

    //    GO FROM 100, 200 OVER edge1,edge2
    //    WHERE edge1.edge1_col1 != "a"
    //    YIELD edge1._dst, edge2._dst, $^.tag1.tag1_col1, edge1._src,
    //    edge2._src, $^.tag2.tag2_col1, edge1.edge1_col1, edge2.edge2_col1;
    //    =====================================================================================================================================  // NOLINT
    //    | edge1._dst | edge2._dst | $^.tag1.tag1_col1 | edge1._src | edge2._src | $^.tag2.tag2_col1 | edge1.edge1_col1   | edge2.edge2_col1 |  // NOLINT
    //    =====================================================================================================================================  // NOLINT
    //    | 300        |            | tag1_col1_100     | 100        |            | tag2_col1_100     | edge1_col1_100_300 |                  |  // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------  // NOLINT
    //    | 200        |            | tag1_col1_100     | 100        |            | tag2_col1_100     | edge1_col1_100_200 |                  |  // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------  // NOLINT
    //    | 300        |            | tag1_col1_200     | 200        |            | tag2_col1_200     | edge1_col1_200_300 |                  |  // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------  // NOLINT
    //    | 100        |            | tag1_col1_200     | 200        |            | tag2_col1_200     | edge1_col1_200_100 |                  |  // NOLINT
    //    -------------------------------------------------------------------------------------------------------------------------------------  // NOLINT

    filter = "edge1.edge1_col1 != \"a\"";
    nebula::NeighborsContext input4({100, 200}, {"edge1", "edge2"}, filter, std::move(returns));
    ret = client->getNeighbors(input4, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 4) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }
    return nebula::ResultCode::SUCCEEDED;
}

nebula::ResultCode getVertexProps(nebula::NebulaStorageClient *client) {
//    FETCH PROP ON tag1 100, 200, 300 ;
//    ==============================================
//    | VertexID | tag1.tag1_col1 | tag1.tag1_col2 |
//    ==============================================
//    | 200      | tag1_col1_200  | tag1_col2_200  |
//    ----------------------------------------------
//    | 100      | tag1_col1_100  | tag1_col2_100  |
//    ----------------------------------------------
//    | 300      | tag1_col1_300  | tag1_col2_300  |
//    ----------------------------------------------

    nebula::ResultSet rst;
    std::vector<nebula::PropDef> returns;
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::TAG, "tag1", "tag1_col1"));
    returns.emplace_back(nebula::PropDef(nebula::PropOwner::TAG, "tag1", "tag1_col2"));
    nebula::VertexPropsContext input({100, 200, 300}, std::move(returns));
    auto ret = client->getVertexProps(input, rst);
    if (ret != nebula::ResultCode::SUCCEEDED) {
        return ret;
    }
    if (rst.rows.size() != 3) {
        return nebula::ResultCode::E_PARTIAL_RESULT;
    }
    return nebula::ResultCode::SUCCEEDED;
}
