/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

inline void execute(GraphClient* client, const std::string& nGQL) {
    cpp2::ExecutionResponse resp;
    auto code = client->execute(nGQL, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code) << "Do cmd:" << nGQL << " failed";
}

class FetchEmptyPropsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Access the Nebula Environment
        client_ = gEnv->getClient();
        ASSERT_NE(client_, nullptr);

        prepareSchema();
        prepareData();
    }

    void TearDown() override {
        execute(client_.get(), "DROP SPACE empty");
    }

    std::unique_ptr<GraphClient> client_ = nullptr;

private:
    void prepareSchema() {
        execute(client_.get(), "CREATE SPACE empty(partition_num=1, replica_factor=1)");

        auto kvstore = gEnv->storageServer()->kvStore_.get();
        GraphSpaceID spaceId = 1;  // default_space id is 1
        nebula::storage::TestUtils::waitUntilAllElected(kvstore, spaceId, 1);

        const std::vector<std::string> queries = {
            "USE empty",
            "CREATE TAG empty_tag()",
            "CREATE EDGE empty_edge()"
        };

        for (auto& q : queries) {
            execute(client_.get(), q);
        }

        sleep(FLAGS_load_data_interval_secs + 3);
    }

    void prepareData() {
        const std::vector<std::string> queries = {
            "INSERT VERTEX empty_tag() values 1:(), 2:()",
            "INSERT EDGE empty_edge() values 1->2:()",
        };

        for (auto& q : queries) {
            execute(client_.get(), q);
        }
    }
};

static inline void assertEmptyResult(GraphClient* client, const std::string& stmt) {
    cpp2::ExecutionResponse resp;
    auto code = DCHECK_NOTNULL(client)->execute(stmt, resp);
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    ASSERT_NE(resp.get_column_names(), nullptr);
    ASSERT_TRUE(resp.get_column_names()->empty());
    ASSERT_NE(resp.get_rows(), nullptr);
    ASSERT_TRUE(resp.get_rows()->empty());
}

// #1478
// Fetch property from empty tag
TEST_F(FetchEmptyPropsTest, EmptyProps) {
    {
        const std::string stmt = "FETCH PROP ON empty_tag 1";
        assertEmptyResult(client_.get(), stmt);
    }
    {
        const std::string stmt = "FETCH PROP ON empty_edge 1->2";
        assertEmptyResult(client_.get(), stmt);
    }
}

TEST_F(FetchEmptyPropsTest, WithInput) {
    {
        const std::string stmt = "GO FROM 1 OVER empty_edge YIELD empty_edge._dst as id"
                                 " | FETCH PROP ON empty_tag $-.id";
        assertEmptyResult(client_.get(), stmt);
    }
    {
        const std::string stmt =
            "GO FROM 1 OVER empty_edge YIELD empty_edge._src as src, empty_edge._dst as dst"
            " | FETCH PROP ON empty_edge $-.src->$-.dst";
        assertEmptyResult(client_.get(), stmt);
    }
}

}  // namespace graph
}  // namespace nebula
