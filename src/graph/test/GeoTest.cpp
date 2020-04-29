/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "filter/geo/GeoIndex.h"
#include "filter/geo/GeoParams.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {

class GeoTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }

    static void SetUpTestCase() {
        client_ = gEnv->getClient();
        storagePort_ = gEnv->storageServerPort();

        ASSERT_NE(nullptr, client_);

        ASSERT_TRUE(prepareSchema());

        ASSERT_TRUE(prepareData());
    }

    static void TearDownTestCase() {
        ASSERT_TRUE(removeData());
        client_.reset();
    }

    static AssertionResult prepareSchema();

    static AssertionResult prepareData();

    static AssertionResult removeData();

    class Merchant final {
    public:
        Merchant(std::string name, std::string coordinate, double rate) {
            name_ = std::move(name);
            coordinate_ = std::move(coordinate);
            rate_ = rate;
        }

        std::string name() const {
            return name_;
        }

        std::string coordinate() const {
            return coordinate_;
        }

        double rate() const {
            return rate_;
        }

    private:
        std::string name_;
        std::string coordinate_;
        double      rate_;
    };

protected:
    static uint16_t                             storagePort_;
    static std::unique_ptr<GraphClient>         client_;
    static std::vector<Merchant>                merchants_;
};

uint16_t                              GeoTest::storagePort_;
std::unique_ptr<GraphClient>          GeoTest::client_;
std::vector<GeoTest::Merchant>        GeoTest::merchants_ = {
    Merchant{"HCYQJ Convenience", "(30.28522 120.01338)", 4.1},
    Merchant{"LYJ Convenience", "(30.28115 120.01438)", 3.7}
};

// static
AssertionResult GeoTest::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE geo(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE myspace(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE myspace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG merchant(name string, coordinate string, rate double)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE geo";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE locate(name string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }

    sleep(10 + 3);
    return TestOK();
}

AssertionResult GeoTest::prepareData() {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT EDGE locate(name) VALUES ";
        geo::Point loc;
        geo::GeoIndex geoIndex;
        std::vector<S2CellId> cells;
        for (decltype(merchants_.size()) index = 0; index < merchants_.size(); ++index) {
            auto &merchant = merchants_[index];
            std::string locStr = geo::kWktPointPrefix;
            locStr += merchant.coordinate();
            boost::geometry::read_wkt(locStr, loc);
            geoIndex.indexCellsForPoint(loc, cells);
            for (auto &cell : cells) {
                query += folly::to<std::string>(cell.id());
                query += " -> ";
                query += folly::to<std::string>(index);
                query += ": (\"";
                query += merchant.name();
                query += "\"),\n\t";
            }
            cells.clear();
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `locate' failed: "
                               << static_cast<int32_t>(code)
                               << " errmsg:" << *resp.get_error_msg();
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE myspace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT VERTEX merchant(name, coordinate, rate) VALUES ";
        for (decltype(merchants_.size()) index = 0; index < merchants_.size(); ++index) {
            auto &merchant = merchants_[index];
            query += folly::to<std::string>(index);
            query += ": (\"";
            query += merchant.name();
            query += "\",\"";
            query += merchant.coordinate();
            query += "\",";
            query += folly::to<std::string>(merchant.rate());
            query += "),\n\t";
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `merchant' failed: "
                               << static_cast<int32_t>(code);
        }
    }

    return TestOK();
}

AssertionResult GeoTest::removeData() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE geo";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE myspace";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    return TestOK();
}

TEST_F(GeoTest, Near) {
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "USE geo;"
                    "GO FROM near(%s, 5000) OVER locate";
        std::string vesoftLoc = "\"(30.28243 120.01198)\"";
        std::string query = folly::stringPrintf(fmt, vesoftLoc.c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"locate._dst"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {0},
            {1}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "USE geo;"
                    "$locate = GO FROM near(%s, 5000) OVER locate YIELD locate._dst as id;"
                    "USE myspace;"
                    "FETCH PROP on merchant $locate.id;";
        std::string vesoftLoc = "\"(30.28243 120.01198)\"";
        std::string query = folly::stringPrintf(fmt, vesoftLoc.c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"merchant.name"}, {"merchant.coordinate"}, {"merchant.rate"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, std::string, double>> expected = {
            {merchants_[0].name(), merchants_[0].coordinate(), merchants_[0].rate()},
            {merchants_[1].name(), merchants_[1].coordinate(), merchants_[1].rate()},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        auto *fmt = "USE geo;"
                    "$locate = GO FROM near(%s, 5000) OVER locate YIELD locate._dst as id;"
                    "USE myspace;"
                    "FETCH PROP on merchant $locate.id"
                    " YIELD merchant.name AS name, merchant.rate AS rate"
                    " | ORDER BY $-.rate;";
        std::string vesoftLoc = "\"(30.28243 120.01198)\"";
        std::string query = folly::stringPrintf(fmt, vesoftLoc.c_str());
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"VertexID"}, {"name"}, {"rate"}
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<std::string, double>> expected = {
            {merchants_[1].name(), merchants_[1].rate()},
            {merchants_[0].name(), merchants_[0].rate()},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false, {0}));
    }
}
}  // namespace graph
}  // namespace nebula
