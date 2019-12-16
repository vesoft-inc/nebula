/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TEST_TRAVERSETESTBASE_H_
#define GRAPH_TEST_TRAVERSETESTBASE_H_

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace graph {
class TraverseTestBase : public TestBase {
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

    AssertionResult verifyPath(const cpp2::ExecutionResponse &resp,
                               std::vector<std::string> &expected) {
        if (resp.get_error_code() != cpp2::ErrorCode::SUCCEEDED) {
            auto *errmsg = resp.get_error_msg();
            return TestError() << "Query failed with `"
                               << static_cast<int32_t>(resp.get_error_code())
                               << (errmsg == nullptr ? "'" : "': " + *errmsg);
        }

        if (resp.get_rows() == nullptr && expected.empty()) {
            return TestOK();
        }

        auto rows = buildPathString(*resp.get_rows());

        if (expected.size() != rows.size()) {
            return TestError() << "Rows' count not match: "
                               << rows.size() << " vs. " << expected.size();
        }

        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        for (decltype(rows.size()) i = 0; i < rows.size(); ++i) {
            VLOG(2) << rows[i];
        }

        for (decltype(rows.size()) i = 0; i < rows.size(); ++i) {
            if (rows[i] != expected[i]) {
                return TestError() << rows[i] << " vs. " << expected[i];
            }
        }
        return TestOK();
    }

    static std::vector<std::string> buildPathString(std::vector<cpp2::RowValue> rows) {
        std::vector<std::string> paths;
        for (auto &row : rows) {
            auto &pathValue = row.get_columns().back().get_path();
            auto &cols = pathValue.get_entry_list();
            std::string pathStr;
            auto iter = cols.begin();
            while (iter < (cols.end() - 1)) {
                pathStr += folly::stringPrintf("%ld<%s,%ld>",
                                iter->get_vertex().get_id(),
                                (iter + 1)->get_edge().get_type().c_str(),
                                (iter + 1)->get_edge().get_ranking());
                iter = iter + 2;
            }
            pathStr += folly::to<std::string>(iter->get_vertex().get_id());
            paths.emplace_back(std::move(pathStr));
        }

        return paths;
    }


    static AssertionResult prepareSchema();

    static AssertionResult prepareData();

    static AssertionResult removeData();

    class Player final {
    public:
        Player(std::string name, int64_t age) {
            name_ = std::move(name);
            age_ = age;
            vid_ = std::hash<std::string>()(name_);
        }

        Player(const Player&) = default;
        Player(Player&&) = default;
        Player& operator=(const Player&) = default;
        Player& operator=(Player&&) = default;

        const std::string& name() const {
            return name_;
        }

        int64_t vid() const {
            return vid_;
        }

        int64_t age() const {
            return age_;
        }

        Player& serve(std::string team, int64_t startYear, int64_t endYear) {
            serves_.emplace_back(std::move(team), startYear, endYear);
            return *this;
        }

        Player& like(std::string player, int64_t likeness) {
            likes_.emplace_back(std::move(player), likeness);
            return *this;
        }

        Player& teammate(std::string player, int64_t startYear, int64_t endYear) {
            teammates_.emplace_back(std::move(player), startYear, endYear);
            return *this;
        }

        const auto& serves() const {
            return serves_;
        }

        const auto& likes() const {
            return likes_;
        }

        const auto& teammates() const {
            return teammates_;
        }

    private:
        using Serve = std::tuple<std::string, int64_t, int64_t>;
        using Like = std::tuple<std::string, int64_t>;
        using TeamMate = std::tuple<std::string, int64_t, int64_t>;
        std::string                             name_;
        int64_t                                 vid_{0};
        int64_t                                 age_{0};
        std::vector<Serve>                      serves_;
        std::vector<Like>                       likes_;
        std::vector<TeamMate>                   teammates_;
    };

    template <typename Vertex, typename Key = std::string>
    class VertexHolder final {
    public:
        using KeyGetter = std::function<Key(const Vertex&)>;
        using Container = std::unordered_map<Key, Vertex>;
        using InternalIterator = typename Container::iterator;
        VertexHolder(KeyGetter getter, std::initializer_list<Vertex> vertices) {
            getter_ = std::move(getter);
            for (auto &vertex : vertices) {
                vertices_.emplace(getter_(vertex), std::move(vertex));
            }
        }

        const Vertex& operator[](const Key &key) const {
            auto iter = vertices_.find(key);
            CHECK(iter != vertices_.end()) << "Vertex not exist, key: " << key;;
            return iter->second;
        }

        Vertex& operator[](const Key &key) {
            auto iter = vertices_.find(key);
            CHECK(iter != vertices_.end()) << "Vertex not exist, key: " << key;
            return iter->second;
        }

        class Iterator final {
        public:
            explicit Iterator(InternalIterator iter) {
                iter_ = iter;
            }

            Iterator& operator++() {
                ++iter_;
                return *this;
            }

            Vertex& operator*() {
                return iter_->second;
            }

            Vertex* operator->() {
                return &iter_->second;
            }

            bool operator==(const Iterator &rhs) const {
                return iter_ == rhs.iter_;
            }

            bool operator!=(const Iterator &rhs) const {
                return !(*this == rhs);
            }

        private:
            InternalIterator iter_;
        };

        Iterator begin() {
            return Iterator(vertices_.begin());
        }

        Iterator end() {
            return Iterator(vertices_.end());
        }

    private:
        KeyGetter                               getter_;
        Container                               vertices_;
    };


    class Team final {
    public:
        explicit Team(std::string name) {
            name_ = std::move(name);
            vid_ = std::hash<std::string>()(name_);
        }

        const std::string& name() const {
            return name_;
        }

        int64_t vid() const {
            return vid_;
        }

    private:
        std::string                             name_;
        int64_t                                 vid_{0};
    };

protected:
    static uint16_t                             storagePort_;
    static std::unique_ptr<GraphClient>         client_;
    static VertexHolder<Player>                 players_;
    static VertexHolder<Team>                   teams_;
};

uint16_t TraverseTestBase::storagePort_ = 0;

std::unique_ptr<GraphClient> TraverseTestBase::client_;

TraverseTestBase::VertexHolder<TraverseTestBase::Player> TraverseTestBase::players_ = {
    [] (const auto &player) { return player.name(); }, {
        Player{"Tim Duncan", 42/*, 1997*/},
        Player{"Tony Parker", 36},
        Player{"LaMarcus Aldridge", 33},
        Player{"Rudy Gay", 32},
        Player{"Marco Belinelli", 32},
        Player{"Danny Green", 31},
        Player{"Kyle Anderson", 25},
        Player{"Aron Baynes", 32},
        Player{"Boris Diaw", 36},
        Player{"Tiago Splitter", 34},
        Player{"Cory Joseph", 27},
        Player{"David West", 38},
        Player{"Jonathon Simmons", 29},
        Player{"Dejounte Murray", 29},
        Player{"Tracy McGrady", 39},
        Player{"Kobe Bryant", 40},
        Player{"LeBron James", 34},
        Player{"Stephen Curry", 31},
        Player{"Russell Westbrook", 30},
        Player{"Kevin Durant", 30},
        Player{"James Harden", 29},
        Player{"Chris Paul", 33},
        Player{"DeAndre Jordan", 30},
        Player{"Ricky Rubio", 28},

        Player{"Rajon Rondo", 33},
        Player{"Manu Ginobili", 41},
        Player{"Kyrie Irving", 26},
        Player{"Vince Carter", 42},
        Player{"Carmelo Anthony", 34},
        Player{"Dwyane Wade", 37},
        Player{"Joel Embiid", 25},
        Player{"Paul George", 28},
        Player{"Giannis Antetokounmpo", 24},
        Player{"Yao Ming", 38},
        Player{"Blake Griffin", 30},
        Player{"Damian Lillard", 28},
        Player{"Steve Nash", 45},
        Player{"Dirk Nowitzki", 40},
        Player{"Paul Gasol", 38},
        Player{"Marc Gasol", 34},
        Player{"Grant Hill", 46},
        Player{"Ray Allen", 43},
        Player{"Klay Thompson", 29},
        Player{"Kristaps Porzingis", 23},
        Player{"Shaquile O'Neal", 47},
        Player{"JaVale McGee", 31},
        Player{"Dwight Howard", 33},
        Player{"Amar'e Stoudemire", 36},
        Player{"Jason Kidd", 45},
        Player{"Ben Simmons", 22},
        Player{"Luka Doncic", 20},

        Player{"Nobody", 0},
    }
};

TraverseTestBase::VertexHolder<TraverseTestBase::Team> TraverseTestBase::teams_ = {
    [] (const auto &team) { return team.name(); }, {
        Team{"Warriors"},
        Team{"Nuggets"},
        Team{"Rockets"},
        Team{"Trail Blazers"},
        Team{"Spurs"},
        Team{"Thunders"},
        Team{"Jazz"},
        Team{"Clippers"},
        Team{"Kings"},
        Team{"Timberwolves"},
        Team{"Lakers"},
        Team{"Pelicans"},
        Team{"Grizzlies"},
        Team{"Mavericks"},
        Team{"Suns"},

        Team{"Hornets"},
        Team{"Cavaliers"},
        Team{"Celtics"},
        Team{"Raptors"},
        Team{"76ers"},
        Team{"Pacers"},
        Team{"Bulls"},
        Team{"Hawks"},
        Team{"Knicks"},
        Team{"Pistons"},
        Team{"Bucks"},
        Team{"Magic"},
        Team{"Nets"},
        Team{"Wizards"},
        Team{"Heat"},
    }
};


// static
AssertionResult TraverseTestBase::prepareSchema() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE nba(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }

    // fix UTs failed sometimes
    auto kvstore = gEnv->storageServer()->kvStore_.get();
    GraphSpaceID spaceId = 1;  // default_space id is 1
    nebula::storage::TestUtils::waitUntilAllElected(kvstore, spaceId, 1);

    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE nba";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG player(name string, age int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG team(name string)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE serve(start_year int, end_year int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE like(likeness int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE teammate(start_year int, end_year int)";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    sleep(FLAGS_load_data_interval_secs + 3);
    return TestOK();
}


AssertionResult TraverseTestBase::prepareData() {
    // TODO(dutor) Maybe we should move these data into some kind of testing resources, later.
    players_["Tim Duncan"].serve("Spurs", 1997, 2016)
                          .like("Tony Parker", 95)
                          .like("Manu Ginobili", 95)
                          .teammate("Tony Parker", 2001, 2016)
                          .teammate("Manu Ginobili", 2002, 2016)
                          .teammate("LaMarcus Aldridge", 2015, 2016)
                          .teammate("Danny Green", 2010, 2016);

    players_["Tony Parker"].serve("Spurs", 1999, 2018)
                           .serve("Hornets", 2018, 2019)
                           .like("Tim Duncan", 95)
                           .like("Manu Ginobili", 95)
                           .like("LaMarcus Aldridge", 90)
                           .teammate("Tim Duncan", 2001, 2016)
                           .teammate("Manu Ginobili", 2002, 2018)
                           .teammate("LaMarcus Aldridge", 2015, 2018)
                           .teammate("Kyle Anderson", 2014, 2016);

    players_["Manu Ginobili"].serve("Spurs", 2002, 2018)
                             .like("Tim Duncan", 90)
                             .teammate("Tim Duncan", 2002, 2016)
                             .teammate("Tony Parker", 2002, 2016);

    players_["LaMarcus Aldridge"].serve("Trail Blazers", 2006, 2015)
                                 .serve("Spurs", 2015, 2019)
                                 .like("Tony Parker", 75)
                                 .like("Tim Duncan", 75);

    players_["Rudy Gay"].serve("Grizzlies", 2006, 2013)
                        .serve("Raptors", 2013, 2013)
                        .serve("Kings", 2013, 2017)
                        .serve("Spurs", 2017, 2019)
                        .like("LaMarcus Aldridge", 70);

    players_["Marco Belinelli"].serve("Warriors", 2007, 2009)
                               .serve("Raptors", 2009, 2010)
                               .serve("Hornets", 2010, 2012)
                               .serve("Bulls", 2012, 2013)
                               .serve("Spurs", 2013, 2015)
                               .serve("Kings", 2015, 2016)
                               .serve("Hornets", 2016, 2017)
                               .serve("Hawks", 2017, 2018)
                               .serve("76ers", 2018, 2018)
                               .serve("Spurs", 2018, 2019)
                               .like("Tony Parker", 50)
                               .like("Tim Duncan", 55)
                               .like("Danny Green", 60);

    players_["Danny Green"].serve("Cavaliers", 2009, 2010)
                           .serve("Spurs", 2010, 2018)
                           .serve("Raptors", 2018, 2019)
                           .like("Marco Belinelli", 83)
                           .like("Tim Duncan", 70)
                           .like("LeBron James", 80);

    players_["Kyle Anderson"].serve("Spurs", 2014, 2018)
                             .serve("Grizzlies", 2018, 2019);

    players_["Aron Baynes"].serve("Spurs", 2013, 2015)
                           .serve("Pistons", 2015, 2017)
                           .serve("Celtics", 2017, 2019)
                           .like("Tim Duncan", 80);

    players_["Boris Diaw"].serve("Hawks", 2003, 2005)
                          .serve("Suns", 2005, 2008)
                          .serve("Hornets", 2008, 2012)
                          .serve("Spurs", 2012, 2016)
                          .serve("Jazz", 2016, 2017)
                          .like("Tony Parker", 80)
                          .like("Tim Duncan", 80);

    players_["Tiago Splitter"].serve("Spurs", 2010, 2015)
                              .serve("Hawks", 2015, 2017)
                              .serve("76ers", 2017, 2017)
                              .like("Tim Duncan", 80)
                              .like("Manu Ginobili", 90);

    players_["Cory Joseph"].serve("Spurs", 2011, 2015)
                           .serve("Raptors", 2015, 2017)
                           .serve("Pacers", 2017, 2019);

    players_["David West"].serve("Hornets", 2003, 2011)
                          .serve("Pacers", 2011, 2015)
                          .serve("Spurs", 2015, 2016)
                          .serve("Warriors", 2016, 2018);

    players_["Jonathon Simmons"].serve("Spurs", 2015, 2017)
                                .serve("Magic", 2017, 2019)
                                .serve("76ers", 2019, 2019);

    players_["Dejounte Murray"].serve("Spurs", 2016, 2019)
                               .like("Tim Duncan", 99)
                               .like("Tony Parker", 99)
                               .like("Manu Ginobili", 99)
                               .like("Marco Belinelli", 99)
                               .like("Danny Green", 99)
                               .like("LeBron James", 99)
                               .like("Russell Westbrook", 99)
                               .like("Chris Paul", 99)
                               .like("Kyle Anderson", 99)
                               .like("Kevin Durant", 99)
                               .like("James Harden", 99)
                               .like("Tony Parker", 99);

    players_["Tracy McGrady"].serve("Raptors", 1997, 2000)
                             .serve("Magic", 2000, 2004)
                             .serve("Rockets", 2004, 2010)
                             .serve("Spurs", 2013, 2013)
                             .like("Kobe Bryant", 90)
                             .like("Grant Hill", 90)
                             .like("Rudy Gay", 90);

    players_["Kobe Bryant"].serve("Lakers", 1996, 2016);

    players_["LeBron James"].serve("Cavaliers", 2003, 2010)
                            .serve("Heat", 2010, 2014)
                            .serve("Cavaliers", 2014, 2018)
                            .serve("Lakers", 2018, 2019)
                            .like("Ray Allen", 100);

    players_["Stephen Curry"].serve("Warriors", 2009, 2019);

    players_["Russell Westbrook"].serve("Thunders", 2008, 2019)
                                 .like("Paul George", 90)
                                 .like("James Harden", 90);

    players_["Kevin Durant"].serve("Thunders", 2007, 2016)
                            .serve("Warriors", 2016, 2019);

    players_["James Harden"].serve("Thunders", 2009, 2012)
                            .serve("Rockets", 2012, 2019)
                            .like("Russell Westbrook", 80);

    players_["Chris Paul"].serve("Hornets", 2005, 2011)
                          .serve("Clippers", 2011, 2017)
                          .serve("Rockets", 2017, 2021)
                          .like("LeBron James", 90)
                          .like("Carmelo Anthony", 90)
                          .like("Dwyane Wade", 90);

    players_["DeAndre Jordan"].serve("Clippers", 2008, 2018)
                              .serve("Mavericks", 2018, 2019)
                              .serve("Knicks", 2019, 2019);

    players_["Ricky Rubio"].serve("Timberwolves", 2011, 2017)
                            .serve("Jazz", 2017, 2019);

    players_["Rajon Rondo"].serve("Celtics", 2006, 2014)
                           .serve("Mavericks", 2014, 2015)
                           .serve("Kings", 2015, 2016)
                           .serve("Bulls", 2016, 2017)
                           .serve("Pelicans", 2017, 2018)
                           .serve("Lakers", 2018, 2019)
                           .like("Ray Allen", -1);

    players_["Kyrie Irving"].serve("Cavaliers", 2011, 2017)
                            .serve("Celtics", 2017, 2019)
                            .like("LeBron James", 13);

    players_["Vince Carter"].serve("Raptors", 1998, 2004)
                            .serve("Nets", 2004, 2009)
                            .serve("Magic", 2009, 2010)
                            .serve("Suns", 2010, 2011)
                            .serve("Mavericks", 2011, 2014)
                            .serve("Grizzlies", 2014, 2017)
                            .serve("Kings", 2017, 2018)
                            .serve("Hawks", 2018, 2019)
                            .like("Tracy McGrady", 90)
                            .like("Jason Kidd", 70);

    players_["Carmelo Anthony"].serve("Nuggets", 2003, 2011)
                               .serve("Knicks", 2011, 2017)
                               .serve("Thunders", 2017, 2018)
                               .serve("Rockets", 2018, 2019)
                               .like("LeBron James", 90)
                               .like("Chris Paul", 90)
                               .like("Dwyane Wade", 90);

    players_["Dwyane Wade"].serve("Heat", 2003, 2016)
                           .serve("Bulls", 2016, 2017)
                           .serve("Cavaliers", 2017, 2018)
                           .serve("Heat", 2018, 2019)
                           .like("LeBron James", 90)
                           .like("Chris Paul", 90)
                           .like("Carmelo Anthony", 90);

    players_["Joel Embiid"].serve("76ers", 2014, 2019)
                           .like("Ben Simmons", 80);

    players_["Paul George"].serve("Pacers", 2010, 2017)
                           .serve("Thunders", 2017, 2019)
                           .like("Russell Westbrook", 95);

    players_["Giannis Antetokounmpo"].serve("Bucks", 2013, 2019);

    players_["Yao Ming"].serve("Rockets", 2002, 2011)
                        .like("Tracy McGrady", 90)
                        .like("Shaquile O'Neal", 90);

    players_["Blake Griffin"].serve("Clippers", 2009, 2018)
                             .serve("Pistons", 2018, 2019)
                             .like("Chris Paul", -1);

    players_["Damian Lillard"].serve("Trail Blazers", 2012, 2019)
                              .like("LaMarcus Aldridge", 80);

    players_["Steve Nash"].serve("Suns", 1996, 1998)
                          .serve("Mavericks", 1998, 2004)
                          .serve("Suns", 2004, 2012)
                          .serve("Lakers", 2012, 2015)
                          .like("Amar'e Stoudemire", 90)
                          .like("Dirk Nowitzki", 88)
                          .like("Stephen Curry", 90)
                          .like("Jason Kidd", 85);

    players_["Dirk Nowitzki"].serve("Mavericks", 1998, 2019)
                             .like("Steve Nash", 80)
                             .like("Jason Kidd", 80)
                             .like("Dwyane Wade", 10);

    players_["Paul Gasol"].serve("Grizzlies", 2001, 2008)
                          .serve("Lakers", 2008, 2014)
                          .serve("Bulls", 2014, 2016)
                          .serve("Spurs", 2016, 2019)
                          .serve("Bucks", 2019, 2020)
                          .like("Kobe Bryant", 90)
                          .like("Marc Gasol", 99);

    players_["Marc Gasol"].serve("Grizzlies", 2008, 2019)
                          .serve("Raptors", 2019, 2019)
                          .like("Paul Gasol", 99);

    players_["Grant Hill"].serve("Pistons", 1994, 2000)
                          .serve("Magic", 2000, 2007)
                          .serve("Suns", 2007, 2012)
                          .serve("Clippers", 2012, 2013)
                          .like("Tracy McGrady", 90);

    players_["Ray Allen"].serve("Bucks", 1996, 2003)
                         .serve("Thunders", 2003, 2007)
                         .serve("Celtics", 2007, 2012)
                         .serve("Heat", 2012, 2014)
                         .like("Rajon Rondo", 9);

    players_["Klay Thompson"].serve("Warriors", 2011, 2019)
                             .like("Stephen Curry", 90);

    players_["Kristaps Porzingis"].serve("Knicks", 2015, 2019)
                                  .serve("Mavericks", 2019, 2020)
                                  .like("Luka Doncic", 90);

    players_["Shaquile O'Neal"].serve("Magic", 1992, 1996)
                               .serve("Lakers", 1996, 2004)
                               .serve("Heat", 2004, 2008)
                               .serve("Suns", 2008, 2009)
                               .serve("Cavaliers", 2009, 2010)
                               .serve("Celtics", 2010, 2011)
                               .like("JaVale McGee", 100)
                               .like("Tim Duncan", 80);

    players_["JaVale McGee"].serve("Wizards", 2008, 2012)
                            .serve("Nuggets", 2012, 2015)
                            .serve("Mavericks", 2015, 2016)
                            .serve("Warriors", 2016, 2018)
                            .serve("Lakers", 2018, 2019);

    players_["Dwight Howard"].serve("Magic", 2004, 2012)
                            .serve("Lakers", 2012, 2013)
                            .serve("Rockets", 2013, 2016)
                            .serve("Hawks", 2016, 2017)
                            .serve("Hornets", 2017, 2018)
                            .serve("Wizards", 2018, 2019);

    players_["Amar'e Stoudemire"].serve("Suns", 2002, 2010)
                                 .serve("Knicks", 2010, 2015)
                                 .serve("Heat", 2015, 2016)
                                 .like("Steve Nash", 90);

    players_["Jason Kidd"].serve("Mavericks", 1994, 1996)
                          .serve("Suns", 1996, 2001)
                          .serve("Nets", 2001, 2008)
                          .serve("Mavericks", 2008, 2012)
                          .serve("Knicks", 2012, 2013)
                          .like("Vince Carter", 80)
                          .like("Steve Nash", 90)
                          .like("Dirk Nowitzki", 85);

    players_["Ben Simmons"].serve("76ers", 2016, 2019)
                           .like("Joel Embiid", 80);

    players_["Luka Doncic"].serve("Mavericks", 2018, 2019)
                           .like("Dirk Nowitzki", 90)
                           .like("Kristaps Porzingis", 90)
                           .like("James Harden", 80);

    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE nba";
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "USE nba failed"
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert vertices `player'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT VERTEX player(name, age) VALUES ";
        for (auto &player : players_) {
            query += std::to_string(player.vid());
            query += ": ";
            query += "(";
            query += "\"";
            query += player.name();
            query += "\"";
            query += ",";
            query += std::to_string(player.age());
            query += "),\n\t";
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `players' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert vertices `player' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT VERTEX player(name, age) VALUES ";
        for (auto &player : players_) {
            query += "uuid(\"";
            query += player.name();
            query += "\"): ";
            query += "(";
            query += "\"";
            query += player.name();
            query += "\"";
            query += ",";
            query += std::to_string(player.age());
            query += "),\n\t";
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `players' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert vertices `team'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT VERTEX team(name) VALUES ";
        for (auto &team : teams_) {
            query += std::to_string(team.vid());
            query += ": ";
            query += "(";
            query += "\"";
            query += team.name();
            query += "\"";
            query += "),\n\t";
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `teams' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert vertices `team' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT VERTEX team(name) VALUES ";
        for (auto &team : teams_) {
            query += "uuid(\"";
            query += team.name();
            query += "\"): ";
            query += "(";
            query += "\"";
            query += team.name();
            query += "\"";
            query += "),\n\t";
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `teams' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert edges `serve'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT EDGE serve(start_year, end_year) VALUES ";
        for (auto &player : players_) {
            for (auto &serve : player.serves()) {
                auto &team = std::get<0>(serve);
                auto startYear = std::get<1>(serve);
                auto endYear = std::get<2>(serve);
                query += std::to_string(player.vid());
                query += " -> ";
                query += std::to_string(teams_[team].vid());
                query += ": ";
                query += "(";
                query += std::to_string(startYear);
                query += ", ";
                query += std::to_string(endYear);
                query += "),\n\t";
            }
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `serve' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert edges `serve' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT EDGE serve(start_year, end_year) VALUES ";
        for (auto &player : players_) {
            for (auto &serve : player.serves()) {
                auto &team = std::get<0>(serve);
                auto startYear = std::get<1>(serve);
                auto endYear = std::get<2>(serve);
                query += "uuid(\"";
                query += player.name();
                query += "\") -> uuid(\"";
                query += teams_[team].name();
                query += "\"): ";
                query += "(";
                query += std::to_string(startYear);
                query += ", ";
                query += std::to_string(endYear);
                query += "),\n\t";
            }
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `serve' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert edges `like'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT EDGE like(likeness) VALUES ";
        for (auto &player : players_) {
            for (auto &like : player.likes()) {
                auto &other = std::get<0>(like);
                auto likeness = std::get<1>(like);
                query += std::to_string(player.vid());
                query += " -> ";
                query += std::to_string(players_[other].vid());
                query += ": ";
                query += "(";
                query += std::to_string(likeness);
                query += "),\n\t";
            }
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `like' failed: "
                               << static_cast<int32_t>(code);
        }
    }
    {
        // Insert edges `like' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT EDGE like(likeness) VALUES ";
        for (auto &player : players_) {
            for (auto &like : player.likes()) {
                auto &other = std::get<0>(like);
                auto likeness = std::get<1>(like);
                query += "uuid(\"";
                query += player.name();
                query += "\") -> uuid(\"";
                query += players_[other].name();
                query += "\"): ";
                query += "(";
                query += std::to_string(likeness);
                query += "),\n\t";
            }
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `like' failed: "
                               << static_cast<int32_t>(code);
        }
    }

    {
        // Insert edges `teammate'
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT EDGE teammate(start_year, end_year) VALUES ";
        for (auto &player : players_) {
            for (auto &tm : player.teammates()) {
                auto &other = std::get<0>(tm);
                auto startYear = std::get<1>(tm);
                auto endYear = std::get<2>(tm);
                query += std::to_string(player.vid()) + " -> ";
                query += std::to_string(players_[other].vid()) + ": (";
                query += std::to_string(startYear) + ", ";
                query += std::to_string(endYear) + "),\n\t";
            }
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `teammate' failed: " << static_cast<int32_t>(code);
        }
    }
    {
        // Insert edges `teammate' with uuid
        cpp2::ExecutionResponse resp;
        std::string query;
        query.reserve(1024);
        query += "INSERT EDGE teammate(start_year, end_year) VALUES ";
        for (auto &player : players_) {
            for (auto &tm : player.teammates()) {
                auto &other = std::get<0>(tm);
                auto startYear = std::get<1>(tm);
                auto endYear = std::get<2>(tm);
                query += "uuid(\"";
                query += player.name();
                query += "\") -> uuid(\"";
                query += players_[other].name();
                query += "\"): (";
                query += std::to_string(startYear) + ", ";
                query += std::to_string(endYear);
                query += "),\n\t";
            }
        }
        query.resize(query.size() - 3);
        auto code = client_->execute(query, resp);
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return TestError() << "Insert `teammate' failed: " << static_cast<int32_t>(code);
        }
    }
    return TestOK();
}

AssertionResult TraverseTestBase::removeData() {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE nba";
        auto code = client_->execute(cmd, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return TestError() << "Do cmd:" << cmd << " failed";
        }
    }
    return TestOK();
}
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_TEST_TRAVERSETESTBASE_H
