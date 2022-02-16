/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>    // for find
#include <memory>       // for allocator, uni...
#include <string>       // for string, basic_...
#include <type_traits>  // for remove_referen...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/datatypes/DataSet.h"                  // for Row, DataSet
#include "common/datatypes/Date.h"                     // for DateTime, Date...
#include "common/datatypes/HostAddr.h"                 // for HostAddr
#include "common/datatypes/List.h"                     // for List
#include "common/time/TimeConversion.h"                // for TimeConversion
#include "graph/executor/admin/ShowQueriesExecutor.h"  // for ShowQueriesExe...
#include "graph/executor/test/QueryTestBase.h"         // for QueryTestBase
#include "graph/planner/plan/Admin.h"                  // for ShowQueries
#include "interface/gen-cpp2/meta_types.h"             // for QueryDesc, Ses...

namespace nebula {
namespace graph {
class ShowQueriesTest : public QueryTestBase {};

TEST_F(ShowQueriesTest, TestAddQueryAndTopN) {
  meta::cpp2::Session session;
  session.session_id_ref() = 1;
  session.create_time_ref() = 123;
  session.update_time_ref() = 456;
  session.user_name_ref() = "root";
  session.space_name_ref() = "test";
  session.graph_addr_ref() = HostAddr("127.0.0.1", 9669);

  {
    meta::cpp2::QueryDesc desc;
    desc.start_time_ref() = 123;
    desc.status_ref() = meta::cpp2::QueryStatus::RUNNING;
    desc.duration_ref() = 100;
    desc.query_ref() = "";
    desc.graph_addr_ref() = HostAddr("127.0.0.1", 9669);

    session.queries_ref()->emplace(1, std::move(desc));
  }
  {
    meta::cpp2::QueryDesc desc;
    desc.start_time_ref() = 123;
    desc.status_ref() = meta::cpp2::QueryStatus::RUNNING;
    desc.duration_ref() = 200;
    desc.query_ref() = "";
    desc.graph_addr_ref() = HostAddr("127.0.0.1", 9669);

    session.queries_ref()->emplace(2, std::move(desc));
  }

  DataSet dataSet({"SessionID",
                   "ExecutionPlanID",
                   "User",
                   "Host",
                   "StartTime",
                   "DurationInUSec",
                   "Status",
                   "Query"});
  DataSet expected = dataSet;
  {
    Row row;
    row.emplace_back(1);
    row.emplace_back(1);
    row.emplace_back("root");
    row.emplace_back("\"127.0.0.1\":9669");
    auto dateTime = time::TimeConversion::unixSecondsToDateTime(123 / 1000000);
    dateTime.microsec = 123;
    row.emplace_back(std::move(dateTime));
    row.emplace_back(100);
    row.emplace_back("RUNNING");
    row.emplace_back("");
    expected.rows.emplace_back(std::move(row));
  }
  {
    Row row;
    row.emplace_back(1);
    row.emplace_back(2);
    row.emplace_back("root");
    row.emplace_back("\"127.0.0.1\":9669");
    auto dateTime = time::TimeConversion::unixSecondsToDateTime(123 / 1000000);
    dateTime.microsec = 123;
    row.emplace_back(std::move(dateTime));
    row.emplace_back(200);
    row.emplace_back("RUNNING");
    row.emplace_back("");
    expected.rows.emplace_back(std::move(row));
  }

  auto* showQueries = ShowQueries::make(qctx_.get(), nullptr, true);
  ShowQueriesExecutor exe(showQueries, qctx_.get());
  exe.addQueries(session, dataSet);
  EXPECT_EQ(expected.size(), dataSet.size());
  for (auto& row : expected) {
    EXPECT_TRUE(std::find(dataSet.rows.begin(), dataSet.rows.end(), row) != dataSet.rows.end());
  }
}
}  // namespace graph
}  // namespace nebula
