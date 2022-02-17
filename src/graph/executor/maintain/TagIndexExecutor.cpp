/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/maintain/TagIndexExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>  // for PromiseException::Promise...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>  // for PromiseException::Promise...

#include <algorithm>    // for max
#include <map>          // for map, operator!=, _Rb_tree...
#include <string>       // for string, basic_string, ope...
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move, pair
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for operator<<, Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/List.h"          // for List
#include "common/datatypes/Value.h"         // for Value
#include "common/thrift/ThriftTypes.h"      // for IndexID
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/planner/plan/Maintain.h"    // for CreateTagIndex, DropTagIndex
#include "graph/service/RequestContext.h"   // for RequestContext
#include "graph/session/ClientSession.h"    // for ClientSession, SpaceInfo
#include "graph/util/IndexUtil.h"           // for IndexUtil
#include "interface/gen-cpp2/meta_types.h"  // for IndexFieldDef, IndexItem

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *ctiNode = asNode<CreateTagIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->createTagIndex(spaceId,
                       ctiNode->getIndexName(),
                       ctiNode->getSchemaName(),
                       ctiNode->getFields(),
                       ctiNode->getIfNotExists(),
                       ctiNode->getIndexParams(),
                       ctiNode->getComment())
      .via(runner())
      .thenValue([ctiNode, spaceId](StatusOr<IndexID> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Create index `" << ctiNode->getIndexName()
                     << "' at tag: `" << ctiNode->getSchemaName() << "' failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DropTagIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *dtiNode = asNode<DropTagIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->dropTagIndex(spaceId, dtiNode->getIndexName(), dtiNode->getIfExists())
      .via(runner())
      .thenValue([dtiNode, spaceId](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Drop tag index `" << dtiNode->getIndexName()
                     << "' failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DescTagIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *dtiNode = asNode<DescTagIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->getTagIndex(spaceId, dtiNode->getIndexName())
      .via(runner())
      .thenValue([this, dtiNode, spaceId](StatusOr<meta::cpp2::IndexItem> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Desc tag index `" << dtiNode->getIndexName()
                     << "' failed: " << resp.status();
          return resp.status();
        }

        auto ret = IndexUtil::toDescIndex(resp.value());
        if (!ret.ok()) {
          LOG(ERROR) << ret.status();
          return ret.status();
        }
        return finish(
            ResultBuilder().value(std::move(ret).value()).iter(Iterator::Kind::kDefault).build());
      });
}

folly::Future<Status> ShowCreateTagIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *sctiNode = asNode<ShowCreateTagIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->getTagIndex(spaceId, sctiNode->getIndexName())
      .via(runner())
      .thenValue([this, sctiNode, spaceId](StatusOr<meta::cpp2::IndexItem> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Show create tag index `"
                     << sctiNode->getIndexName() << "' failed: " << resp.status();
          return resp.status();
        }
        auto ret = IndexUtil::toShowCreateIndex(true, sctiNode->getIndexName(), resp.value());
        if (!ret.ok()) {
          LOG(ERROR) << ret.status();
          return ret.status();
        }
        return finish(
            ResultBuilder().value(std::move(ret).value()).iter(Iterator::Kind::kDefault).build());
      });
}

folly::Future<Status> ShowTagIndexesExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *iNode = asNode<ShowTagIndexes>(node());
  const auto &bySchema = iNode->name();
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listTagIndexes(spaceId).via(runner()).thenValue(
      [this, spaceId, bySchema](StatusOr<std::vector<meta::cpp2::IndexItem>> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Show tag indexes failed" << resp.status();
          return resp.status();
        }

        auto tagIndexItems = std::move(resp).value();

        DataSet dataSet;
        dataSet.colNames.emplace_back("Index Name");
        if (bySchema.empty()) {
          dataSet.colNames.emplace_back("By Tag");
        }
        dataSet.colNames.emplace_back("Columns");

        std::map<std::string, std::pair<std::string, std::vector<std::string>>> ids;
        for (auto &tagIndex : tagIndexItems) {
          const auto &sch = tagIndex.get_schema_name();
          const auto &cols = tagIndex.get_fields();
          std::vector<std::string> colsName;
          for (const auto &col : cols) {
            colsName.emplace_back(col.get_name());
          }
          ids[tagIndex.get_index_name()] = {sch, std::move(colsName)};
        }
        for (const auto &i : ids) {
          if (!bySchema.empty() && bySchema != i.second.first) {
            continue;
          }
          Row row;
          row.values.emplace_back(i.first);
          if (bySchema.empty()) {
            row.values.emplace_back(i.second.first);
          }
          List list;
          for (const auto &c : i.second.second) {
            list.values.emplace_back(c);
          }
          row.values.emplace_back(std::move(list));
          dataSet.rows.emplace_back(std::move(row));
        }
        return finish(ResultBuilder()
                          .value(Value(std::move(dataSet)))
                          .iter(Iterator::Kind::kDefault)
                          .build());
      });
}

folly::Future<Status> ShowTagIndexStatusExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listTagIndexStatus(spaceId).via(runner()).thenValue(
      [this, spaceId](StatusOr<std::vector<meta::cpp2::IndexStatus>> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Show tag index status failed" << resp.status();
          return resp.status();
        }

        auto indexStatuses = std::move(resp).value();

        DataSet dataSet;
        dataSet.colNames = {"Name", "Index Status"};
        for (auto &indexStatus : indexStatuses) {
          Row row;
          row.values.emplace_back(std::move(indexStatus.get_name()));
          row.values.emplace_back(std::move(indexStatus.get_status()));
          dataSet.rows.emplace_back(std::move(row));
        }
        return finish(ResultBuilder()
                          .value(Value(std::move(dataSet)))
                          .iter(Iterator::Kind::kDefault)
                          .build());
      });
}

}  // namespace graph
}  // namespace nebula
