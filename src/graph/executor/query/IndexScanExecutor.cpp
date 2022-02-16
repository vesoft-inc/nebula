/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/IndexScanExecutor.h"

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for Future::Future<T>
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Pro...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for PromiseException::Pro...

#include <algorithm>      // for find_if
#include <ostream>        // for basic_ostream::operat...
#include <string>         // for string, basic_string
#include <tuple>          // for get
#include <unordered_map>  // for operator!=
#include <utility>        // for move
#include <vector>         // for vector<>::iterator

#include "clients/storage/StorageClient.h"      // for StorageClient, Storag...
#include "clients/storage/StorageClientBase.h"  // for StorageRpcResponse
#include "common/base/Logging.h"                // for Check_EQImpl, DCHECK_EQ
#include "common/datatypes/DataSet.h"           // for Row, DataSet
#include "common/datatypes/Value.h"             // for Value
#include "graph/context/Iterator.h"             // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/context/Result.h"               // for ResultBuilder, Result
#include "graph/planner/plan/ExecutionPlan.h"   // for ExecutionPlan
#include "graph/planner/plan/PlanNode.h"        // for PlanNode
#include "graph/service/GraphFlags.h"           // for FLAGS_accept_partial_...
#include "graph/service/RequestContext.h"       // for RequestContext
#include "graph/session/ClientSession.h"        // for ClientSession
#include "interface/gen-cpp2/storage_types.h"   // for OrderBy, LookupIndexResp

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::LookupIndexResp;

namespace nebula {
namespace graph {

folly::Future<Status> IndexScanExecutor::execute() {
  return indexScan();
}

folly::Future<Status> IndexScanExecutor::indexScan() {
  StorageClient *storageClient = qctx_->getStorageClient();
  auto *lookup = asNode<IndexScan>(node());
  if (lookup->isEmptyResultSet()) {
    DataSet dataSet({"dummy"});
    return finish(ResultBuilder().value(Value(std::move(dataSet))).build());
  }

  const auto &ictxs = lookup->queryContext();
  auto iter = std::find_if(
      ictxs.begin(), ictxs.end(), [](auto &ictx) { return !ictx.index_id_ref().is_set(); });
  if (ictxs.empty() || iter != ictxs.end()) {
    return Status::Error("There is no index to use at runtime");
  }

  StorageClient::CommonRequestParam param(lookup->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient
      ->lookupIndex(param,
                    ictxs,
                    lookup->isEdge(),
                    lookup->schemaId(),
                    lookup->returnColumns(),
                    lookup->orderBy(),
                    lookup->limit(qctx_))
      .via(runner())
      .thenValue([this](StorageRpcResponse<LookupIndexResp> &&rpcResp) {
        addStats(rpcResp, otherStats_);
        return handleResp(std::move(rpcResp));
      });
}

// TODO(shylock) merge the handler with GetProp
template <typename Resp>
Status IndexScanExecutor::handleResp(storage::StorageRpcResponse<Resp> &&rpcResp) {
  auto completeness = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  if (!completeness.ok()) {
    return std::move(completeness).status();
  }
  auto state = std::move(completeness).value();
  nebula::DataSet v;
  for (auto &resp : rpcResp.responses()) {
    if (resp.data_ref().has_value()) {
      nebula::DataSet &data = *resp.data_ref();
      // TODO: convert the column name to alias.
      if (v.colNames.empty()) {
        v.colNames = data.colNames;
      }
      v.rows.insert(v.rows.end(), data.rows.begin(), data.rows.end());
    } else {
      state = Result::State::kPartialSuccess;
    }
  }
  if (!node()->colNames().empty()) {
    DCHECK_EQ(node()->colNames().size(), v.colNames.size());
    v.colNames = node()->colNames();
  }
  return finish(
      ResultBuilder().value(std::move(v)).iter(Iterator::Kind::kProp).state(state).build());
}

}  // namespace graph
}  // namespace nebula
