// Copyright (c) 2025 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/AnnIndexScanExecutor.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/OptimizerUtils.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::LookupIndexResp;

using IndexQueryContextList = nebula::graph::OptimizerUtils::IndexQueryContextList;

namespace nebula {
namespace graph {

folly::Future<Status> AnnIndexScanExecutor::execute() {
  return annIndexScan();
}

folly::Future<Status> AnnIndexScanExecutor::annIndexScan() {
  StorageClient *storageClient = qctx_->getStorageClient();
  auto *lookup = asNode<AnnIndexScan>(node());

  storage::cpp2::IndexQueryContext ictx;
  ictx = lookup->vectorContext();

  if (!ictx.index_id_ref().is_set()) {
    return Status::Error("There is no ann index to use at runtime");
  }

  StorageClient::CommonRequestParam param(lookup->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return storageClient
      ->lookupAnnIndex(param,
                       ictx,
                       lookup->isEdge(),
                       lookup->vectorSchemaIds(),
                       lookup->returnColumns(),
                       lookup->limit(qctx_),
                       lookup->queryVector(),
                       lookup->queryParam())

      .via(runner())
      .thenValue([this](StorageRpcResponse<LookupIndexResp> &&rpcResp) {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        addStats(rpcResp);
        return handleResp(std::move(rpcResp));
      });
}

// TODO(shylock) merge the handler with GetProp
template <typename Resp>
Status AnnIndexScanExecutor::handleResp(storage::StorageRpcResponse<Resp> &&rpcResp) {
  auto completeness = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  if (!completeness.ok()) {
    return std::move(completeness).status();
  }
  auto state = std::move(completeness).value();
  nebula::DataSet v;
  for (auto &resp : rpcResp.responses()) {
    if (resp.data_ref().has_value()) {
      nebula::DataSet &data = *resp.data_ref();
      v.rows.insert(v.rows.end(), data.rows.begin(), data.rows.end());
    } else {
      state = Result::State::kPartialSuccess;
    }
  }
  // It will appear multitag with _vid, so we set it again.
  v.colNames = {kVid};
  if (!node()->colNames().empty()) {
    const auto &expected = node()->colNames();
    if (expected.size() == v.colNames.size()) {
      v.colNames = expected;
    }
  }

  if (qctx()->plan()->isProfileEnabled()) {
    std::vector<std::string> vids;
    for (const auto &row : v.rows) {
      if (!row.values.empty()) {
        vids.push_back(row.values[0].toString());
      }
    }
  }
  return finish(
      ResultBuilder().value(std::move(v)).iter(Iterator::Kind::kProp).state(state).build());
}

}  // namespace graph
}  // namespace nebula
