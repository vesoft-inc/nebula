//
// Created by fujie on 24-10-21.
//

#include "LeaderTransferProcessor.h"

namespace nebula {
namespace meta {
void LeaderTransferProcessor::process(const cpp2::LeaderTransferReq &req) {
  auto ret = LeaderTransferProcessor::instance(kv_)->leaderTransfer(req);
  handleErrorCode(ret);
  onFinished();
}
} //namespace meta
} //namespace nebula
