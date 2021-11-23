/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "codec/RowReaderWrapper.h"
#include "kvstore/KVEngine.h"
#include "kvstore/Listener.h"

namespace nebula {
namespace kvstore {

class TopoListener : public Listener {
 public:
  TopoListener(GraphSpaceID spaceId,
               PartitionID partId,
               HostAddr localAddr,
               const std::string& walPath,
               std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
               std::shared_ptr<thread::GenericThreadPool> workers,
               std::shared_ptr<folly::Executor> handlers,
               KVEngine* engine,
               int32_t vIdLen)
      : Listener(spaceId,
                 partId,
                 std::move(localAddr),
                 walPath,
                 ioPool,
                 workers,
                 handlers,
                 nullptr,
                 nullptr,
                 nullptr),
        engine_(engine),
        vIdLen_(vIdLen) {}

 protected:
  void init() override;

  bool apply(const std::vector<KV>& data) override;

  bool persist(LogID lastId, TermID lastTerm, LogID applyLogId) override;

  std::pair<LogID, TermID> lastCommittedLogId() override;

  LogID lastApplyLogId() override;

 private:
  KVEngine* engine_{nullptr};
  int32_t vIdLen_;
  static constexpr PartitionID kTopoPartId{0};
};

}  // namespace kvstore
}  // namespace nebula
