/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPSTATSHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPSTATSHANDLER_H_

#include <folly/dynamic.h>                        // for dynamic
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError

#include <string>  // for string

#include "common/base/Base.h"
#include "webservice/GetStatsHandler.h"  // for GetStatsHandler

namespace nebula {
namespace storage {

class StorageHttpStatsHandler : public nebula::GetStatsHandler {
 public:
  StorageHttpStatsHandler() = default;
  void onError(proxygen::ProxygenError err) noexcept override;
  folly::dynamic getStats() const override;

 private:
  bool statFiltered(const std::string& stat) const;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_HTTP_STORAGEHTTPSTATSHANDLER_H_
