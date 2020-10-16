/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPSTATSHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPSTATSHANDLER_H_

#include "common/base/Base.h"
#include "common/webservice/GetStatsHandler.h"

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
