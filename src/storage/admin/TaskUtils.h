/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_ADMIN_TASKUTILS_H_
#define STORAGE_ADMIN_TASKUTILS_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"

namespace nebula {
namespace storage {

cpp2::ErrorCode toStorageErr(nebula::kvstore::ResultCode code);

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_TASKUTILS_H_
