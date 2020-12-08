/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONTYPE_H_
#define STORAGE_TRANSACTION_TRANSACTIONTYPE_H_

#include <folly/FBVector.h>

namespace nebula {
namespace storage {

class Transaction {
public:
    Transaction() = default;
    virtual ~Transaction() = default;
    virtual bool run() = 0;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTION_H_
