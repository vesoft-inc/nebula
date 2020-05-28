/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_KVITERATOR_H_
#define KVSTORE_KVITERATOR_H_

#include "common/base/Base.h"

namespace nebula {
namespace kvstore {

class KVIterator {
public:
    virtual ~KVIterator()  = default;

    virtual bool valid() const = 0;

    virtual void next() = 0;

    virtual void prev() = 0;

    virtual folly::StringPiece key() const = 0;

    virtual folly::StringPiece val() const = 0;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_KVITERATOR_H_

