/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_INCLUDE_ITERATOR_H_
#define STORAGE_INCLUDE_ITERATOR_H_

#include "base/Base.h"

namespace nebula {
namespace storage {

class StorageIter {
public:
    virtual ~StorageIter()  = default;

    virtual bool valid() = 0;

    virtual void next() = 0;

    virtual void prev() = 0;

    virtual folly::StringPiece key() = 0;

    virtual folly::StringPiece val() = 0;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_INCLUDE_ITERATOR_H_

