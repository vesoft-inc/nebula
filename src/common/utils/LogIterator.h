/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTILS_LOGITERATOR_H_
#define UTILS_LOGITERATOR_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {

class LogIterator {
public:
    virtual ~LogIterator() = default;

    virtual LogIterator& operator++() = 0;

    virtual bool valid() const = 0;
    virtual operator bool() const {
        return valid();
    }

    virtual LogID logId() const = 0;
    virtual TermID logTerm() const = 0;
    virtual ClusterID logSource() const = 0;
    virtual folly::StringPiece logMsg() const = 0;
};

}  // namespace nebula
#endif  // UTILS_LOGITERATOR_H_
