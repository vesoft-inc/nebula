/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_LOGITERATOR_H_
#define RAFTEX_LOGITERATOR_H_

#include "base/Base.h"

namespace vesoft {
namespace vgraph {
namespace raftex {

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
    virtual folly::StringPiece logMsg() const = 0;
};

}  // namespace raftex
}  // namespace vgraph
}  // namespace vesoft
#endif  // RAFTEX_LOGITERATOR_H_

