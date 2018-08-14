/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef CONSENSUS_LOGITERATOR_H_
#define CONSENSUS_LOGITERATOR_H_

#include "base/Base.h"

namespace vesoft {
namespace vgraph {
namespace consensus {

class LogIterator {
public:
    virtual ~LogIterator() = default;

    virtual LogIterator& operator++() = 0;

    virtual bool valid() const = 0;
    virtual operator bool() const {
        return valid();
    }

    virtual LogID logId() const = 0;
    virtual std::string logMsg() const = 0;
};

}  // namespace consensus
}  // namespace vgraph
}  // namespace vesoft
#endif  // CONSENSUS_LOGITERATOR_H_

