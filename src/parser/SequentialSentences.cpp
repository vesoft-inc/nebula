/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "parser/SequentialSentences.h"

namespace nebula {

std::string SequentialSentences::toString() const {
    std::string buf;
    buf.reserve(1024);
    for (auto& s : sentences_) {
        buf += s->toString();
        buf += ";";
    }
    return buf;
}

}   // namespace nebula
