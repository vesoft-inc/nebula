/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "parser/Statement.h"

namespace vesoft {

std::string Statement::toString() const {
    std::string buf;
    buf.reserve(1024);
    auto i = 0UL;
    buf += sentences_[i++]->toString();
    for ( ; i < sentences_.size(); i++) {
        buf += "; ";
        buf += sentences_[i]->toString();
    }
    return buf;
}

}   // namespace vesoft
