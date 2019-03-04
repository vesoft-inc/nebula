/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "parser/ShowSentences.h"

namespace nebula {

std::string ShowSentence::toString() const {
    std::string buf;
    switch (showKind_) {
        case ShowKind::kShowHosts:
            buf = "SHOW HOSTS";
            break;
        case ShowKind::kUnknown:
        default:
            LOG(FATAL) << "Show Sentence kind illegal: " << showKind_;
            break;
    }
    return buf;
}

}   // namespace nebula
