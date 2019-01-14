/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/ShowSentences.h"

namespace nebula {

std::string ShowSentence::toString() const {
    switch (showKind_) {
        case ShowKind::kShowHosts:
            std::string buf = "SHOW HOSTS";
            break;
        default:
            LOG(FATAL) << "Show Sentence kind illegal: " << kind_;
            break;
    }
    return buf;
}

}   // namespace nebula
