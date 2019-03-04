/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/LoadSentences.h"

namespace nebula {

std::string LoadSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    switch (loadKind_) {
        case LoadKind::kLoadVertex:
            buf += "Load Vertex ";
            buf += *path_;
            break;
        case LoadKind::kLoadEdge:
            buf += "Load Edge ";
            buf += *path_;
            break;
        case LoadKind::kUnknown:
        default:
            LOG(FATAL) << "Load Sentence kind illegal: " << loadKind_;
            break;
    }
    return buf;
}

}  // namespace nebula
