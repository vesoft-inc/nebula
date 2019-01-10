/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/ShowSentences.h"

namespace nebula {


std::string ShowAllHostSentence::toString() const {
    std::string buf = "SHOW ALL HOST";
    return buf;
}

}   // namespace nebula
