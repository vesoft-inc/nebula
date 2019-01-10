/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_SHOWSENTENCES_H_
#define PARSER_SHOWSENTENCES_H_

#include "base/Base.h"
#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class ShowAllHostSentence final : public Sentence {
 public:
    ShowAllHostSentence() {
        kind_ = Kind::kShowAllHost;
    }

     std::string toString() const override;
};

}   // namespace nebula

#endif  // PARSER_SHOWSENTENCES_H_
