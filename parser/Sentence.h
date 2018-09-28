/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_SENTENCE_H_
#define PARSER_SENTENCE_H_

#include "base/Base.h"
#include "parser/Expressions.h"

namespace vesoft {

class Sentence {
public:
    virtual ~Sentence() {}
    virtual std::string toString() const = 0;
};

}   // namespace vesoft

#endif  // PARSER_SENTENCE_H_
