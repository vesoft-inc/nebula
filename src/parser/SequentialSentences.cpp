/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/SequentialSentences.h"

#include <ext/alloc_traits.h>  // for __alloc_traits<>::value_type

#include "Sentence.h"  // for Sentence

namespace nebula {

std::string SequentialSentences::toString() const {
  std::string buf;
  buf.reserve(1024);
  auto i = 0UL;
  buf += sentences_[i++]->toString();
  for (; i < sentences_.size(); i++) {
    buf += "; ";
    buf += sentences_[i]->toString();
  }
  return buf;
}

}  // namespace nebula
