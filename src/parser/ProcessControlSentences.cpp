/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/ProcessControlSentences.h"

namespace nebula {
std::string ReturnSentence::toString() const {
  std::string buf;
  buf.reserve(256);

  buf += "RETURN $";
  if (var_ != nullptr) {
    buf += *var_;
  }
  if (condition_ != nullptr) {
    buf += " IF $";
    buf += *condition_;
    buf += " IS NOT NULL";
  }
  return buf;
}
}  // namespace nebula
