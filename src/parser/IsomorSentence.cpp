/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/IsomorSentence.h"

#include <stdio.h>
namespace nebula {
std::string IsomorSentence::toString() const {
  std::string buf;
  buf.reserve(256);
  buf += "ISOMOR";
  buf += " ";
  if (tags_->empty()) {
    buf += "*";
  } else {
    buf += tags_->toString();
  }
  printf("Here is the string: %s \n", buf);
  return buf;
}
}  // namespace nebula
