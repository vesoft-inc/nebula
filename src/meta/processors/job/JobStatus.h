/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_JOBSTATUS_H_
#define META_JOBSTATUS_H_

#include <string>
#include <vector>

#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

class JobStatus {
  using Status = cpp2::JobStatus;

 public:
  static std::string toString(Status st);
  static bool laterThan(Status lhs, Status rhs);

 private:
  static int phaseNumber(Status st);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBSTATUS_H_
