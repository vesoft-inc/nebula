/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/thread/NamedThread.h"

#include "common/base/Base.h"

namespace nebula {
namespace thread {

namespace detail {

class TLSThreadID {
 public:
  TLSThreadID() {
    tid_ = ::syscall(SYS_gettid);
  }

  ~TLSThreadID() {
    tid_ = 0;
  }

  pid_t tid() {
    return tid_;
  }

 private:
  pid_t tid_;
};

}  // namespace detail

pid_t gettid() {
  static thread_local detail::TLSThreadID tlstid;
  return tlstid.tid();
}

}  // namespace thread
}  // namespace nebula
