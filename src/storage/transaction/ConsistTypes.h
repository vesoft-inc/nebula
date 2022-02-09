/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CONSISTTYPES_H
#define STORAGE_TRANSACTION_CONSISTTYPES_H

#include <optional>

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

enum class RequestType {
  UNKNOWN = 0,
  INSERT,
  UPDATE,
  DELETE,
};

enum class ResumeType {
  UNKNOWN = 0,
  RESUME_CHAIN,
  RESUME_REMOTE,
};

struct ResumeOptions {
  ResumeOptions(ResumeType tp, std::string val) : resumeType(tp), primeValue(std::move(val)) {}
  ResumeType resumeType;
  std::string primeValue;
};

struct HookFuncPara {
  std::optional<std::vector<std::string>*> keys;
  std::optional<::nebula::kvstore::BatchHolder*> batch;
  std::optional<std::string> result;
};

}  // namespace storage
}  // namespace nebula
#endif
