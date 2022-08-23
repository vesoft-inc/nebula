/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_TIME_PARSER_DATETIMEREADER_H
#define COMMON_TIME_PARSER_DATETIMEREADER_H

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Date.h"
#include "common/time/parser/DatetimeParser.hpp"
#include "common/time/parser/DatetimeScanner.h"
#include "common/time/parser/Result.h"

namespace nebula {
namespace time {

class DatetimeReader {
 public:
  DatetimeReader();

  ~DatetimeReader() {
    if (result_ != nullptr) delete result_;
  }

  StatusOr<Result> readDatetime(std::string input) {
    input = kDatetimePrefix + input;
    return read(std::move(input));
  }

  StatusOr<Date> readDate(std::string input) {
    input = kDatePrefix + input;
    auto result = read(std::move(input));
    NG_RETURN_IF_ERROR(result);
    return result.value().dt.date();
  }

  StatusOr<TimeResult> readTime(std::string input) {
    input = kTimePrefix + input;
    auto result = read(std::move(input));
    NG_RETURN_IF_ERROR(result);
    return result.value().time();
  }

 private:
  StatusOr<Result> read(std::string input);

  std::string buffer_;
  const char *pos_{nullptr};
  const char *end_{nullptr};
  DatetimeScanner scanner_;
  DatetimeParser parser_;
  std::string error_;
  Result *result_{nullptr};

  inline static const std::string kDatetimePrefix = "DATETIME__";
  inline static const std::string kDatePrefix = "DATE__";
  inline static const std::string kTimePrefix = "TIME__";
};

}  // namespace time
}  // namespace nebula
#endif
