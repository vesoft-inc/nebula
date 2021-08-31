/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_TIME_TIMEPARSER_H_
#define COMMON_TIME_TIMEPARSER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Date.h"

namespace nebula {
namespace time {

// parser the date/time/datetime literal
class TimeParser {
 public:
  TimeParser() = default;

  StatusOr<Date> parseDate(folly::StringPiece str) {
    ctx_.type = ExpectType::kDate;
    NG_RETURN_IF_ERROR(lex(str));
    NG_RETURN_IF_ERROR(parse());
    return Date{static_cast<int16_t>(ctx_.result.year),
                static_cast<int8_t>(ctx_.result.month),
                static_cast<int8_t>(ctx_.result.day)};
  }

  StatusOr<Time> parseTime(folly::StringPiece str) {
    ctx_.type = ExpectType::kTime;
    NG_RETURN_IF_ERROR(lex(str));
    NG_RETURN_IF_ERROR(parse());
    return Time{static_cast<int8_t>(ctx_.result.hour),
                static_cast<int8_t>(ctx_.result.minute),
                static_cast<int8_t>(ctx_.result.sec),
                static_cast<int32_t>(ctx_.result.microsec)};
  }

  StatusOr<DateTime> parseDateTime(folly::StringPiece str) {
    ctx_.type = ExpectType::kDateTime;
    NG_RETURN_IF_ERROR(lex(str));
    NG_RETURN_IF_ERROR(parse());
    return ctx_.result;
  }

 private:
  static constexpr char kTimeDelimiter = ':';

  static constexpr char kTimePrefix = 'T';
  static constexpr char kTimeSpacePrefix = ' ';
  static constexpr char kFractionPrefix = '.';
  static constexpr char kPlus = '+';
  static constexpr char kMinus = '-';

  static constexpr char kLeftBracket = '[';
  static constexpr char kRightBracket = ']';

  enum class ExpectType {
    kDate,
    kTime,
    kDateTime,
  };

  enum class TokenType {
    kUnknown,
    kPlaceHolder,  // Only for read-ahead placeholder
    kNumber,
    kPlus,
    kMinus,
    kTimeDelimiter,
    kTimePrefix,
    kTimeZoneName,
  };

  static const char* toString(TokenType t);

  struct Token {
    TokenType type;
    double val;  // only used for number token
    std::string str;
  };

  static std::string toString(const Token& t);

  // TODO(shylock) support weeks/days format when support the convertion
  // The state of current parser
  enum State {
    kInitial,          // 0
    kDateYear,         // 1
    kDateMonth,        // 2
    kDateDay,          // 3
    kTimeHour,         // 4
    kTimeMinute,       // 5
    kTimeSecond,       // 6
    kUtcOffset,        // 8
    kUtcOffsetHour,    // 9
    kUtcOffsetMinute,  // 10
    kTimeZone,         // 11
    kEnd,              // 12
    kSize,             // Just for count
  };

  static const char* toString(State state);

  struct Context {
    DateTime result;
    ExpectType type;

    TokenType utcSign{TokenType::kUnknown};
    int32_t utcOffsetSecs{0};
  };

  struct Component {
    const State state;  // current state
    // State shift function
    std::function<StatusOr<State>(Token, Token, Context&)> next;
  };

  // All states of datetime
  static const std::vector<Component> dateTimeStates;

  Status lex(folly::StringPiece str);

  Status parse();

 private:
  std::vector<Token> tokens_;
  Context ctx_;
};

}  // namespace time
}  // namespace nebula

#endif  // COMMON_TIME_TIMEPARSER_H_
