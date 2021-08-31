/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/time/TimeParser.h"

#include <sstream>

#include "common/time/TimeConversion.h"
#include "common/time/TimeUtils.h"
#include "common/time/TimezoneInfo.h"

namespace nebula {
namespace time {

/*static*/ const std::vector<TimeParser::Component> TimeParser::dateTimeStates = {
    // State, ShiftMap
    {kInitial,
     [](Token, Token, Context &ctx) -> StatusOr<State> {
       if (ctx.type == ExpectType::kDateTime) {
         return kDateYear;
       } else if (ctx.type == ExpectType::kDate) {
         return kDateYear;
       } else if (ctx.type == ExpectType::kTime) {
         return kTimeHour;
       }
       return Status::Error("Unknown time ctx.type.");
     }},
    {kDateYear,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       if (t.type == TokenType::kNumber) {
         if (t.val < std::numeric_limits<int16_t>::min() ||
             t.val > std::numeric_limits<int16_t>::max()) {
           return Status::Error("The year number `%d' exceed the number limit.",
                                static_cast<uint32_t>(t.val));
         }
         ctx.result.year = t.val;
         switch (n.type) {
           case TokenType::kMinus:
             return kDateMonth;
           case TokenType::kTimePrefix:
             if (ctx.type == ExpectType::kDate) {
               return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
             }
             return kTimeHour;
           case TokenType::kPlaceHolder:
             return kEnd;
           default:
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
         }
       } else {
         return Status::Error("Unexpected token `%s'.", toString(n.type));
       }
     }},
    {kDateMonth,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       if (t.type == TokenType::kMinus) {
         return kDateMonth;
       }
       if (t.type == TokenType::kNumber) {
         if (t.val < 1 || t.val > 12) {
           return Status::Error("The month number `%d' exceed the number limit.",
                                static_cast<uint32_t>(t.val));
         }
         ctx.result.month = t.val;
         switch (n.type) {
           case TokenType::kMinus:
             return kDateDay;
           case TokenType::kTimePrefix:
             if (ctx.type == ExpectType::kDate) {
               return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
             }
             return kTimeHour;
           case TokenType::kPlaceHolder:
             return kEnd;
           default:
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
         }
       } else {
         return Status::Error("Unexpected token `%s'.", toString(n.type));
       }
     }},
    {kDateDay,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       if (t.type == TokenType::kMinus) {
         return kDateDay;
       }
       if (t.type == TokenType::kNumber) {
         if (t.val < 1 || t.val > 31) {
           return Status::Error("The day number `%d' exceed the number limit.",
                                static_cast<uint32_t>(t.val));
         }
         ctx.result.day = t.val;
         switch (n.type) {
           case TokenType::kTimePrefix:
             if (ctx.type == ExpectType::kDate) {
               return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
             }
             return kTimeHour;
           case TokenType::kPlaceHolder:
             return kEnd;
           default:
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
         }
       } else {
         return Status::Error("Unexpected token `%s'.", toString(n.type));
       }
     }},
    {kTimeHour,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       if (t.type == TokenType::kTimePrefix) {
         return kTimeHour;
       }
       if (t.type == TokenType::kNumber) {
         if (t.val < 0 || t.val > 23) {
           return Status::Error("The hour number `%d' exceed the number limit.",
                                static_cast<uint32_t>(t.val));
         }
         ctx.result.hour = t.val;
         switch (n.type) {
           case TokenType::kTimeDelimiter:
             return kTimeMinute;
           case TokenType::kPlus:
           case TokenType::kMinus:
             return kUtcOffset;
           case TokenType::kTimeZoneName:
             return kTimeZone;
           case TokenType::kPlaceHolder:
             return kEnd;
           default:
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
         }
       } else {
         return Status::Error("Unexpected token `%s'.", toString(n.type));
       }
       return Status::OK();
     }},
    {kTimeMinute,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       if (t.type == TokenType::kTimeDelimiter) {
         return kTimeMinute;
       }
       if (t.type == TokenType::kNumber) {
         if (t.val < 0 || t.val > 59) {
           return Status::Error("The minute number `%d' exceed the number limit.",
                                static_cast<uint32_t>(t.val));
         }
         ctx.result.minute = t.val;
         switch (n.type) {
           case TokenType::kTimeDelimiter:
             return kTimeSecond;
           case TokenType::kPlus:
           case TokenType::kMinus:
             return kUtcOffset;
           case TokenType::kTimeZoneName:
             return kTimeZone;
           case TokenType::kPlaceHolder:
             return kEnd;
           default:
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
         }
       } else {
         return Status::Error("Unexpected token `%s'.", toString(n.type));
       }
       return Status::OK();
     }},
    {kTimeSecond,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       if (t.type == TokenType::kTimeDelimiter) {
         return kTimeSecond;
       }
       if (t.type == TokenType::kNumber) {
         if (t.val < 0 || t.val >= 60) {
           return Status::Error("The second number `%f' exceed the number limit.", t.val);
         }
         double integer{0};
         double fraction = std::modf(t.val, &integer);
         ctx.result.sec = integer;
         ctx.result.microsec = std::round(fraction * 1000000);
         switch (n.type) {
           case TokenType::kPlus:
           case TokenType::kMinus:
             return kUtcOffset;
           case TokenType::kTimeZoneName:
             return kTimeZone;
           case TokenType::kPlaceHolder:
             return kEnd;
           default:
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
         }
       } else {
         return Status::Error("Unexpected token `%s'.", toString(n.type));
       }
       return Status::OK();
     }},
    {kUtcOffset,
     [](Token t, Token, Context &ctx) -> StatusOr<State> {
       switch (t.type) {
         case TokenType::kPlus:
         case TokenType::kMinus:
           if (ctx.utcSign != TokenType::kUnknown) {
             return Status::Error("Unexpected token `%s'.", toString(t.type));
           }
           ctx.utcSign = t.type;
           return kUtcOffsetHour;
         default:
           return Status::Error("Unexpected token `%s'.", toString(t.type));
       }
     }},
    {kUtcOffsetHour,
     [](Token t, Token, Context &ctx) -> StatusOr<State> {
       switch (t.type) {
         case TokenType::kNumber:
           if (t.val > 23) {
             return Status::Error("Unexpected utc offset hours number `%d'.",
                                  static_cast<uint32_t>(t.val));
           }
           ctx.utcOffsetSecs =
               (ctx.utcSign == TokenType::kPlus ? t.val * 60 * 60 : -t.val * 60 * 60);
           return kUtcOffsetMinute;
         default:
           return Status::Error("Unexpected token `%s'.", toString(t.type));
       }
     }},
    {kUtcOffsetMinute,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       switch (t.type) {
         case TokenType::kTimeDelimiter:
           if (n.type != TokenType::kNumber) {
             return Status::Error("Unexpected read-ahead token `%s'.", toString(n.type));
           }
           return kUtcOffsetMinute;
         case TokenType::kNumber:
           if (t.val > 59) {
             return Status::Error("Unexpected utc offset minutes number `%d'.",
                                  static_cast<uint32_t>(t.val));
           }
           ctx.utcOffsetSecs += (ctx.utcSign == TokenType::kPlus ? t.val * 60 : -t.val * 60);
           if (n.type == TokenType::kPlaceHolder) {
             ctx.result = TimeConversion::dateTimeShift(ctx.result, -ctx.utcOffsetSecs);
             return kEnd;
           } else if (n.type == TokenType::kTimeZoneName) {
             return kTimeZone;
           } else {
             return Status::Error("Unexpected read-head token `%s'.", toString(n.type));
           }
         default:
           return Status::Error("Unexpected token `%s'.", toString(t.type));
       }
     }},
    {kTimeZone,
     [](Token t, Token n, Context &ctx) -> StatusOr<State> {
       DCHECK(t.type == TokenType::kTimeZoneName);
       if (n.type != TokenType::kPlaceHolder) {
         return Status::Error("Unexpected read-head token `%s'.", toString(n.type));
       }
       int32_t utcOffsetSecs = 0;
       Timezone tz;
       auto result = tz.loadFromDb(t.str);
       NG_RETURN_IF_ERROR(result);
       if (ctx.utcSign != TokenType::kUnknown) {
         if (tz.utcOffsetSecs() != ctx.utcOffsetSecs) {
           return Status::Error("Mismatched time zone offset and time zone name.");
         } else {
           utcOffsetSecs = ctx.utcOffsetSecs;
         }
       } else {
         utcOffsetSecs = tz.utcOffsetSecs();
       }
       ctx.result = TimeConversion::dateTimeShift(ctx.result, -utcOffsetSecs);
       return kEnd;
     }},
    {kEnd, [](Token, Token, Context &) -> StatusOr<State> { return Status::OK(); }},
};

/*static*/ const char *TimeParser::toString(TokenType t) {
  switch (t) {
    case TokenType::kUnknown:
      return "Unknown";
    case TokenType::kPlaceHolder:
      return "PlaceHolder";
    case TokenType::kNumber:
      return "Number";
    case TokenType::kPlus:
      return "+";
    case TokenType::kMinus:
      return "-";
    case TokenType::kTimeDelimiter:
      return "TimeDelimiter";
    case TokenType::kTimePrefix:
      return "TimePrefix";
    case TokenType::kTimeZoneName:
      return "TimeZoneName";
  }
  LOG(FATAL) << "Unknown token " << static_cast<int>(t);
  return "Unknown token";
}

/*static*/ std::string TimeParser::toString(const Token &t) {
  std::stringstream ss;
  ss << toString(t.type);
  ss << "(";
  if (t.type == TokenType::kNumber) {
    ss << t.val;
  }
  if (t.type == TokenType::kTimeZoneName) {
    ss << t.str;
  }
  ss << ")";
  return ss.str();
}

/*static*/ const char *TimeParser::toString(State state) {
  switch (state) {
    case kInitial:
      return "Initial";
    case kDateYear:
      return "DateYear";
    case kDateMonth:
      return "DateMonth";
    case kDateDay:
      return "DateDay";
    case kTimeHour:
      return "TimeHour";
    case kTimeMinute:
      return "TimeMinute";
    case kTimeSecond:
      return "TimeSecond";
    case kUtcOffset:
      return "UtcOffset";
    case kUtcOffsetHour:
      return "UtcOffsetHour";
    case kUtcOffsetMinute:
      return "UtcOffsetMinute";
    case kTimeZone:
      return "TimeZone";
    case kEnd:
      return "End";
    case kSize:
      return "Size";
  }
  DLOG(FATAL) << "Unknown state " << static_cast<int>(state);
  return "Unknown";
}

Status TimeParser::lex(folly::StringPiece str) {
  tokens_.reserve(8);
  std::string digits;
  digits.reserve(8);
  auto c = str.start();
  while (*c != '\0') {
    if (std::isdigit(*c) || kFractionPrefix == *c) {
      digits.push_back(*c);
      if (!(std::isdigit(*(c + 1)) || kFractionPrefix == *(c + 1))) {
        if (digits.front() == '.') {
          return Status::Error("Unexpected character `%c'.", digits.front());
        }
        if (digits.back() == '.') {
          return Status::Error("Expected character fraction.");
        }
        Token t;
        try {
          t.val = folly::to<double>(digits);
        } catch (std::exception &e) {
          return Status::Error("%s", e.what());
        }
        t.type = TokenType::kNumber;
        tokens_.emplace_back(t);
        digits.clear();
      }
    } else if (kTimeDelimiter == *c) {
      tokens_.emplace_back(Token{TokenType::kTimeDelimiter, 0, ""});
    } else if (kTimePrefix == *c || kTimeSpacePrefix == *c) {
      tokens_.emplace_back(Token{TokenType::kTimePrefix, 0, ""});
    } else if (kPlus == *c) {
      tokens_.emplace_back(Token{TokenType::kPlus, 0, ""});
    } else if (kMinus == *c) {
      tokens_.emplace_back(Token{TokenType::kMinus, 0, ""});
    } else if (kLeftBracket == *c) {
      std::string s;
      while (*(++c) != kRightBracket) {
        if (*c == '\0') {
          return Status::Error("Unterminated bracket.");
        }
        s.push_back(*c);
      }
      tokens_.emplace_back(Token{TokenType::kTimeZoneName, 0, std::move(s)});
    } else {
      return Status::Error("Illegal character `%c'.", *c);
    }
    ++c;
  }
  // Only for read-ahead placeholder
  tokens_.emplace_back(Token{TokenType::kPlaceHolder, 0, ""});
  return Status::OK();
}

Status TimeParser::parse() {
  auto result = dateTimeStates[kInitial].next({}, {}, ctx_);
  NG_RETURN_IF_ERROR(result);
  auto current = result.value();
  for (std::size_t i = 0; i < tokens_.size() - 1; ++i) {
    result = dateTimeStates[current].next(tokens_[i], tokens_[i + 1], ctx_);
    NG_RETURN_IF_ERROR(result);
    current = result.value();
    if (current == kEnd) {
      break;
    }
  }
  if (current != kEnd) {
    return Status::Error("Not end parse.");
  }
  return Status::OK();
}

}  // namespace time
}  // namespace nebula
