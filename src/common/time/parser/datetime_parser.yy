%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula::time }
%define api.parser.class { DatetimeParser }
%lex-param { nebula::time::DatetimeScanner& scanner }
%parse-param { nebula::time::DatetimeScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::DateTime** output }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include <cstddef>
#include "common/datatypes/Date.h"
#include "common/time/TimeConversion.h"
#include "common/time/TimezoneInfo.h"
#include "common/time/TimeUtils.h"

namespace nebula {
namespace time {
class DatetimeScanner;
}
}

}

%code {
    #include "common/time/parser/DatetimeScanner.h"
    static int yylex(nebula::time::DatetimeParser::semantic_type* yylval,
                     nebula::time::DatetimeParser::location_type *yylloc,
                     nebula::time::DatetimeScanner& scanner);
}

%union {
    int64_t                                 intVal;
    double                                  doubleVal;
    nebula::DateTime                       *dtVal;
    nebula::Date                           *dVal;
    nebula::Time                           *tVal;
    std::string                            *strVal;
}

/* destructors */
%destructor {} <intVal> <doubleVal>
%destructor {} <dtVal>  // for output
%destructor { delete $$; } <*>

/* keyword */
%token KW_TIME_ID

/* symbols */
%token TIME_DELIMITER SPACE POSITIVE NEGATIVE KW_DATETIME KW_DATE KW_TIME

/* token type specification */
%token <intVal> INTEGER
%token <strVal> TIME_ZONE_NAME
%token <doubleVal> DOUBLE

%type <dtVal> datetime
%type <dVal> date
%type <tVal> time
%type <intVal> opt_time_zone time_zone_offset opt_time_zone_offset opt_time_zone_name

%define api.prefix {datetime}

%start datetime

%%

datetime
  : KW_DATETIME date date_time_delimiter time opt_time_zone {
    $$ = new DateTime(TimeConversion::dateTimeShift(DateTime(*$2, *$4), -$5));
    *output = $$;
    delete $2;
    delete $4;
  }
  | KW_DATETIME date {
    $$ = new DateTime(*$2);
    *output = $$;
    delete $2;
  }
  | KW_DATE date {
    $$ = new DateTime(*$2);
    *output = $$;
    delete $2;
  }
  | KW_TIME time opt_time_zone {
    $$ = new DateTime(TimeConversion::dateTimeShift(DateTime(1970, 1, 1, $2->hour, $2->minute, $2->sec, $2->microsec), -$3));
    *output = $$;
    delete $2;
  }
  ;

date_time_delimiter
  : KW_TIME_ID
  | SPACE
  ;

date
  : INTEGER NEGATIVE INTEGER NEGATIVE INTEGER {
    $$ = new nebula::Date($1, $3, $5);
    auto result = nebula::time::TimeUtils::validateDate(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  | INTEGER NEGATIVE INTEGER {
    $$ = new nebula::Date($1, $3, 1);
    auto result = nebula::time::TimeUtils::validateDate(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  | INTEGER {
    $$ = new nebula::Date($1, 1, 1);
    auto result = nebula::time::TimeUtils::validateDate(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  ;

time
  : INTEGER TIME_DELIMITER INTEGER TIME_DELIMITER DOUBLE {
    double integer = 0;
    auto fraction = std::modf($5, &integer);
    $$ = new nebula::Time($1, $3, static_cast<int>(integer), std::round(fraction * 1000 * 1000));
    auto result = nebula::time::TimeUtils::validateTime(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  | INTEGER TIME_DELIMITER INTEGER TIME_DELIMITER INTEGER {
    $$ = new nebula::Time($1, $3, $5, 0);
    auto result = nebula::time::TimeUtils::validateTime(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  | INTEGER TIME_DELIMITER INTEGER {
    $$ = new nebula::Time($1, $3, 0, 0);
    auto result = nebula::time::TimeUtils::validateTime(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  | INTEGER {
    $$ = new nebula::Time($1, 0, 0, 0);
    auto result = nebula::time::TimeUtils::validateTime(*$$);
    if (!result.ok()) {
      delete $$;
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
  }
  ;

opt_time_zone
  : %empty {
    $$ = 0;
  }
  | opt_time_zone_offset opt_time_zone_name {
    if ($1 != $2) {
      throw DatetimeParser::syntax_error(@1, "Mismatched timezone offset.");
    }
    $$ = $1;
  }
  | opt_time_zone_offset {
    $$ = $1;
  }
  | opt_time_zone_name {
    $$ = $1;
  }
  ;

opt_time_zone_offset
  : POSITIVE time_zone_offset {
    $$ = $2;
  }
  | NEGATIVE time_zone_offset {
    $$ = -$2;
  }
  ;

time_zone_offset
  : INTEGER TIME_DELIMITER INTEGER {
    auto time = nebula::Time($1, $3, 0, 0);
    auto result = nebula::time::TimeUtils::validateTime(time);
    if (!result.ok()) {
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
    $$ = nebula::time::TimeConversion::timeToSeconds(time);
  }
  ;

opt_time_zone_name
  : TIME_ZONE_NAME {
    auto zone = nebula::time::Timezone();
    auto result = zone.loadFromDb(*$1);
    if (!result.ok()) {
      throw DatetimeParser::syntax_error(@1, result.toString());
    }
    $$ = zone.utcOffsetSecs();
    delete $1;
  }
  ;

%%

void nebula::time::DatetimeParser::error(const nebula::time::DatetimeParser::location_type& loc,
                                         const std::string &msg) {
    std::ostringstream os;
    if (msg.empty()) {
        os << "syntax error";
    } else {
        os << msg;
    }

    auto *input = scanner.input();
    if (input == nullptr) {
        os << " at " << loc;
        errmsg = os.str();
        return;
    }

    auto begin = loc.begin.column > 0 ? loc.begin.column - 1 : 0;
    if ((loc.end.filename
        && (!loc.begin.filename
            || *loc.begin.filename != *loc.end.filename))
        || loc.begin.line < loc.end.line
        || begin >= input->size()) {
        os << " at " << loc;
    } else if (loc.begin.column < (loc.end.column ? loc.end.column - 1 : 0)) {
        uint32_t len = loc.end.column - loc.begin.column;
        if (len > 80) {
            len = 80;
        }
        os << " near `" << input->substr(begin, len) << "'";
    } else {
        os << " near `" << input->substr(begin, 8) << "'";
    }

    errmsg = os.str();
}

static int yylex(nebula::time::DatetimeParser::semantic_type* yylval,
                 nebula::time::DatetimeParser::location_type *yylloc,
                 nebula::time::DatetimeScanner& scanner) {
    auto token = scanner.yylex(yylval, yylloc);
    return token;
}

