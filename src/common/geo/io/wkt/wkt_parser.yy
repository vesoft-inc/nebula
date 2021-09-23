%language "C++"
%skeleton "lalr1.cc"
%no-lines
%locations
%define api.namespace { nebula }
%define parser_class_name { WKTParser }
%lex-param { nebula::WKTScanner& scanner }
%parse-param { nebula::WKTScanner& scanner }
%parse-param { std::string &errmsg }
%parse-param { nebula::Geometry** geom }

%code requires {
#include <iostream>
#include <sstream>
#include <string>
#include <cstddef>
#include "common/geo/io/Geometry.h"

namespace nebula {

class WKTScanner;

}

}

%code {
    #include "WKTScanner.h"
    static int yylex(nebula::WKTParser::semantic_type* yylval,
                     nebula::WKTParser::location_type *yylloc,
                     nebula::WKTScanner& scanner);
}

%union {
    double                                  doubleval;
    Geometry*                               geomval;
    Point*                                  pointval;
    LineString*                             lineval;
    Polygon*                                polygonval;
}

/* destructors */
%destructor {} <geomval>
%destructor {} <doubleval>

/* wkt shape type prefix */
%token KW_POINT KW_LINESTRING KW_POLYGON

/* symbols */
%token L_PAREN R_PAREN COMMA

/* token type specification */
%token <doubleval> DOUBLE

%type <geomval> geometry
%type <pointval> point coordinate
%type <lineval> linestring coordinate_list
%type <polygonval> polygon coordinate_list_list

%start geometry

%%

geometry
  : point {
    $$ = $1;
  }
  | linestring {
    $$ = $1;
  }
  | polygon {
    $$ = $1;
  }
;

point
  : KW_POINT L_PAREN coordinate R_PAREN {
      $$ = $3;
  }
  ;

linestring
  : KW_LINESTRING L_PAREN coordinate_list R_PAREN {
      $$ = $3;
  }
  ;

polygon
  : KW_POLYGON L_PAREN coordinate_list_list R_PAREN {
      $$ = $3;
  }
  ;

coordinate
  : DOUBLE DOUBLE {
      $$ = new Point();
      $$->x = $1;
      $$->y = $2;
  }
  ;

coordinate_list
  : coordinate {
      $$ = new LineString();
      $$->points.emplace_back(*$1);
  }
  | coordinate_list COMMA coordinate {
      $$ = $1;
      $$->points.emplace_back(*$3);
  }
  ;

coordinate_list_list
  : L_PAREN coordinate_list R_PAREN {
      $$ = new Polygon();
      $$->rings.emplace_back(*$2);

  }
  | coordinate_list_list COMMA L_PAREN coordinate_list R_PAREN {
      $$ = $1;
      $$->rings.emplace_back(*$4);
  }
  ;

%%

void nebula::WKTParser::error(const nebula::WKTParser::location_type& loc,
                                const std::string &msg) {
    std::ostringstream os;
    if (msg.empty()) {
        os << "syntax error";
    } else {
        os << msg;
    }

    auto *wkt = scanner.wkt();
    if (wkt == nullptr) {
        os << " at " << loc;
        errmsg = os.str();
        return;
    }

    auto begin = loc.begin.column > 0 ? loc.begin.column - 1 : 0;
    if ((loc.end.filename
        && (!loc.begin.filename
            || *loc.begin.filename != *loc.end.filename))
        || loc.begin.line < loc.end.line
        || begin >= wkt->size()) {
        os << " at " << loc;
    } else if (loc.begin.column < (loc.end.column ? loc.end.column - 1 : 0)) {
        uint32_t len = loc.end.column - loc.begin.column;
        if (len > 80) {
            len = 80;
        }
        os << " near `" << wkt->substr(begin, len) << "'";
    } else {
        os << " near `" << wkt->substr(begin, 8) << "'";
    }

    errmsg = os.str();
}

static int yylex(nebula::WKTParser::semantic_type* yylval,
                 nebula::WKTParser::location_type *yylloc,
                 nebula::WKTScanner& scanner) {
    auto token = scanner.yylex(yylval, yylloc);
    return token;
}

