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
    double                                  doubleVal;
    Geometry*                               geomVal;
    Point*                                  pointVal;
    LineString*                             lineVal;
    Polygon*                                polygonVal;
    Coordinate*                             coordVal;
    std::vector<Coordinate>*                coordListVal;
    std::vector<std::vector<Coordinate>>*   coordListListVal;
}

/* destructors */
%destructor {} <geomVal> <pointVal> <lineVal> <polygonVal>
%destructor {} <coordVal> <coordListVal> <coordListListVal>
%destructor {} <doubleVal>

/* wkt shape type prefix */
%token KW_POINT KW_LINESTRING KW_POLYGON

/* symbols */
%token L_PAREN R_PAREN COMMA

/* token type specification */
%token <doubleVal> DOUBLE

%type <geomVal> geometry
%type <pointVal> point
%type <lineVal> linestring
%type <polygonVal> polygon
%type <coordVal> coordinate
%type <coordListVal> coordinate_list
%type <coordListListVal> coordinate_list_list

%define api.prefix {wkt}

%start geometry

%%

geometry
  : point {
    $$ = $1;
    LOG(INFO) << "jie test" << static_cast<Point*>($$)->coord.x << static_cast<Point*>($$)->coord.y;
    *geom = $$;
  }
  | linestring {
    $$ = $1;
    *geom = $$;
  }
  | polygon {
    $$ = $1;
    *geom = $$;
  }
;

point
  : KW_POINT L_PAREN coordinate R_PAREN {
      $$ = new Point();
      $$->coord = *$3;
  }
  ;

linestring
  : KW_LINESTRING L_PAREN coordinate_list R_PAREN {
      $$ = new LineString();
      $$->coordList = *$3;
  }
  ;

polygon
  : KW_POLYGON L_PAREN coordinate_list_list R_PAREN {
      $$ = new Polygon();
      $$->coordListList = *$3;
  }
  ;

coordinate
  : DOUBLE DOUBLE {
      $$ = new Coordinate();
      $$->x = $1;
      $$->y = $2;
  }
  ;

coordinate_list
  : coordinate {
      $$ = new std::vector<Coordinate>();
      $$->emplace_back(*$1);
  }
  | coordinate_list COMMA coordinate {
      $$ = $1;
      $$->emplace_back(*$3);
  }
  ;

coordinate_list_list
  : L_PAREN coordinate_list R_PAREN {
      $$ = new std::vector<std::vector<Coordinate>>();
      $$->emplace_back(*$2);

  }
  | coordinate_list_list COMMA L_PAREN coordinate_list R_PAREN {
      $$ = $1;
      $$->emplace_back(*$4);
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

