/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/MockData.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "common/time/WallClock.h"
#include "common/expression/ConstantExpression.h"
#include "utils/IndexKeyUtils.h"

DEFINE_bool(mock_ttl_col, false, "Will use a column as ttl_col if set to true");
DEFINE_int32(mock_ttl_duration, 5, "Ttl duration for ttl col");

namespace nebula {
namespace mock {

// 30 teams
std::vector<std::string> MockData::teams_ = {
    "Warriors",
    "Nuggets",
    "Rockets",
    "Trail Blazers",
    "Spurs",
    "Thunders",
    "Jazz",
    "Clippers",
    "Kings",
    "Timberwolves",
    "Lakers",
    "Pelicans",
    "Grizzlies",
    "Mavericks",
    "Suns",
    "Hornets",
    "Cavaliers",
    "Celtics",
    "Raptors",
    "76ers",
    "Pacers",
    "Bulls",
    "Hawks",
    "Knicks",
    "Pistons",
    "Bucks",
    "Magic",
    "Nets",
    "Wizards",
    "Heat"
};

// Player name, age, playing, NBA career(years), start year, end year, games,
// career average score, served teams, country(default value America), champions(nullable)
// 51 players
std::vector<Player> MockData::players_ = {
    {"Tim Duncan",            44,  false,  19,  1997,  2016,  1392,  19.0,  1,  "America",   5},
    {"Tony Parker",           38,  false,  18,  2001,  2019,  1254,  15.5,  2,  "France",    4},
    {"LaMarcus Aldridge",     35,  true,   14,  2006,  2020,  1003,  19.5,  2,  "America"     },
    {"Rudy Gay",              34,  true,   14,  2006,  2020,   939,  17.1,  4,  "America"     },
    {"Marco Belinelli",       34,  true,   13,  2007,  2020,   855,   9.7,  9,  "Italy",     1},
    {"Danny Green",           33,  true,   11,  2009,  2020,   681,   8.9,  4,  "America",   2},
    {"Kyle Anderson",         27,  true,    6,  2014,  2020,   359,   5.4,  2,  "America"     },
    {"Aron Baynes",           34,  true,    8,  2012,  2020,   469,   6.0,  4,  "Australia", 1},
    {"Boris Diaw",            38,  false,  14,  2003,  2017,  1064,   8.6,  5,  "France",    1},
    {"Tiago Splitter",        36,  false,   7,  2010,  2017,   355,   7.9,  3,  "Brazil",    1},
    {"Cory Joseph",           29,  true,    9,  2011,  2020,   592,   6.9,  4,  "Canada",    1},
    {"David West",            40,  false,  15,  2003,  2018,  1034,  13.6,  4,  "America ",  2},
    {"Jonathon Simmons",      31,  false,   4,  2015,  2019,   258,   8.3,  3,  "America"     },
    {"Dejounte Murray",       24,  true,    3,  2016,  2020,   177,   7.9,  1,  "America"     },
    {"Tracy McGrady",         41,  false,  15,  1997,  2012,   938,  19.6,  6,  "America"     },
    {"Kobe Bryant",           41,  false,  20,  1996,  2016,  1346,  25.0,  1,  "America",   5},
    {"LeBron James",          35,  true,   17,  2003,  2020,  1258,  27.1,  3,  "America",   3},
    {"Stephen Curry",         32,  true,   11,  2009,  2020,   699,  23.5,  1,  "America",   3},
    {"Russell Westbrook",     31,  true,   12,  2008,  2020,   874,  23.2,  2,  "America"     },
    {"Kevin Durant",          31,  true,   12,  2007,  2020,   849,  27.0,  3,  "America",   2},
    {"James Harden",          30,  true,   11,  2009,  2020,   826,  25.1,  2,  "America"     },
    {"Chris Paul",            35,  true,   15,  2005,  2020,  1013,  18.5,  4,  "America"     },
    {"DeAndre Jordan",        31,  true,   12,  2008,  2020,   875,   9.5,  4,  "America"     },
    {"Ricky Rubio",           29,  true,    9,  2011,  2020,   555,  11.3,  3,  "Spain"       },
    {"Rajon Rondo",           34,  true,   14,  2006,  2020,   873,  10.2,  6,  "America",   1},
    {"Manu Ginobili",         42,  false,  16,  2002,  2018,  1057,  13.3,  1,  "Argentina", 4},
    {"Kyrie Irving",          28,  true,    9,  2011,  2020,   528,  22.4,  3,  "America",   1},
    {"Vince Carter",          43,  true,   22,  1998,  2020,  1541,  16.7,  8,  "America"     },
    {"Carmelo Anthony",       35,  true,   17,  2003,  2020,  1114,  23.6,  5,  "America"     },
    {"Dwyane Wade",           38,  false,  16,  2003,  2019,  1054,  22.0,  3,  "America",   3},
    {"Joel Embiid",           26,  true,    4,  2016,  2020,   202,  24.1,  1,  "Cameroon"    },
    {"Paul George",           30,  true,   10,  2010,  2020,   646,  19.9,  3,  "America"     },
    {"Giannis Antetokounmpo", 25,  true,    7,  2013,  2020,   522,  20.0,  1,  "Greece"      },
    {"Yao Ming",              39,  false,   8,  2002,  2011,   486,  19.0,  1,  "China"       },
    {"Blake Griffin",         31,  true,   10,  2010,  2020,   622,  21.7,  2,  "America"     },
    {"Damian Lillard",        29,  true,    8,  2012,  2020,   607,  24.0,  1,  "America"     },
    {"Steve Nash",            46,  false,  18,  1996,  2014,  1217,  14.3,  3,  "Canada"      },
    {"Dirk Nowitzki",         41,  false,  21,  1998,  2019,  1522,  20.7,  1,  "Germany",   1},
    {"Pau Gasol",             39,  false,  18,  2001,  2019,  1226,  17.0,  5,  "Spain",     2},
    {"Marc Gasol",            35,  true,   12,  2008,  2020,   831,  14.7,  2,  "Spain",     1},
    {"Grant Hill",            47,  false,  18,  1994,  2013,  1026,  16.7,  4,  "America"     },
    {"Ray Allen",             44,  false,  18,  1996,  2014,  1300,  18.9,  4,  "America",   2},
    {"Klay Thompson",         30,  true,    8,  2011,  2019,   615,  19.5,  1,  "America",   3},
    {"Kristaps Porzingis",    24,  true,    4,  2015,  2020,   237,  18.1,  2,  "Latvia"      },
    {"Shaquille O'Neal",      48,  false,  19,  1992,  2011,  1207,  23.7,  6,  "America",   4},
    {"JaVale McGee",          32,  true,   12,  2008,  2020,   694,   7.9,  6,  "America",   2},
    {"Jason Kidd",            47,  false,  19,  1994,  2013,  1391,  12.6,  4,  "America",   1},
    {"Ben Simmons",           23,  true,    3,  2017,  2020,   214,  16.4,  1,  "Australia"   },
    {"Luka Doncic",           21,  true,    2,  2018,  2020,   126,  24.4,  1,  "Slovenia"    },
    {"Dwight Howard",         34,  true,   16,  2004,  2020,  1106,  16.8,  6                 },
    {"Amare Stoudemire",      37,  false,  14,  2002,  2016,   846,  18.9,  4                 }
};


std::vector<Serve> MockData::serves_ = {
    {"Tim Duncan",            "Spurs",         1997, 2016, 19, 1392, 19.0, "zzzzz", 5},
    {"Tony Parker",           "Spurs",         2001, 2018, 17, 1198, 16.6, "trade", 4},
    {"Tony Parker",           "Hornets",       2018, 2019,  1,   56,  9.5            },
    {"LaMarcus Aldridge",     "Trail Blazers", 2006, 2015,  9,  648, 21.7            },
    {"LaMarcus Aldridge",     "Spurs",         2015, 2020,  5,  355, 18.9            },
    {"Rudy Gay",              "Grizzlies",     2006, 2013,  7,  479, 19.0            },
    {"Rudy Gay",              "Raptors",       2013, 2013,  1,   51, 19.5            },
    {"Rudy Gay",              "Kings",         2013, 2017,  4,  223, 21.1            },
    {"Rudy Gay",              "Spurs",         2017, 2020,  3,  186, 13.7            },
    {"Marco Belinelli",       "Warriors",      2007, 2009,  2,   75,  8.9            },
    {"Marco Belinelli",       "Raptors",       2009, 2010,  1,   66,  7.1            },
    {"Marco Belinelli",       "Hornets",       2010, 2012,  2,  146, 11.8            },
    {"Marco Belinelli",       "Bulls",         2012, 2013,  1,   73,  9.6            },
    {"Marco Belinelli",       "Spurs",         2013, 2015,  2,  142, 11.4, "trade", 1},
    {"Marco Belinelli",       "Kings",         2015, 2016,  1,   68, 10.2            },
    {"Marco Belinelli",       "Hornets",       2016, 2017,  1,   74, 10.5            },
    {"Marco Belinelli",       "Hawks",         2017, 2018,  1,   52, 11.4            },
    {"Marco Belinelli",       "76ers",         2018, 2018,  1,   28, 13.6            },
    {"Marco Belinelli",       "Spurs",         2018, 2020,  2,  131,  5.8            },
    {"Danny Green",           "Cavaliers",     2009, 2010,  1,   20,  2.0            },
    {"Danny Green",           "Spurs",         2010, 2018,  8,  520,  9.1, "trade", 1},
    {"Danny Green",           "Raptors",       2018, 2019,  1,   80, 10.3, "trade", 1},
    {"Danny Green",           "Lakers",        2019, 2020,  1,   61,  8.2,           },
    {"Kyle Anderson",         "Spurs",         2014, 2018,  4,  257,  4.5            },
    {"Kyle Anderson",         "Grizzlies",     2018, 2020,  2,  102,  5.7            },
    {"Aron Baynes",           "Spurs",         2012, 2015,  3,  139,  3.0, "trade", 1},
    {"Aron Baynes",           "Pistons",       2015, 2017,  2,  157,  6.3            },
    {"Aron Baynes",           "Celtics",       2017, 2019,  2,  132,  6.0            },
    {"Aron Baynes",           "Suns",          2019, 2020,  1,   42, 11.5            },
    {"Boris Diaw",            "Hawks",         2003, 2005,  2,  142,  4.8            },
    {"Boris Diaw",            "Suns",          2005, 2008,  4,  258,  8.8            },
    {"Boris Diaw",            "Hornets",       2008, 2012,  4,  260, 11.3            },
    {"Boris Diaw",            "Spurs",         2012, 2016,  4,  331,  8.7, "trade", 1},
    {"Boris Diaw",            "Jazz",          2016, 2017,  1,   73,  4.6            },
    {"Tiago Splitter",        "Spurs",         2010, 2015,  5,  311,  8.2, "trade", 1},
    {"Tiago Splitter",        "Hawks",         2015, 2016,  1,   36,  5.6            },
    {"Tiago Splitter",        "76ers",         2016, 2017,  1,    8,  4.9            },
    {"Cory Joseph",           "Spurs",         2011, 2015,  4,  204,  4.5, "trade", 1},
    {"Cory Joseph",           "Raptors",       2015, 2017,  2,  160,  9.3            },
    {"Cory Joseph",           "Pacers",        2017, 2019,  2,  164,  6.5            },
    {"Cory Joseph",           "Kings",         2019, 2020,  1,   64,  6.3            },
    {"David West",            "Hornets",       2003, 2011,  8,  530, 19.0            },
    {"David West",            "Pacers",        2011, 2015,  4,  285, 11.7            },
    {"David West",            "Spurs",         2015, 2016,  1,   78,  7.1            },
    {"David West",            "Warriors",      2016, 2018,  2,  141,  6.8, "trade", 2},
    {"Jonathon Simmons",      "Spurs",         2015, 2017,  2,  133,  6.2            },
    {"Jonathon Simmons",      "Magic",         2017, 2019,  2,  110, 13.9            },
    {"Jonathon Simmons",      "76ers",         2019, 2019,  1,   15,  5.5            },
    {"Dejounte Murray",       "Spurs",         2016, 2020,  3,  177,  7.9            },
    {"Tracy McGrady",         "Raptors",       1997, 2000,  3,  192, 15.4, "zzzzz"   },
    {"Tracy McGrady",         "Magic",         2000, 2004,  4,  295, 28.0            },
    {"Tracy McGrady",         "Rockets",       2004, 2010,  6,  303, 24.4            },
    {"Tracy McGrady",         "Knicks",        2010, 2010,  1,   24,  9.4            },
    {"Tracy McGrady",         "Pistons",       2010, 2011,  1,   72,  8.0            },
    {"Tracy McGrady",         "Hawks",         2011, 2012,  1,   52,  5.3            },
    {"Kobe Bryant",           "Lakers",        1996, 2016, 20, 1346, 25.0, "trade", 5},
    {"LeBron James",          "Cavaliers",     2003, 2010,  7,  548, 29.7, "zzzzz"   },
    {"LeBron James",          "Heat",          2010, 2014,  4,  294, 27.1, "trade", 2},
    {"LeBron James",          "Cavaliers",     2014, 2018,  4,  301, 27.5, "trade", 1},
    {"LeBron James",          "Lakers",        2018, 2020,  2,  115, 25.7            },
    {"Stephen Curry",         "Warriors",      2009, 2020, 11,  699, 23.5, "zzzzz", 3},
    {"Russell Westbrook",     "Thunders",      2008, 2019, 11,  821, 22.9,           },
    {"Russell Westbrook",     "Rockets",       2019, 2020,  1,   53, 27.5,           },
    {"Kevin Durant",          "Thunders",      2007, 2016,  9,  641, 28.2,           },
    {"Kevin Durant",          "Warriors",      2016, 2019,  3,  208, 26.0, "trade", 2},
    {"James Harden",          "Thunders",      2009, 2012,  3,  220, 12.2, "zzzzz"   },
    {"James Harden",          "Rockets",       2012, 2020,  8,  606, 30.4            },
    {"Chris Paul",            "Hornets",       2005, 2011,  6,  425, 22.8, "zzzzz"   },
    {"Chris Paul",            "Clippers",      2011, 2017,  6,  409, 19.1            },
    {"Chris Paul",            "Rockets",       2017, 2019,  2,  116, 15.6            },
    {"Chris Paul",            "Thunders",      2019, 2020,  1,   63, 17.7            },
    {"DeAndre Jordan",        "Clippers",      2008, 2018, 10,  750,  8.8            },
    {"DeAndre Jordan",        "Mavericks",     2018, 2019,  1,   50, 11.0            },
    {"DeAndre Jordan",        "Knicks",        2019, 2019,  1,   19, 10.9            },
    {"DeAndre Jordan",        "Nets",          2019, 2020,  1,   56,  8.3            },
    {"Ricky Rubio",           "Timberwolves",  2011, 2017,  6,  353, 11.1            },
    {"Ricky Rubio",           "Jazz",          2017, 2019,  2,  145, 12.7            },
    {"Ricky Rubio",           "Suns",          2019, 2020,  1,   57, 13.1            },
    {"Rajon Rondo",           "Celtics",       2006, 2014,  9,  527, 11.9, "trade", 1},
    {"Rajon Rondo",           "Mavericks",     2014, 2015,  1,   46,  9.3            },
    {"Rajon Rondo",           "Kings",         2015, 2016,  1,   72, 11.9            },
    {"Rajon Rondo",           "Bulls",         2016, 2017,  1,   69,  7.8            },
    {"Rajon Rondo",           "Pelicans",      2017, 2018,  1,   65,  8.3            },
    {"Rajon Rondo",           "Lakers",        2018, 2020,  2,   94,  7.1            },
    {"Manu Ginobili",         "Spurs",         2002, 2018, 16, 1057, 13.3, "zzzzz", 4},
    {"Kyrie Irving",          "Cavaliers",     2011, 2017,  6,  381, 20.8, "zzzzz", 1},
    {"Kyrie Irving",          "Celtics",       2017, 2019,  2,  127, 24.4            },
    {"Kyrie Irving",          "Nets",          2019, 2020,  1,   20, 27.4            },
    {"Vince Carter",          "Raptors",       1998, 2004,  6,  403, 18.3            },
    {"Vince Carter",          "Nets",          2004, 2009,  5,  374, 20.8            },
    {"Vince Carter",          "Magic",         2009, 2011,  2,   97, 15.1            },
    {"Vince Carter",          "Suns",          2011, 2011,  1,   51, 13.5            },
    {"Vince Carter",          "Mavericks",     2011, 2014,  3,  223, 10.1            },
    {"Vince Carter",          "Grizzlies",     2014, 2017,  3,  199,  8.0            },
    {"Vince Carter",          "Kings",         2017, 2018,  1,   58,  5.4            },
    {"Vince Carter",          "Hawks",         2018, 2020,  2,  136,  5.0            },
    {"Carmelo Anthony",       "Nuggets",       2003, 2011,  8,  564, 25.2, "zzzzz"   },
    {"Carmelo Anthony",       "Knicks",        2011, 2017,  6,  412, 22.4            },
    {"Carmelo Anthony",       "Thunders",      2017, 2018,  1,   78, 16.2            },
    {"Carmelo Anthony",       "Rockets",       2018, 2019,  1,   10, 13.4            },
    {"Carmelo Anthony",       "Trail Blazers", 2019, 2020,  1,   50, 15.3            },
    {"Dwyane Wade",           "Heat",          2003, 2016, 13,  855, 26.6, "zzzzz", 3},
    {"Dwyane Wade",           "Bulls",         2016, 2017,  1,   60, 18.3            },
    {"Dwyane Wade",           "Cavaliers",     2017, 2017,  1,   46, 11.2            },
    {"Dwyane Wade",           "Heat",          2017, 2019,  2,   93, 15.0            },
    {"Joel Embiid",           "76ers",         2016, 2020,  4,  202, 24.1            },
    {"Paul George",           "Pacers",        2010, 2017,  7,  448, 17.4            },
    {"Paul George",           "Thunders",      2017, 2019,  2,  156, 28.0            },
    {"Paul George",           "Clippers",      2019, 2020,  1,   42, 21.0            },
    {"Giannis Antetokounmpo", "Bucks",         2013, 2020,  7,  522, 20.0            },
    {"Yao Ming",              "Rockets",       2002, 2011,  8,  486, 19.0            },
    {"Blake Griffin",         "Clippers",      2010, 2018,  8,  504, 22.6            },
    {"Blake Griffin",         "Pistons",       2018, 2020,  2,  118, 21.4            },
    {"Damian Lillard",        "Trail Blazers", 2012, 2020,  8,  607, 24.0            },
    {"Steve Nash",            "Suns",          1996, 1998,  2,  141,  9.1            },
    {"Steve Nash",            "Mavericks",     1998, 2004,  6,  408, 14.5            },
    {"Steve Nash",            "Suns",          2004, 2012,  8,  603, 16.5            },
    {"Steve Nash",            "Lakers",        2012, 2014,  2,   65,  6.8            },
    {"Dirk Nowitzki",         "Mavericks",     1998, 2019, 21, 1522, 20.7, "trade", 1},
    {"Pau Gasol",             "Grizzlies",     2001, 2008,  7,  476, 18.9            },
    {"Pau Gasol",             "Lakers",        2008, 2014,  6,  429, 17.4, "trade", 2},
    {"Pau Gasol",             "Bulls",         2014, 2016,  2,  150, 16.5            },
    {"Pau Gasol",             "Spurs",         2016, 2019,  3,  168, 10.1            },
    {"Pau Gasol",             "Bucks",         2019, 2019,  1,    3,  1.3            },
    {"Marc Gasol",            "Grizzlies",     2008, 2019, 11,  769, 15.7            },
    {"Marc Gasol",            "Raptors",       2018, 2020,  2,   62, 13.6, "trade", 1},
    {"Grant Hill",            "Pistons",       1994, 2000,  6,  435, 21.1            },
    {"Grant Hill",            "Magic",         2000, 2007,  6,  200, 14.4            },
    {"Grant Hill",            "Suns",          2007, 2012,  5,  362, 13.2            },
    {"Grant Hill",            "Clippers",      2012, 2013,  1,   29,  3.2            },
    {"Ray Allen",             "Bucks",         1996, 2003,  7,  494, 21.8            },
    {"Ray Allen",             "Thunders",      2003, 2007,  4,  296, 23.9            },
    {"Ray Allen",             "Celtics",       2007, 2012,  5,  358, 16.5, "trade", 1},
    {"Ray Allen",             "Heat",          2012, 2014,  2,  152, 10.9, "trade", 1},
    {"Klay Thompson",         "Warriors",      2011, 2019,  8,  615, 19.5, "zzzzz", 3},
    {"Kristaps Porzingis",    "Knicks",        2015, 2018,  3,  186, 18.1            },
    {"Kristaps Porzingis",    "Mavericks",     2019, 2020,  1,   51, 19.2            },
    {"Shaquille O'Neal",      "Magic",         1992, 1996,  4,  295, 26.6            },
    {"Shaquille O'Neal",      "Lakers",        1996, 2004,  8,  514, 27.2, "trade", 3},
    {"Shaquille O'Neal",      "Heat",          2004, 2008,  4,  205, 17.3, "trade", 1},
    {"Shaquille O'Neal",      "Suns",          2008, 2009,  1,  103, 17.8            },
    {"Shaquille O'Neal",      "Cavaliers",     2009, 2010,  1,   53, 12.0            },
    {"Shaquille O'Neal",      "Celtics",       2010, 2011,  1,   37,  9.2            },
    {"JaVale McGee",          "Wizards",       2008, 2012,  4,  255, 10.1            },
    {"JaVale McGee",          "Nuggets",       2012, 2015,  3,  121,  7.0            },
    {"JaVale McGee",          "76ers",         2015, 2015,  1,    6,  3.0            },
    {"JaVale McGee",          "Mavericks",     2015, 2016,  1,   34,  5.1            },
    {"JaVale McGee",          "Warriors",      2016, 2018,  2,  142,  6.1, "trade", 2},
    {"JaVale McGee",          "Lakers",        2018, 2020,  2,  136,  6.8            },
    {"Dwight Howard",         "Magic",         2004, 2012,  8,  621, 20.6            },
    {"Dwight Howard",         "Lakers",        2012, 2013,  1,   76, 17.1            },
    {"Dwight Howard",         "Rockets",       2013, 2016,  3,  183, 15.8            },
    {"Dwight Howard",         "Hawks",         2016, 2017,  1,   74, 13.5            },
    {"Dwight Howard",         "Hornets",       2017, 2018,  1,   81, 16.6            },
    {"Dwight Howard",         "Wizards",       2018, 2019,  1,    9, 12.8            },
    {"Dwight Howard",         "Lakers",        2019, 2020,  1,   62,  7.5            },
    {"Amare Stoudemire",      "Suns",          2002, 2010,  8,  516, 23.1            },
    {"Amare Stoudemire",      "Knicks",        2010, 2015,  5,  255, 14.2            },
    {"Amare Stoudemire",      "Mavericks",     2015, 2015,  1,   23, 10.8            },
    {"Amare Stoudemire",      "Heat",          2015, 2016,  1,   52,  5.8            },
    {"Jason Kidd",            "Mavericks",     1994, 1997,  3,  182, 11.7, "zzzzz"   },
    {"Jason Kidd",            "Suns",          1997, 2001,  4,  313, 16.9            },
    {"Jason Kidd",            "Nets",          2001, 2008,  7,  506, 13.3            },
    {"Jason Kidd",            "Mavericks",     2008, 2012,  4,  318, 10.3, "trade", 1},
    {"Jason Kidd",            "Knicks",        2012, 2013,  1,   76,  6.0            },
    {"Ben Simmons",           "76ers",         2017, 2020,  3,  214, 16.4            },
    {"Luka Doncic",           "Mavericks",     2018, 2020,  2,  126, 24.4            },
};


std::vector<Teammate> MockData::teammates_ = {
    {"Tim Duncan",     "Tony Parker",      "Spurs",    2001, 2016},
    {"Tim Duncan",     "Manu Ginobili",    "Spurs",    2002, 2016},
    {"Tony Parker",    "Manu Ginobili",    "Spurs",    2002, 2018},
    {"LeBron James",   "Dwyane Wade",      "Heat",     2010, 2014},
    {"Yao Ming",       "Tracy McGrady",    "Rockets",  2004, 2010},
    {"Kobe Bryant",    "Shaquille O'Neal", "Lakers",   1996, 2004},
    {"Dwyane Wade",    "Shaquille O'Neal", "Heat",     2004, 2008},
    {"Stephen Curry",  "Kevin Durant",     "Warriors", 2016, 2019},
    {"Stephen Curry",  "Klay Thompson",    "Warriors", 2011, 2019},
};

std::unordered_map<std::string, std::vector<Serve>> MockData::playerServes_ = playerServes();
std::unordered_map<std::string, std::vector<Serve>> MockData::teamServes_ = teamServes();

// Mock schema
std::shared_ptr<meta::NebulaSchemaProvider> MockData::mockPlayerTagSchema(ObjectPool* pool,
                                                                          SchemaVer ver,
                                                                          bool hasProp) {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(ver));
    if (!hasProp) {
        return schema;
    }
    schema->addField("name",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     false,
                     ConstantExpression::make(pool, ""));
    // only age filed has no default value and nullable is false
    schema->addField("age",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false);
    schema->addField("playing",
                     meta::cpp2::PropertyType::BOOL,
                     0,
                     false,
                     ConstantExpression::make(pool, true));
    schema->addField("career",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 10L));
    schema->addField("startYear",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 0L));
    schema->addField("endYear",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 0L));
    schema->addField("games",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 0L));
    schema->addField("avgScore",
                     meta::cpp2::PropertyType::DOUBLE,
                     0,
                     false,
                     ConstantExpression::make(pool, 0.0));
    schema->addField("serveTeams",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 0L));

    // Set ttl property
    if (FLAGS_mock_ttl_col) {
        schema->addField("insertTime",
                         meta::cpp2::PropertyType::INT64,
                         0,
                         false,
                         ConstantExpression::make(pool, 0L));
        meta::cpp2::SchemaProp prop;
        prop.set_ttl_col("insertTime");
        prop.set_ttl_duration(FLAGS_mock_ttl_duration);
        schema->setProp(prop);
    }

    // Use default value
    schema->addField("country",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     false,
                     ConstantExpression::make(pool, "America"));
    // Use nullable
    schema->addField("champions",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     true);

    return schema;
}

std::shared_ptr<meta::NebulaSchemaProvider>
MockData::mockTeamTagSchema(SchemaVer ver, bool hasProp) {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(ver));
    if (!hasProp) {
        return schema;
    }
    schema->addField("name", meta::cpp2::PropertyType::STRING);
    return schema;
}

std::shared_ptr<meta::NebulaSchemaProvider>
MockData::mockServeEdgeSchema(ObjectPool* pool, SchemaVer ver, bool hasProp) {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(ver));
    if (!hasProp) {
        return schema;
    }
    schema->addField("playerName",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     false,
                     ConstantExpression::make(pool, ""));
    schema->addField("teamName",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     false,
                     ConstantExpression::make(pool, ""));
    schema->addField("startYear",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 2020L));
    schema->addField("endYear",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 2020L));
    // only teamCareer filed has no default value and nullable is false
    schema->addField("teamCareer",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false);
    schema->addField("teamGames",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 1L));
    schema->addField("teamAvgScore",
                     meta::cpp2::PropertyType::DOUBLE,
                     0,
                     false,
                     ConstantExpression::make(pool, 0.0));

    // Set ttl property
    if (FLAGS_mock_ttl_col) {
        schema->addField("insertTime",
                         meta::cpp2::PropertyType::INT64,
                         0,
                         false,
                         ConstantExpression::make(pool, 0L));
        meta::cpp2::SchemaProp prop;
        prop.set_ttl_col("insertTime");
        prop.set_ttl_duration(FLAGS_mock_ttl_duration);
        schema->setProp(prop);
    }

    // Use default value
    schema->addField("type",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     false,
                     ConstantExpression::make(pool, "trade"));
    // Use nullable
    schema->addField("champions",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     true);

    return schema;
}

std::shared_ptr<meta::NebulaSchemaProvider>
MockData::mockTeammateEdgeSchema(SchemaVer ver, bool hasProp) {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(ver));
    if (!hasProp) {
        return schema;
    }
    schema->addField("player1",   meta::cpp2::PropertyType::STRING);
    schema->addField("player2",   meta::cpp2::PropertyType::STRING);
    schema->addField("teamName",  meta::cpp2::PropertyType::STRING);
    schema->addField("startYear", meta::cpp2::PropertyType::INT64);
    schema->addField("endYear",   meta::cpp2::PropertyType::INT64);
    return schema;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockGeneralTagIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "col_bool";
    col.type.set_type(meta::cpp2::PropertyType::BOOL);
    cols.emplace_back(std::move(col));

    col.name = "col_int";
    col.type.set_type(meta::cpp2::PropertyType::INT64);
    cols.emplace_back(std::move(col));

    col.name = "col_float";
    col.type.set_type(meta::cpp2::PropertyType::FLOAT);
    cols.emplace_back(std::move(col));

    col.name = "col_double";
    col.type.set_type(meta::cpp2::PropertyType::DOUBLE);
    cols.emplace_back(std::move(col));

    col.name = "col_str";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));
    return cols;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockPlayerTagIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "name";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));

    col.name = "age";
    col.type.set_type(meta::cpp2::PropertyType::INT64);
    cols.emplace_back(std::move(col));

    col.name = "playing";
    col.type.set_type(meta::cpp2::PropertyType::BOOL);
    cols.emplace_back(std::move(col));
    return cols;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockTeamTagIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "name";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));
    return cols;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockSimpleTagIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "col_date";
    col.type.set_type(meta::cpp2::PropertyType::DATE);
    cols.emplace_back(std::move(col));
    return cols;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockServeEdgeIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "playerName";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));

    col.name = "teamName";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));

    col.name = "startYear";
    col.type.set_type(meta::cpp2::PropertyType::INT64);
    cols.emplace_back(std::move(col));
    return cols;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockTeammateEdgeIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col;
    col.name = "player1";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));

    col.name = "player2";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));

    col.name = "teamName";
    col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col.type.set_type_length(20);
    cols.emplace_back(std::move(col));
    return cols;
}
std::shared_ptr<meta::NebulaSchemaProvider> MockData::mockGeneralTagSchemaV1() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    schema->addField("col_bool",   meta::cpp2::PropertyType::BOOL);
    schema->addField("col_int",    meta::cpp2::PropertyType::INT64);
    schema->addField("col_float",  meta::cpp2::PropertyType::FLOAT);
    schema->addField("col_double", meta::cpp2::PropertyType::DOUBLE);
    schema->addField("col_str",    meta::cpp2::PropertyType::STRING);
    return schema;
 }

std::shared_ptr<meta::NebulaSchemaProvider> MockData::mockGeneralTagSchemaV2() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(1));
    schema->addField("col_bool", meta::cpp2::PropertyType::BOOL);
    schema->addField("col_int", meta::cpp2::PropertyType::INT64);
    schema->addField("col_float", meta::cpp2::PropertyType::FLOAT);
    schema->addField("col_double", meta::cpp2::PropertyType::DOUBLE);
    schema->addField("col_str", meta::cpp2::PropertyType::STRING);
    schema->addField("col_int8", meta::cpp2::PropertyType::INT8);
    schema->addField("col_int16", meta::cpp2::PropertyType::INT16);
    schema->addField("col_int32", meta::cpp2::PropertyType::INT32);
    schema->addField("col_timestamp", meta::cpp2::PropertyType::TIMESTAMP);
    schema->addField("col_date", meta::cpp2::PropertyType::DATE);
    schema->addField("col_datetime", meta::cpp2::PropertyType::DATETIME);
    return schema;
}

std::shared_ptr<meta::NebulaSchemaProvider> MockData::mockTypicaSchemaV2(ObjectPool* pool) {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    schema->addField("col_bool", meta::cpp2::PropertyType::BOOL);
    schema->addField("col_bool_null",
                     meta::cpp2::PropertyType::BOOL,
                     0,
                     true);
    schema->addField("col_bool_default",
                     meta::cpp2::PropertyType::BOOL,
                     0,
                     false,
                     ConstantExpression::make(pool, true));
    schema->addField("col_int", meta::cpp2::PropertyType::INT64);
    schema->addField("col_int_null",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     true);
    schema->addField("col_int_default",
                     meta::cpp2::PropertyType::INT64,
                     0,
                     false,
                     ConstantExpression::make(pool, 20L));
    schema->addField("col_float", meta::cpp2::PropertyType::FLOAT);
    schema->addField("col_float_null",
                     meta::cpp2::PropertyType::FLOAT,
                     0,
                     true);
    schema->addField("col_float_default",
                     meta::cpp2::PropertyType::FLOAT,
                     0,
                     false,
                     ConstantExpression::make(pool, 2.2F));
    schema->addField("col_str", meta::cpp2::PropertyType::STRING);
    schema->addField("col_str_null",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     true);
    schema->addField("col_str_default",
                     meta::cpp2::PropertyType::STRING,
                     0,
                     false,
                     ConstantExpression::make(pool, "sky"));
    schema->addField("col_date", meta::cpp2::PropertyType::DATE);
    schema->addField("col_date_null",
                     meta::cpp2::PropertyType::DATE,
                     0,
                     true);

    const Date date = {2020, 2, 20};
    schema->addField("col_date_default",
                     meta::cpp2::PropertyType::DATE,
                     0,
                     false,
                     ConstantExpression::make(pool, date));

    return schema;
}

std::vector<nebula::meta::cpp2::ColumnDef>
MockData::mockTypicaIndexColumns() {
    std::vector<nebula::meta::cpp2::ColumnDef> cols;
    meta::cpp2::ColumnDef col_bool;
    col_bool.name = "col_bool";
    col_bool.type.set_type(meta::cpp2::PropertyType::BOOL);
    cols.emplace_back(std::move(col_bool));

    meta::cpp2::ColumnDef col_bool_null;
    col_bool_null.name = "col_bool_null";
    col_bool_null.type.set_type(meta::cpp2::PropertyType::BOOL);
    col_bool_null.set_nullable(true);
    cols.emplace_back(std::move(col_bool_null));

    meta::cpp2::ColumnDef col_bool_default;
    col_bool_default.name = "col_bool_default";
    col_bool_default.type.set_type(meta::cpp2::PropertyType::BOOL);
    cols.emplace_back(std::move(col_bool_default));

    meta::cpp2::ColumnDef col_int;
    col_int.name = "col_int";
    col_int.type.set_type(meta::cpp2::PropertyType::INT64);
    cols.emplace_back(std::move(col_int));

    meta::cpp2::ColumnDef col_int_null;
    col_int_null.name = "col_int_null";
    col_int_null.type.set_type(meta::cpp2::PropertyType::INT64);
    col_int_null.set_nullable(true);
    cols.emplace_back(std::move(col_int_null));

    meta::cpp2::ColumnDef col_float;
    col_float.name = "col_float";
    col_float.type.set_type(meta::cpp2::PropertyType::FLOAT);
    cols.emplace_back(std::move(col_float));

    meta::cpp2::ColumnDef col_float_null;
    col_float_null.name = "col_float_null";
    col_float_null.type.set_type(meta::cpp2::PropertyType::FLOAT);
    col_float_null.set_nullable(true);
    cols.emplace_back(std::move(col_float_null));

    meta::cpp2::ColumnDef col_str;
    col_str.name = "col_str";
    col_str.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col_str.type.set_type_length(20);
    cols.emplace_back(std::move(col_str));

    meta::cpp2::ColumnDef col_str_null;
    col_str_null.name = "col_str_null";
    col_str_null.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
    col_str_null.type.set_type_length(20);
    col_str_null.set_nullable(true);
    cols.emplace_back(std::move(col_str_null));

    meta::cpp2::ColumnDef col_date;
    col_date.name = "col_date";
    col_date.type.set_type(meta::cpp2::PropertyType::DATE);
    cols.emplace_back(std::move(col_date));

    meta::cpp2::ColumnDef col_date_null;
    col_date_null.name = "col_date_null";
    col_date_null.type.set_type(meta::cpp2::PropertyType::DATE);
    col_date_null.set_nullable(true);
    cols.emplace_back(std::move(col_date_null));
    return cols;
}

// Mock data
std::vector<VertexData> MockData::mockVertices(bool upper) {
    std::vector<VertexData> ret;
    // Multiple vertices, each vertex has two tags, player and team
    // player tagId is 1, team tagId is 2
    for (auto& player : players_) {
        VertexData data;
        data.tId_ = 1;

        std::vector<Value> props;
        if (upper) {
            std::string upperName;
            upperName.resize(player.name_.size());
            auto toupper = [] (auto c) { return ::toupper(c); };
            std::transform(player.name_.begin(), player.name_.end(), upperName.begin(), toupper);
            data.vId_  = upperName;
            props.emplace_back(upperName);
        } else {
            data.vId_ = player.name_;
            props.emplace_back(player.name_);
        }

        props.emplace_back(player.age_);
        props.emplace_back(player.playing_);
        props.emplace_back(player.career_);
        props.emplace_back(player.startYear_);
        props.emplace_back(player.endYear_);
        props.emplace_back(player.games_);
        props.emplace_back(player.avgScore_);
        props.emplace_back(player.serveTeams_);

        // Use insert time as ttl col
        if (FLAGS_mock_ttl_col) {
            props.emplace_back(time::WallClock::fastNowInSec());
        }
        // Use default value and nullable
        if (player.country_.compare("") != 0) {
            props.emplace_back(player.country_);
            if (player.champions_) {
                props.emplace_back(player.champions_);
            }
        }
        data.props_ = std::move(props);
        ret.emplace_back(std::move(data));
    }

    for (auto& team : teams_) {
        VertexData data;
        data.vId_ = team;
        data.tId_ = 2;
        std::vector<Value> props;
        props.emplace_back(team);
        data.props_ = std::move(props);
        ret.emplace_back(std::move(data));
    }
    return ret;
}

std::vector<std::pair<PartitionID, std::string>> MockData::mockPlayerIndexKeys(bool upper) {
    std::vector<std::pair<PartitionID, std::string>> keys;
    for (auto& player : players_) {
        std::string name;
        if (upper) {
            std::string upperName;
            upperName.resize(player.name_.size());
            auto toupper = [] (auto c) { return ::toupper(c); };
            std::transform(player.name_.begin(), player.name_.end(), upperName.begin(), toupper);
            name = upperName;
        } else {
            name = player.name_;
        }
        auto part = std::hash<std::string>()(name) % 6 + 1;
        std::string values;
        values.append(encodeFixedStr(name, 20));
        values.append(IndexKeyUtils::encodeValue(player.age_));
        values.append(IndexKeyUtils::encodeValue(player.playing_));
        auto key = IndexKeyUtils::vertexIndexKey(32, part, 1, name, std::move(values));
        auto pair = std::make_pair(part, std::move(key));
        keys.emplace_back(std::move(pair));
    }
    return keys;
}

std::unordered_map<PartitionID, std::vector<VertexData>>
MockData::mockVerticesofPart(int32_t parts) {
    std::unordered_map<PartitionID, std::vector<VertexData>> ret;
    auto verticesVec = mockVertices();
    for (auto& vertexData : verticesVec) {
        auto partId = std::hash<std::string>()(vertexData.vId_) % parts + 1;
        ret[partId].emplace_back(std::move(vertexData));
    }
    return ret;
}

std::vector<VertexID> MockData::mockVerticeIds() {
    std::vector<VertexID> ret;
    // Multiple vertices, each vertex has two tags, player and team
    // player tagId is 1, team tagId is 2
    for (auto& player : players_) {
        VertexID data;
        data = player.name_;
        ret.push_back(data);
    }

    for (auto& team : teams_) {
        VertexID data;
        data = team;
        ret.push_back(data);
    }
    return ret;
}

std::vector<VertexID> MockData::mockPlayerVerticeIds() {
    std::vector<VertexID> ret;
    for (auto& player : players_) {
        VertexID data;
        data = player.name_;
        ret.push_back(data);
    }
    return ret;
}

std::vector<EdgeData> MockData::mockEdges(bool upper) {
    std::vector<EdgeData> ret;
    // Use serve data, positive edgeType is 101, reverse edgeType is -101
    for (auto& serve : serves_) {
        EdgeData positiveEdge;
        positiveEdge.type_ = 101;
        positiveEdge.rank_ = serve.startYear_;
        positiveEdge.dstId_ = serve.teamName_;

        std::vector<Value> props;
        if (upper) {
            std::string upperName;
            upperName.resize(serve.playerName_.size());
            auto toupper = [] (auto c) { return ::toupper(c); };
            std::transform(serve.playerName_.begin(), serve.playerName_.end(),
                           upperName.begin(), toupper);
            positiveEdge.srcId_ = upperName;
            props.emplace_back(upperName);
        } else {
            positiveEdge.srcId_ = serve.playerName_;
            props.emplace_back(serve.playerName_);
        }

        props.emplace_back(serve.teamName_);
        props.emplace_back(serve.startYear_);
        props.emplace_back(serve.endYear_);
        props.emplace_back(serve.teamCareer_);
        props.emplace_back(serve.teamGames_);
        props.emplace_back(serve.teamAvgScore_);

        // Use insert time as ttl col
        if (FLAGS_mock_ttl_col) {
            props.emplace_back(time::WallClock::fastNowInSec());
        }
        // Use default value and nullable
        if (!serve.type_.empty()) {
            props.emplace_back(serve.type_);
            if (serve.champions_) {
                props.emplace_back(serve.champions_);
            }
        }
        positiveEdge.props_ = std::move(props);
        auto reverseData = getReverseEdge(positiveEdge);
        ret.emplace_back(std::move(positiveEdge));
        ret.emplace_back(std::move(reverseData));
    }
    return ret;
}

std::vector<std::pair<PartitionID, std::string>> MockData::mockServeIndexKeys() {
    std::vector<std::pair<PartitionID, std::string>> keys;
    for (auto& serve : serves_) {
        auto part = std::hash<std::string>()(serve.playerName_) % 6 + 1;
        std::string values;
        values.append(encodeFixedStr(serve.playerName_, 20));
        values.append(encodeFixedStr(serve.teamName_, 20));
        values.append(IndexKeyUtils::encodeValue(serve.startYear_));
        auto key = IndexKeyUtils::edgeIndexKey(32, part, 101,
                                               serve.playerName_,
                                               serve.startYear_,
                                               serve.teamName_,
                                               std::move(values));
        auto pair = std::make_pair(part, std::move(key));
        keys.emplace_back(std::move(pair));
    }
    return keys;
}

std::vector<EdgeData> MockData::mockMultiEdges() {
    auto ret = mockEdges();
    for (const auto& teammate : teammates_) {
        EdgeData data;
        data.srcId_ = teammate.player1_;
        data.type_ = 102,
        data.rank_ = teammate.startYear_;
        data.dstId_ = teammate.player2_;

        std::vector<Value> props;
        props.emplace_back(teammate.player1_);
        props.emplace_back(teammate.player2_);
        props.emplace_back(teammate.teamName_);
        props.emplace_back(teammate.startYear_);
        props.emplace_back(teammate.endYear_);
        data.props_ = std::move(props);

        EdgeData antiData = data;
        data.srcId_ = teammate.player2_;
        data.dstId_ = teammate.player1_;

        auto reverseData = getReverseEdge(data);
        ret.emplace_back(std::move(data));
        ret.emplace_back(std::move(reverseData));

        auto reverseAntiData = getReverseEdge(antiData);
        ret.emplace_back(std::move(antiData));
        ret.emplace_back(std::move(reverseAntiData));
    }
    return ret;
}

std::unordered_map<PartitionID, std::vector<EdgeData>>
MockData::mockEdgesofPart(int32_t parts) {
    std::unordered_map<PartitionID, std::vector<EdgeData>> ret;
    auto edgesVec = mockEdges();
    for (auto& EdgeData : edgesVec) {
        auto partId = std::hash<std::string>()(EdgeData.srcId_) % parts + 1;
        ret[partId].emplace_back(std::move(EdgeData));
    }
    return ret;
}

std::vector<EdgeData> MockData::mockEdgeKeys() {
    std::vector<EdgeData> ret;
    // Use serve data, positive edgeType is 101, reverse edgeType is -101
    for (auto& serve : serves_) {
        EdgeData positiveEdge;
        positiveEdge.srcId_ = serve.playerName_;
        positiveEdge.type_ = 101;
        positiveEdge.rank_ = serve.startYear_;
        positiveEdge.dstId_ = serve.teamName_;

        // Reverse edge
        auto reverseEdge = getReverseEdge(positiveEdge);
        ret.push_back(positiveEdge);
        ret.push_back(reverseEdge);
    }
    return ret;
}

std::unordered_map<VertexID, std::vector<EdgeData>> MockData::mockmMultiRankServes(
        EdgeRanking rankCount) {
    std::unordered_map<VertexID, std::vector<EdgeData>> ret;
    // add multiple serve edge for benchmark
    // Use serve data, EdgeType is 101, also use the rank as startYear and endYear
    for (EdgeRanking rank = 0; rank < rankCount; rank++) {
        for (const auto& serve : serves_) {
            EdgeData data;
            data.srcId_ = serve.playerName_;
            data.type_ = 101;
            data.rank_ = rank;
            data.dstId_ = serve.teamName_;

            std::vector<Value> props;
            props.emplace_back(serve.playerName_);
            props.emplace_back(serve.teamName_);
            props.emplace_back(rank);
            props.emplace_back(rank);
            props.emplace_back(serve.teamCareer_);
            props.emplace_back(serve.teamGames_);
            props.emplace_back(serve.teamAvgScore_);

            // Use insert time as ttl col
            if (FLAGS_mock_ttl_col) {
                props.emplace_back(time::WallClock::fastNowInSec());
            }
            // Use default value and nullable
            if (!serve.type_.empty()) {
                props.emplace_back(serve.type_);
                if (serve.champions_) {
                    props.emplace_back(serve.champions_);
                }
            }
            data.props_ = std::move(props);
            ret[data.srcId_].emplace_back(std::move(data));
        }
    }
    return ret;
}

nebula::storage::cpp2::AddVerticesRequest MockData::mockAddVerticesReq(bool upper, int32_t parts) {
    nebula::storage::cpp2::AddVerticesRequest req;
    req.set_space_id(1);
    req.set_if_not_exists(true);

    auto retRecs = mockVertices(upper);

    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewVertex newVertex;
        nebula::storage::cpp2::NewTag newTag;
        auto partId = std::hash<std::string>()(rec.vId_) % parts + 1;

        newTag.set_tag_id(rec.tId_);
        newTag.set_props(std::move(rec.props_));

        std::vector<nebula::storage::cpp2::NewTag> newTags;
        newTags.push_back(std::move(newTag));

        newVertex.set_id(rec.vId_);
        newVertex.set_tags(std::move(newTags));
        (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    return req;
}

nebula::storage::cpp2::DeleteVerticesRequest MockData::mockDeleteVerticesReq(int32_t parts) {
    nebula::storage::cpp2::DeleteVerticesRequest req;
    req.set_space_id(1);

    auto retRecs = mockVerticeIds();
    for (auto& rec : retRecs) {
        auto partId = std::hash<std::string>()(rec) % parts + 1;
        (*req.parts_ref())[partId].emplace_back(std::move(rec));
    }
    return req;
}

nebula::storage::cpp2::AddEdgesRequest MockData::mockAddEdgesReq(bool upper, int32_t parts) {
    nebula::storage::cpp2::AddEdgesRequest req;
    req.set_space_id(1);
    req.set_if_not_exists(true);
    auto retRecs = mockEdges(upper);
    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewEdge newEdge;
        nebula::storage::cpp2::EdgeKey edgeKey;
        auto partId = std::hash<std::string>()(rec.srcId_) % parts + 1;

        edgeKey.set_src(rec.srcId_);
        edgeKey.set_edge_type(rec.type_);
        edgeKey.set_ranking(rec.rank_);
        edgeKey.set_dst(rec.dstId_);

        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(rec.props_));

        (*req.parts_ref())[partId].emplace_back(std::move(newEdge));
    }
    return req;
}

nebula::storage::cpp2::DeleteEdgesRequest MockData::mockDeleteEdgesReq(int32_t parts) {
    nebula::storage::cpp2::DeleteEdgesRequest req;
    req.set_space_id(1);

    auto retRecs = mockEdgeKeys();
    for (auto& rec : retRecs) {
        auto partId = std::hash<std::string>()(rec.srcId_) % parts + 1;

        nebula::storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(rec.srcId_);
        edgeKey.set_edge_type(rec.type_);
        edgeKey.set_ranking(rec.rank_);
        edgeKey.set_dst(rec.dstId_);
        (*req.parts_ref())[partId].emplace_back(std::move(edgeKey));
    }
    return req;
}

std::vector<VertexData> MockData::mockVerticesSpecifiedOrder() {
    std::vector<VertexData> ret;
    // Multiple vertices, vertex has two tags, players and teams
    // players tagId is 1, teams tagId is 2
    for (auto& player : players_) {
        VertexData data;
        data.vId_ = player.name_;
        data.tId_ = 1;

        std::vector<Value>  props;
        props.emplace_back(player.serveTeams_);
        props.emplace_back(player.avgScore_);
        props.emplace_back(player.games_);
        props.emplace_back(player.endYear_);
        props.emplace_back(player.startYear_);
        props.emplace_back(player.career_);
        props.emplace_back(player.playing_);
        props.emplace_back(player.age_);
        props.emplace_back(player.name_);

        // player.country_ use default value,
        // player.champions_ use null value

        data.props_ = std::move(props);
        ret.push_back(data);
    }
    for (auto& team : teams_) {
        VertexData data;
        data.vId_ = team;
        data.tId_ = 2;
        std::vector<Value>  props;
        props.emplace_back(team);
        data.props_ = std::move(props);
        ret.push_back(data);
    }
    return ret;
}

std::vector<EdgeData> MockData::mockEdgesSpecifiedOrder() {
    std::vector<EdgeData> ret;
    // Use serve data, positive edgeType is 101, reverse edgeType is -101
    for (auto& serve : serves_) {
        EdgeData positiveEdge;
        positiveEdge.srcId_ = serve.playerName_;
        positiveEdge.type_ = 101;
        positiveEdge.rank_ = serve.startYear_;
        positiveEdge.dstId_ = serve.teamName_;

        std::vector<Value>  props;
        props.emplace_back(serve.teamAvgScore_);
        props.emplace_back(serve.teamGames_);
        props.emplace_back(serve.teamCareer_);
        props.emplace_back(serve.endYear_);
        props.emplace_back(serve.startYear_);
        props.emplace_back(serve.teamName_);
        props.emplace_back(serve.playerName_);

        // serve.starting_ use default value
        // serve.champions_ use null value
        positiveEdge.props_ = std::move(props);

        // Reverse edge
        auto reverseEdge = getReverseEdge(positiveEdge);
        ret.push_back(positiveEdge);
        ret.push_back(reverseEdge);
    }
    return ret;
}

nebula::storage::cpp2::AddVerticesRequest
MockData::mockAddVerticesSpecifiedOrderReq(int32_t parts) {
    nebula::storage::cpp2::AddVerticesRequest req;
    req.set_space_id(1);
    req.set_if_not_exists(false);
    auto retRecs = mockVerticesSpecifiedOrder();

    for (auto& rec : retRecs) {
        auto partId = std::hash<std::string>()(rec.vId_) % parts + 1;

        if (rec.tId_ == 1) {
            std::vector<std::string> colNames{"serveTeams", "avgScore", "games", "endYear",
                                              "startYear",  "career", "playing", "age",
                                              "name"};
            (*req.prop_names_ref())[1] = std::move(colNames);
        } else {
            std::vector<std::string> colNames{"name"};
            (*req.prop_names_ref())[2] = std::move(colNames);
        }

        nebula::storage::cpp2::NewVertex newVertex;
        nebula::storage::cpp2::NewTag newTag;

        newTag.set_tag_id(rec.tId_);
        newTag.set_props(std::move(rec.props_));
        std::vector<nebula::storage::cpp2::NewTag> newTags;
        newTags.push_back(std::move(newTag));

        newVertex.set_id(rec.vId_);
        newVertex.set_tags(std::move(newTags));
        (*req.parts_ref())[partId].emplace_back(std::move(newVertex));
    }
    return req;
}

nebula::storage::cpp2::AddEdgesRequest
MockData::mockAddEdgesSpecifiedOrderReq(int32_t parts) {
    nebula::storage::cpp2::AddEdgesRequest req;
    // Use space id is 1 when mock
    req.set_space_id(1);
    req.set_if_not_exists(false);

    auto retRecs = mockEdgesSpecifiedOrder();

    for (auto& rec : retRecs) {
        auto partId = std::hash<std::string>()(rec.srcId_) % parts + 1;

        nebula::storage::cpp2::NewEdge newEdge;
        nebula::storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(rec.srcId_);
        edgeKey.set_edge_type(rec.type_);
        edgeKey.set_ranking(rec.rank_);
        edgeKey.set_dst(rec.dstId_);

        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(rec.props_));
        (*req.parts_ref())[partId].emplace_back(std::move(newEdge));

        std::vector<std::string> colNames{"teamAvgScore", "teamGames", "teamCareer",
                                          "endYear", "startYear", "teamName", "playerName"};
        req.set_prop_names(std::move(colNames));
    }
    return req;
}

EdgeData MockData::getReverseEdge(const EdgeData& edge) {
    EdgeData reverse;
    reverse.srcId_ = edge.dstId_;
    reverse.type_ = -(edge.type_);
    reverse.rank_ = edge.rank_;
    reverse.dstId_ = edge.srcId_;
    reverse.props_ = edge.props_;
    return reverse;
}

nebula::storage::cpp2::KVPutRequest
MockData::mockKVPut() {
    nebula::storage::cpp2::KVPutRequest req;
    req.set_space_id(1);

    const int32_t totalParts = 6;
    std::unordered_map<PartitionID, std::vector<nebula::KeyValue>> data;
    for (size_t part = 1; part <= totalParts; part++) {
        nebula::KeyValue pair;
        pair.key = folly::stringPrintf("key_%ld", part);
        pair.value = folly::stringPrintf("value_%ld", part);
        std::vector<nebula::KeyValue> pairs;
        pairs.emplace_back(std::move(pair));
        data.emplace(part, std::move(pairs));
    }
    req.set_parts(std::move(data));
    return req;
}

nebula::storage::cpp2::KVGetRequest
MockData::mockKVGet() {
    nebula::storage::cpp2::KVGetRequest req;
    req.set_space_id(1);

    const int32_t totalParts = 6;
    std::unordered_map<PartitionID, std::vector<std::string>> data;
    for (size_t part = 1; part <= totalParts; part++) {
        std::vector<std::string> keys;
        keys.emplace_back(folly::stringPrintf("key_%ld", part));
        data.insert(std::make_pair(part, std::move(keys)));
    }
    req.set_parts(std::move(data));
    return req;
}

nebula::storage::cpp2::KVRemoveRequest
MockData::mockKVRemove() {
    nebula::storage::cpp2::KVRemoveRequest req;
    req.set_space_id(1);

    const int32_t totalParts = 6;
    std::unordered_map<PartitionID, std::vector<std::string>> data;
    for (size_t part = 1; part <= totalParts; part++) {
        std::vector<std::string> keys;
        keys.emplace_back(folly::stringPrintf("key_%ld", part));
        data.insert(std::make_pair(part, std::move(keys)));
    }
    req.set_parts(std::move(data));
    return req;
}

std::string MockData::encodeFixedStr(const std::string& v, size_t len) {
    std::string fs = v;
    if (len > fs.size()) {
        fs.append(len - fs.size(), '\0');
    } else {
        fs = fs.substr(0, len);
    }
    return fs;
}

}  // namespace mock
}  // namespace nebula
