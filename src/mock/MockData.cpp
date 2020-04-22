/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/MockData.h"
#include "meta/NebulaSchemaProvider.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"

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
    "Bulls"
    "Hawks"
    "Knicks"
    "Pistons"
    "Bucks",
    "Magic",
    "Nets",
    "Wizards",
    "Heat"
};

// Player name, age, playing, NBA career(years), start year, end year, games,
// career average score, served teams, country(default value America), champions(nullable)
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

// Player name, team name, start year, end year,team career(years), games, average scores
// Starting game(default value false), champions in team(nullable)
std::vector<Serve> MockData::serve_ = {
    {"Tim Duncan",            "Spurs",         1997, 2016, 19, 1392, 19.0, true,  5},
    {"Tony Parker",           "Spurs",         2001, 2018, 17, 1198, 16.6, true,  4},
    {"Tony Parker",           "Hornets",       2018, 2019,  1,   56,  9.5          },
    {"LaMarcus Aldridge",     "Trail Blazers", 2006, 2015,  9,  648, 21.7, true    },
    {"LaMarcus Aldridge",     "Spurs",         2015, 2020,  5,  355, 18.9, true    },
    {"Rudy Gay",              "Grizzlies",     2006, 2013,  7,  479, 19.0, true    },
    {"Rudy Gay",              "Raptors",       2013, 2013,  1,   51, 19.5, true    },
    {"Rudy Gay",              "Kings",         2013, 2017,  4,  223, 21.1          },
    {"Rudy Gay",              "Spurs",         2017, 2020,  3,  186, 13.7          },
    {"Marco Belinelli",       "Warriors",      2007, 2009,  2,   75,  8.9          },
    {"Marco Belinelli",       "Raptors",       2009, 2010,  1,   66,  7.1          },
    {"Marco Belinelli",       "Hornets",       2010, 2012,  2,  146, 11.8          },
    {"Marco Belinelli",       "Bulls",         2012, 2013,  1,   73,  9.6          },
    {"Marco Belinelli",       "Spurs",         2013, 2015,  2,  142, 11.4, false, 1},
    {"Marco Belinelli",       "Kings",         2015, 2016,  1,   68, 10.2          },
    {"Marco Belinelli",       "Hornets",       2016, 2017,  1,   74, 10.5          },
    {"Marco Belinelli",       "Hawks",         2017, 2018,  1,   52, 11.4          },
    {"Marco Belinelli",       "76ers",         2018, 2018,  1,   28, 13.6          },
    {"Marco Belinelli",       "Spurs",         2018, 2020,  2,  131,  5.8          },
    {"Danny Green",           "Cavaliers",     2009, 2010,  1,   20,  2.0          },
    {"Danny Green",           "Spurs",         2010, 2018,  8,  520,  9.1, false, 1},
    {"Danny Green",           "Raptors",       2018, 2019,  1,   80, 10.3, true,  1},
    {"Danny Green",           "Lakers",        2019, 2020,  1,   61,  8.2, true    },
    {"Kyle Anderson",         "Spurs",         2014, 2018,  4,  257,  4.5          },
    {"Kyle Anderson",         "Grizzlies",     2018, 2020,  2,  102,  5.7          },
    {"Aron Baynes",           "Spurs",         2012, 2015,  3,  139,  3.0, false, 1},
    {"Aron Baynes",           "Pistons",       2015, 2017,  2,  157,  6.3          },
    {"Aron Baynes",           "Celtics",       2017, 2019,  2,  132,  6.0          },
    {"Aron Baynes",           "Suns",          2019, 2020,  1,   42, 11.5          },
    {"Boris Diaw",            "Hawks",         2003, 2005,  2,  142,  4.8          },
    {"Boris Diaw",            "Suns",          2005, 2008,  4,  258,  8.8          },
    {"Boris Diaw",            "Hornets",       2008, 2012,  4,  260, 11.3, true    },
    {"Boris Diaw",            "Spurs",         2012, 2016,  4,  331,  8.7, false, 1},
    {"Boris Diaw",            "Jazz",          2016, 2017,  1,   73,  4.6          },
    {"Tiago Splitter",        "Spurs",         2010, 2015,  5,  311,  8.2, false, 1},
    {"Tiago Splitter",        "Hawks",         2015, 2016,  1,   36,  5.6          },
    {"Tiago Splitter",        "76ers",         2016, 2017,  1,    8,  4.9          },
    {"Cory Joseph",           "Spurs",         2011, 2015,  4,  204,  4.5, false, 1},
    {"Cory Joseph",           "Raptors",       2015, 2017,  2,  160,  9.3          },
    {"Cory Joseph",           "Pacers",        2017, 2019,  2,  164,  6.5          },
    {"Cory Joseph",           "Kings",         2019, 2020,  1,   64,  6.3          },
    {"David West",            "Hornets",       2003, 2011,  8,  530, 19.0, true    },
    {"David West",            "Pacers",        2011, 2015,  4,  285, 11.7, true    },
    {"David West",            "Spurs",         2015, 2016,  1,   78,  7.1          },
    {"David West",            "Warriors",      2016, 2018,  2,  141,  6.8, false, 2},
    {"Jonathon Simmons",      "Spurs",         2015, 2017,  2,  133,  6.2          },
    {"Jonathon Simmons",      "Magic",         2017, 2019,  2,  110, 13.9          },
    {"Jonathon Simmons",      "76ers",         2019, 2019,  1,   15,  5.5          },
    {"Dejounte Murray",       "Spurs",         2016, 2020,  3,  177,  7.9          },
    {"Tracy McGrady",         "Raptors",       1997, 2000,  3,  192, 15.4          },
    {"Tracy McGrady",         "Magic",         2000, 2004,  4,  295, 28.0, true    },
    {"Tracy McGrady",         "Rockets",       2004, 2010,  6,  303, 24.4, true    },
    {"Tracy McGrady",         "Knicks",        2010, 2010,  1,   24,  9.4          },
    {"Tracy McGrady",         "Pistons",       2010, 2011,  1,   72,  8.0          },
    {"Tracy McGrady",         "Hawks",         2011, 2012,  1,   52,  5.3          },
    {"Kobe Bryant",           "Lakers",        1996, 2016, 20, 1346, 25.0, true,  5},
    {"LeBron James",          "Cavaliers",     2003, 2010,  7,  548, 29.7, true    },
    {"LeBron James",          "Heat",          2010, 2014,  4,  294, 27.1, true,  2},
    {"LeBron James",          "Cavaliers",     2014, 2018,  4,  301, 27.5, true,  1},
    {"LeBron James",          "Lakers",        2018, 2020,  2,  115, 25.7, true    },
    {"Stephen Curry",         "Warriors",      2009, 2020, 11,  699, 23.5, true,  3},
    {"Russell Westbrook",     "Thunders",      2008, 2019, 11,  821, 22.9, true    },
    {"Russell Westbrook",     "Rockets",       2019, 2020,  1,   53, 27.5, true    },
    {"Kevin Durant",          "Thunders",      2007, 2016,  9,  641, 28.2, true    },
    {"Kevin Durant",          "Warriors",      2016, 2019,  3,  208, 26.0, true,  2},
    {"James Harden",          "Thunders",      2009, 2012,  3,  220, 12.2          },
    {"James Harden",          "Rockets",       2012, 2020,  8,  606, 30.4, true    },
    {"Chris Paul",            "Hornets",       2005, 2011,  6,  425, 22.8, true    },
    {"Chris Paul",            "Clippers",      2011, 2017,  6,  409, 19.1, true    },
    {"Chris Paul",            "Rockets",       2017, 2019,  2,  116, 15.6, true    },
    {"Chris Paul",            "Thunders",      2019, 2020,  1,   63, 17.7, true    },
    {"DeAndre Jordan",        "Clippers",      2008, 2018, 10,  750,  8.8, true    },
    {"DeAndre Jordan",        "Mavericks",     2018, 2019,  1,   50, 11.0, true    },
    {"DeAndre Jordan",        "Knicks",        2019, 2019,  1,   19, 10.9, true    },
    {"DeAndre Jordan",        "Nets",          2019, 2020,  1,   56,  8.3          },
    {"Ricky Rubio",           "Timberwolves",  2011, 2017,  6,  353, 11.1, true    },
    {"Ricky Rubio",           "Jazz",          2017, 2019,  2,  145, 12.7, true    },
    {"Ricky Rubio",           "Suns",          2019, 2020,  1,   57, 13.1, true    },
    {"Rajon Rondo",           "Celtics",       2006, 2014,  9,  527, 11.9, true,  1},
    {"Rajon Rondo",           "Mavericks",     2014, 2015,  1,   46,  9.3, true    },
    {"Rajon Rondo",           "Kings",         2015, 2016,  1,   72, 11.9, true    },
    {"Rajon Rondo",           "Bulls",         2016, 2017,  1,   69,  7.8, true    },
    {"Rajon Rondo",           "Pelicans",      2017, 2018,  1,   65,  8.3, true    },
    {"Rajon Rondo",           "Lakers",        2018, 2020,  2,   94,  7.1          },
    {"Manu Ginobili",         "Spurs",         2002, 2018, 16, 1057, 13.3, false, 4},
    {"Kyrie Irving",          "Cavaliers",     2011, 2017,  6,  381, 20.8, true,  1},
    {"Kyrie Irving",          "Celtics",       2017, 2019,  2,  127, 24.4, true    },
    {"Kyrie Irving",          "Nets",          2019, 2020,  1,   20, 27.4, true    },
    {"Vince Carter",          "Raptors",       1998, 2004,  6,  403, 18.3, true    },
    {"Vince Carter",          "Nets",          2004, 2009,  5,  374, 20.8, true    },
    {"Vince Carter",          "Magic",         2009, 2011,  2,   97, 15.1, true    },
    {"Vince Carter",          "Suns",          2011, 2011,  1,   51, 13.5          },
    {"Vince Carter",          "Mavericks",     2011, 2014,  3,  223, 10.1          },
    {"Vince Carter",          "Grizzlies",     2014, 2017,  3,  199,  8.0          },
    {"Vince Carter",          "Kings",         2017, 2018,  1,   58,  5.4          },
    {"Vince Carter",          "Hawks",         2018, 2020,  2,  136,  5.0          },
    {"Carmelo Anthony",       "Nuggets",       2003, 2011,  8,  564, 25.2, true    },
    {"Carmelo Anthony",       "Knicks",        2011, 2017,  6,  412, 22.4, true    },
    {"Carmelo Anthony",       "Thunders",      2017, 2018,  1,   78, 16.2, true    },
    {"Carmelo Anthony",       "Rockets",       2018, 2019,  1,   10, 13.4          },
    {"Carmelo Anthony",       "Trail Blazers", 2019, 2020,  1,   50, 15.3, true    },
    {"Dwyane Wade",           "Heat",          2003, 2016, 13,  855, 26.6, true,  3},
    {"Dwyane Wade",           "Bulls",         2016, 2017,  1,   60, 18.3, true    },
    {"Dwyane Wade",           "Cavaliers",     2017, 2017,  1,   46, 11.2          },
    {"Dwyane Wade",           "Heat",          2017, 2019,  2,   93, 15.0          },
    {"Joel Embiid",           "76ers",         2016, 2020,  4,  202, 24.1, true    },
    {"Paul George",           "Pacers",        2010, 2017,  7,  448, 17.4, true    },
    {"Paul George",           "Thunders",      2017, 2019,  2,  156, 28.0, true    },
    {"Paul George",           "Clippers",      2019, 2020,  1,   42, 21.0, true    },
    {"Giannis Antetokounmpo", "Bucks",         2013, 2020,  7,  522, 20.0, true    },
    {"Yao Ming",              "Rockets",       2002, 2011,  8,  486, 19.0, true    },
    {"Blake Griffin",         "Clippers",      2010, 2018,  8,  504, 22.6, true    },
    {"Blake Griffin",         "Pistons",       2018, 2020,  2,  118, 21.4, true    },
    {"Damian Lillard",        "Trail Blazers", 2012, 2020,  8,  607, 24.0, true    },
    {"Steve Nash",            "Suns",          1996, 1998,  2,  141,  9.1          },
    {"Steve Nash",            "Mavericks",     1998, 2004,  6,  408, 14.5, true    },
    {"Steve Nash",            "Suns",          2004, 2012,  8,  603, 16.5, true    },
    {"Steve Nash",            "Lakers",        2012, 2014,  2,   65,  6.8, true    },
    {"Dirk Nowitzki",         "Mavericks",     1998, 2019, 21, 1522, 20.7, true,  1},
    {"Pau Gasol",             "Grizzlies",     2001, 2008,  7,  476, 18.9, true    },
    {"Pau Gasol",             "Lakers",        2008, 2014,  6,  429, 17.4, true,  2},
    {"Pau Gasol",             "Bulls",         2014, 2016,  2,  150, 16.5, true    },
    {"Pau Gasol",             "Spurs",         2016, 2019,  3,  168, 10.1          },
    {"Pau Gasol",             "Bucks",         2019, 2019,  1,    3,  1.3          },
    {"Marc Gasol",            "Grizzlies",     2008, 2019, 11,  769, 15.7, true    },
    {"Marc Gasol",            "Raptors",       2018, 2020,  2,   62, 13.6, true,  1},
    {"Grant Hill",            "Pistons",       1994, 2000,  6,  435, 21.1, true    },
    {"Grant Hill",            "Magic",         2000, 2007,  6,  200, 14.4, true    },
    {"Grant Hill",            "Suns",          2007, 2012,  5,  362, 13.2, true    },
    {"Grant Hill",            "Clippers",      2012, 2013,  1,   29,  3.2          },
    {"Ray Allen",             "Bucks",         1996, 2003,  7,  494, 21.8, true    },
    {"Ray Allen",             "Thunders",      2003, 2007,  4,  296, 23.9, true    },
    {"Ray Allen",             "Celtics",       2007, 2012,  5,  358, 16.5, true,  1},
    {"Ray Allen",             "Heat",          2012, 2014,  2,  152, 10.9, false, 1},
    {"Klay Thompson",         "Warriors",      2011, 2019,  8,  615, 19.5, true,  3},
    {"Kristaps Porzingis",    "Knicks",        2015, 2018,  3,  186, 18.1, true    },
    {"Kristaps Porzingis",    "Mavericks",     2019, 2020,  1,   51, 19.2, true    },
    {"Shaquille O'Neal",      "Magic",         1992, 1996,  4,  295, 26.6, true    },
    {"Shaquille O'Neal",      "Lakers",        1996, 2004,  8,  514, 27.2, true,  3},
    {"Shaquille O'Neal",      "Heat",          2004, 2008,  4,  205, 17.3, true,  1},
    {"Shaquille O'Neal",      "Suns",          2008, 2009,  1,  103, 17.8, true    },
    {"Shaquille O'Neal",      "Cavaliers",     2009, 2010,  1,   53, 12.0, true    },
    {"Shaquille O'Neal",      "Celtics",       2010, 2011,  1,   37,  9.2, true    },
    {"JaVale McGee",          "Wizards",       2008, 2012,  4,  255, 10.1          },
    {"JaVale McGee",          "Nuggets",       2012, 2015,  3,  121,  7.0          },
    {"JaVale McGee",          "76ers",         2015, 2015,  1,    6,  3.0          },
    {"JaVale McGee",          "Mavericks",     2015, 2016,  1,   34,  5.1          },
    {"JaVale McGee",          "Warriors",      2016, 2018,  2,  142,  6.1, false, 2},
    {"JaVale McGee",          "Lakers",        2018, 2020,  2,  136,  6.8, true    },
    {"Dwight Howard",         "Magic",         2004, 2012,  8,  621, 20.6, true    },
    {"Dwight Howard",         "Lakers",        2012, 2013,  1,   76, 17.1, true    },
    {"Dwight Howard",         "Rockets",       2013, 2016,  3,  183, 15.8, true    },
    {"Dwight Howard",         "Hawks",         2016, 2017,  1,   74, 13.5, true    },
    {"Dwight Howard",         "Hornets",       2017, 2018,  1,   81, 16.6, true    },
    {"Dwight Howard",         "Wizards",       2018, 2019,  1,    9, 12.8, true    },
    {"Dwight Howard",         "Lakers",        2019, 2020,  1,   62,  7.5          },
    {"Amare Stoudemire",      "Suns",          2002, 2010,  8,  516, 23.1, true    },
    {"Amare Stoudemire",      "Knicks",        2010, 2015,  5,  255, 14.2          },
    {"Amare Stoudemire",      "Mavericks",     2015, 2015,  1,   23, 10.8          },
    {"Amare Stoudemire",      "Heat",          2015, 2016,  1,   52,  5.8          },
    {"Jason Kidd",            "Mavericks",     1994, 1997,  3,  182, 11.7, true    },
    {"Jason Kidd",            "Suns",          1997, 2001,  4,  313, 16.9, true    },
    {"Jason Kidd",            "Nets",          2001, 2008,  7,  506, 13.3, true    },
    {"Jason Kidd",            "Mavericks",     2008, 2012,  4,  318, 10.3, true,  1},
    {"Jason Kidd",            "Knicks",        2012, 2013,  1,   76,  6.0, true    },
    {"Ben Simmons",           "76ers",         2017, 2020,  3,  214, 16.4, true    },
    {"Luka Doncic",           "Mavericks",     2018, 2020,  2,  126, 24.4, true    }
};

// Mock schema
std::shared_ptr<meta::SchemaProviderIf> MockData::mockPlayerTagSchema() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    meta::cpp2::ColumnDef col1;
    col1.name = "name";
    col1.type = meta::cpp2::PropertyType::STRING;
    schema->addField(std::move(col1.name), std::move(col1.type));

    meta::cpp2::ColumnDef col2;
    col2.name = "age";
    col2.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col2.name), std::move(col2.type));

    meta::cpp2::ColumnDef col3;
    col3.name = "playing";
    col3.type = meta::cpp2::PropertyType::BOOL;
    schema->addField(std::move(col3.name), std::move(col3.type));

    meta::cpp2::ColumnDef col4;
    col4.name = "career";
    col4.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col4.name), std::move(col4.type));

    meta::cpp2::ColumnDef col5;
    col5.name = "startYear";
    col5.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col5.name), std::move(col5.type));

    meta::cpp2::ColumnDef col6;
    col6.name = "endYear";
    col6.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col6.name), std::move(col6.type));

    meta::cpp2::ColumnDef col7;
    col7.name = "games";
    col7.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col7.name), std::move(col7.type));

    meta::cpp2::ColumnDef col8;
    col8.name = "avgScore";
    col8.type = meta::cpp2::PropertyType::DOUBLE;
    schema->addField(std::move(col8.name), std::move(col8.type));

    meta::cpp2::ColumnDef col9;
    col9.name = "serveTeams";
    col9.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col9.name), std::move(col9.type));

    // Use default value
    meta::cpp2::ColumnDef col10;
    col10.name = "country";
    col10.type = meta::cpp2::PropertyType::STRING;
    Value defaultVal = "America";
    schema->addField(std::move(col10.name), std::move(col10.type), 0, false, defaultVal);

    // Use nullable
    meta::cpp2::ColumnDef col11;
    col11.name = "champions";
    col11.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col11.name), std::move(col11.type), 0, true);

    // Set ttl property
    if (FLAGS_mock_ttl_col) {
        meta::cpp2::SchemaProp prop;
        prop.set_ttl_col("endYear");
        prop.set_ttl_duration(FLAGS_mock_ttl_duration);
        schema->setProp(prop);
    }
    return schema;
}

std::shared_ptr<meta::SchemaProviderIf> MockData::mockTeamTagSchema() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    meta::cpp2::ColumnDef col;
    col.name = "name";
    col.type = meta::cpp2::PropertyType::STRING;
    schema->addField(std::move(col.name), std::move(col.type));
    return schema;
}

std::shared_ptr<meta::SchemaProviderIf> MockData::mockEdgeSchema() {
    std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
    meta::cpp2::ColumnDef col1;
    col1.name = "playerName";
    col1.type = meta::cpp2::PropertyType::STRING;
    schema->addField(std::move(col1.name), std::move(col1.type));

    meta::cpp2::ColumnDef col2;
    col2.name = "teamName";
    col2.type = meta::cpp2::PropertyType::STRING;
    schema->addField(std::move(col2.name), std::move(col2.type));

    meta::cpp2::ColumnDef col3;
    col3.name = "startYear";
    col3.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col3.name), std::move(col3.type));

    meta::cpp2::ColumnDef col4;
    col4.name = "endYear";
    col4.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col4.name), std::move(col4.type));

    meta::cpp2::ColumnDef col5;
    col5.name = "teamCareer";
    col5.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col5.name), std::move(col5.type));

    meta::cpp2::ColumnDef col6;
    col6.name = "teamGames";
    col6.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col6.name), std::move(col6.type));

    meta::cpp2::ColumnDef col7;
    col7.name = "teamAvgScore";
    col7.type = meta::cpp2::PropertyType::DOUBLE;
    schema->addField(std::move(col7.name), std::move(col7.type));

    // Use default value
    meta::cpp2::ColumnDef col8;
    col8.name = "starting";
    col8.type = meta::cpp2::PropertyType::BOOL;
    Value defaultVal = false;
    schema->addField(std::move(col8.name), std::move(col8.type), 0, false, defaultVal);

    // Use nullable
    meta::cpp2::ColumnDef col9;
    col9.name = "champions";
    col9.type = meta::cpp2::PropertyType::INT64;
    schema->addField(std::move(col9.name), std::move(col9.type), 0, true);

    // Set ttl property
    if (FLAGS_mock_ttl_col) {
        meta::cpp2::SchemaProp prop;
        prop.set_ttl_col("endYear");
        prop.set_ttl_duration(FLAGS_mock_ttl_duration);
        schema->setProp(prop);
    }
    return schema;
}

// Mock data
std::vector<VertexData> MockData::mockVertices() {
    std::vector<VertexData> ret;
    // Multiple vertices, each vertex has two tags, player and team
    // player tagId is 1, team tagId is 2
    for (auto& player : players_) {
        VertexData data;
        data.vId_ = player.name_;
        data.tId_ = 1;

        std::vector<Value>  props;
        props.emplace_back(player.name_);
        props.emplace_back(player.age_);
        props.emplace_back(player.playing_);
        props.emplace_back(player.career_);
        props.emplace_back(player.startYear_);
        props.emplace_back(player.endYear_);
        props.emplace_back(player.games_);
        props.emplace_back(player.avgScore_);
        props.emplace_back(player.serveTeams_);

        // Use default value and nullable
        if (player.country_.compare("") == 0) {
            data.props_ = std::move(props);
            ret.push_back(data);
        } else {
            props.emplace_back(player.country_);
            if (player.champions_) {
                props.emplace_back(player.champions_);
            }
            data.props_ = std::move(props);
            ret.push_back(data);
        }
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

std::vector<EdgeData> MockData::mockEdges() {
    std::vector<EdgeData> ret;
    // Use serve data, EdgeType is 101, EdgeRanking is 102
    for (auto& serve : serve_) {
        EdgeData data;
        data.srcId_ = serve.playerName_;
        data.type_ = 101,
        data.rank_ = 102,
        data.dstId_ = serve.teamName_;

        std::vector<Value>  props;
        props.emplace_back(serve.playerName_);
        props.emplace_back(serve.teamName_);
        props.emplace_back(serve.startYear_);
        props.emplace_back(serve.endYear_);
        props.emplace_back(serve.teamCareer_);
        props.emplace_back(serve.teamGames_);
        props.emplace_back(serve.teamAvgScore_);

        // Use default value and nullable
        if (!serve.starting_) {
            data.props_ = std::move(props);
            ret.push_back(data);
        } else {
            props.emplace_back(serve.starting_);
            if (serve.champions_) {
                props.emplace_back(serve.champions_);
            }
            data.props_ = std::move(props);
            ret.push_back(data);
        }
    }
    return ret;
}

nebula::storage::cpp2::AddVerticesRequest MockData::mockAddVertices(int32_t parts) {
    nebula::storage::cpp2::AddVerticesRequest req;
    req.space_id = 1;
    req.overwritable = true;

    auto retRecs = mockVertices();

    int count = 0;
    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewVertex newVertex;
        nebula::storage::cpp2::NewTag newTag;
        nebula::storage::cpp2::PropDataByRow  row;

        row.set_props(std::move(rec.props_));
        newTag.set_tag_id(rec.tId_);
        newTag.set_props(std::move(row));

        std::vector<nebula::storage::cpp2::NewTag> newTags;
        newTags.push_back(std::move(newTag));

        newVertex.id = rec.vId_;
        newVertex.set_tags(std::move(newTags));
        req.parts[(count % parts) + 1].emplace_back(std::move(newVertex));
    }
    return req;
}

nebula::storage::cpp2::AddEdgesRequest MockData::mockAddEdges(int32_t parts) {
    nebula::storage::cpp2::AddEdgesRequest req;
    req.space_id = 1;
    req.overwritable = true;

    auto retRecs = mockEdges();
    int count = 0;
    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewEdge newEdge;
        nebula::storage::cpp2::EdgeKey edgeKey;
        edgeKey.src = rec.srcId_;
        edgeKey.edge_type = rec.type_;
        edgeKey.ranking = rec.rank_;
        edgeKey.dst = rec.dstId_;

        nebula::storage::cpp2::PropDataByRow  row;
        row.set_props(std::move(rec.props_));

        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(row));

        req.parts[(count % parts) + 1].emplace_back(std::move(newEdge));
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
    // Use serve data, EdgeType is 101, EdgeRanking is 102
    for (auto& serve : serve_) {
        EdgeData data;
        data.srcId_ = serve.playerName_;
        data.type_ = 101,
        data.rank_ = 102,
        data.dstId_ = serve.teamName_;

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

        data.props_ = std::move(props);
        ret.push_back(data);
    }
    return ret;
}

nebula::storage::cpp2::AddVerticesRequest
MockData::mockAddVerticesSpecifiedOrder(int32_t parts) {
    nebula::storage::cpp2::AddVerticesRequest req;
    req.space_id = 1;
    req.overwritable = true;

    auto retRecs = mockVertices();

    int count = 0;
    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewVertex newVertex;
        nebula::storage::cpp2::NewTag newTag;
        nebula::storage::cpp2::PropDataByRow  row;

        row.set_props(std::move(rec.props_));
        newTag.set_tag_id(rec.tId_);
        newTag.set_props(std::move(row));
        if (rec.tId_ == 1) {
            std::vector<std::string> colNames{"serveTeams", "avgScore", "games", "endYear",
                                              "startYear",  "career", "playing", "age",
                                              "name"};
            newTag.set_names(std::move(colNames));
        } else {
            std::vector<std::string> colNames{"name"};
            newTag.set_names(std::move(colNames));
        }
        std::vector<nebula::storage::cpp2::NewTag> newTags;
        newTags.push_back(std::move(newTag));

        newVertex.id = rec.vId_;
        newVertex.set_tags(std::move(newTags));
        req.parts[(count % parts) + 1].emplace_back(std::move(newVertex));
    }
    return req;
}

nebula::storage::cpp2::AddEdgesRequest
MockData::mockAddEdgesSpecifiedOrder(int32_t parts) {
    nebula::storage::cpp2::AddEdgesRequest req;
    req.space_id = 1;
    req.overwritable = true;

    auto retRecs = mockEdges();

    int count = 0;
    for (auto& rec : retRecs) {
        nebula::storage::cpp2::NewEdge newEdge;
        nebula::storage::cpp2::EdgeKey edgeKey;
        edgeKey.src = rec.srcId_;
        edgeKey.edge_type = rec.type_;
        edgeKey.ranking = rec.rank_;
        edgeKey.dst = rec.dstId_;

        nebula::storage::cpp2::PropDataByRow  row;
        row.set_props(std::move(rec.props_));

        newEdge.set_key(std::move(edgeKey));
        newEdge.set_props(std::move(row));

        std::vector<std::string> colNames{"teamAvgScore", "teamGames", "teamCareer",
                                          "endYear", "startYear", "teamName", "playerName"};
        newEdge.set_names(std::move(colNames));
        req.parts[(count % parts) + 1].emplace_back(std::move(newEdge));
    }
    return req;
}

}  // namespace mock
}  // namespace nebula
