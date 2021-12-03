/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <unordered_set>

#include "common/base/Base.h"
#include "common/geo/GeoIndex.h"

namespace nebula {
namespace geo {

uint64_t asUint64(int64_t i) {
  const char* c = reinterpret_cast<const char*>(&i);
  uint64_t u = *reinterpret_cast<const uint64_t*>(c);
  return u;
}

std::vector<uint64_t> toUint64Vector(std::vector<int64_t> expect) {
  std::vector<uint64_t> transformedExpect;
  transformedExpect.reserve(expect.size());
  for (int64_t i : expect) {
    transformedExpect.push_back(asUint64(i));
  }
  return transformedExpect;
}

// The tested wkt data is generated by https://clydedacruz.github.io/openstreetmap-wkt-playground/
// And the expected result comes from BigQuery
TEST(indexCells, point) {
  geo::RegionCoverParams rc;
  geo::GeoIndex geoIndex(rc);
  {
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto cells = geoIndex.indexCells(point);
    EXPECT_EQ(toUint64Vector({1153277837650709461}), cells);
  }
  {
    auto point = Geography::fromWKT("POINT(179.9 -89.999)").value();
    auto cells = geoIndex.indexCells(point);
    EXPECT_EQ(toUint64Vector({-5764607523209916331}), cells);
  }
  {
    auto point = Geography::fromWKT("POINT(0.0 0.0)").value();
    auto cells = geoIndex.indexCells(point);
    EXPECT_EQ(toUint64Vector({1152921504606846977}), cells);
  }
  {
    auto point = Geography::fromWKT("POINT(-36.843143 79.9999999)").value();
    auto cells = geoIndex.indexCells(point);
    EXPECT_EQ(toUint64Vector({5738492864430648919}), cells);
  }
}

TEST(indexCells, lineString) {
  geo::RegionCoverParams rc;
  geo::GeoIndex geoIndex(rc);
  {
    auto line = Geography::fromWKT("LINESTRING(1.0 1.0, 2.0 2.0)").value();
    auto cells = geoIndex.indexCells(line);
    std::vector<int64_t> expect{
        1153290940513779712,
        1154047404446580736,
        1154064996699734016,
        1154135365443911680,
        1154164685843464192,
        1154328879221964800,
        1154346471676444672,
    };
    EXPECT_EQ(toUint64Vector(expect), cells);
  }
  {
    auto line = Geography::fromWKT(
                    "LINESTRING(-100.03601074218751 40.74400563300867,-96.96516036987305 "
                    "39.60634945766583,-91.84398651123048 39.706526341505366)")
                    .value();
    auto cells = geoIndex.indexCells(line);
    std::vector<int64_t> expect{-8676255050873438208,
                                -8675903207152549888,
                                -8674214357292285952,
                                -8665770107990966272,
                                -8665207158037544960,
                                -8664644208084123648,
                                -8664081258130702336,
                                -8656692539992047616};
    EXPECT_EQ(toUint64Vector(expect), cells);
  }
  {
    auto line = Geography::fromWKT(
                    "LINESTRING(-109.18024063110352 40.96952973563833,-102.11740493774414 "
                    "40.98832114106014,-102.00119018554688 37.07120386611709,-108.97098541259767 "
                    "37.00392356248513,-109.09063339233398 40.94178643285866)")
                    .value();
    auto cells = geoIndex.indexCells(line);
    std::vector<int64_t> expect{-8715591178868752384,
                                -8714183803985199104,
                                -8712494954124935168,
                                -8702080379986640896,
                                -8699828580172955648,
                                -8693407432266743808,
                                -8688569581104529408,
                                -8686317781290844160};
    EXPECT_EQ(toUint64Vector(expect), cells);
  }
}

TEST(indexCells, polygon) {
  geo::RegionCoverParams rc;
  geo::GeoIndex geoIndex(rc);
  {
    auto polygon = Geography::fromWKT(
                       "POLYGON((-105.59286117553711 43.12955341892069,-98.76176834106447 "
                       "44.11877181138391,-93.97396087646486 "
                       "38.023348535033705,-105.59286117553711 43.12955341892069))")
                       .value();
    auto cells = geoIndex.indexCells(polygon);
    std::vector<int64_t> expect{-8690821380918214656,
                                -8686317781290844160,
                                -8684065981477158912,
                                -8678436481942945792,
                                -8665770107990966272,
                                -8665207158037544960,
                                -8664644208084123648,
                                -8662955358223859712};
    EXPECT_EQ(toUint64Vector(expect), cells);
  }
  {
    auto polygon =
        Geography::fromWKT(
            "POLYGON((-107.24699020385744 45.21638951846552,-91.75283432006836 "
            "46.158312926461235,-90.07295608520508 35.17914020576748,-109.77504730224612 "
            "38.65334327823746,-107.24699020385744 45.21638951846552))")
            .value();
    auto cells = geoIndex.indexCells(polygon);
    std::vector<int64_t> expect{5958262307011166208,
                                5967269506265907200,
                                5994291104030130176,
                                6002172403378028544,
                                -8714465278961909760,
                                -8702080379986640896,
                                -8696450880452427776,
                                -8687443681197686784,
                                -8678436481942945792,
                                -8669429282688204800,
                                -8660422083433463808,
                                -8651414884178722816};
    EXPECT_EQ(toUint64Vector(expect), cells);
  }
  {
    auto polygon =
        Geography::fromWKT(
            "POLYGON((-107.17094421386722 51.23698687887105,-100.24475097656253 "
            "50.57407993312597,-101.63520812988283 47.57050358015326,-108.1597137451172 "
            "47.614032638527846,-107.17094421386722 51.23698687887105),(-106.00682258605956 "
            "50.35416859141216,-105.23014068603514 50.212503875989455,-105.55715560913085 "
            "49.755319847594194,-106.36962890624999 49.95817799043337,-106.00682258605956 "
            "50.35416859141216),(-103.90560150146483 49.21126151433475,-102.1109676361084 "
            "49.32232483567492,-102.99759864807127 48.52160729809822,-103.90560150146483 "
            "49.21126151433475))")
            .value();
    auto cells = geoIndex.indexCells(polygon);
    std::vector<int64_t> expect{5969732412312125440,
                                5971192563753811968,
                                5971491630916567040,
                                5972899005800120320,
                                5986409804682231808,
                                5988661604495917056,
                                5990913404309602304,
                                5997668803750658048};
    EXPECT_EQ(toUint64Vector(expect), cells);
  }
}

TEST(indexCells, tunningRegionCoverParamsForPoint) {
  // TODO(jie)
}
TEST(indexCells, tunningRegionCoverParamsForLineString) {
  // TODO(jie)
}
TEST(indexCells, tunningRegionCoverParamsForPolygon) {
  // TODO(jie)
}

TEST(intersects, point) {
  geo::RegionCoverParams rc(0, 30, 8);
  {
    bool pointsOnly = false;
    geo::GeoIndex geoIndex(rc, pointsOnly);
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto got = geoIndex.intersects(point);

    std::vector<ScanRange> expect;
    auto cells = toUint64Vector({1153277837650709461});
    expect.reserve(cells.size());
    for (uint64_t cellId : cells) {
      expect.emplace_back(cellId);
    }

    EXPECT_EQ(expect, got);
  }
  {
    bool pointsOnly = false;  // The indexed geo column only has points;
    geo::GeoIndex geoIndex(rc, pointsOnly);
    auto point = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto got = geoIndex.intersects(point);

    std::vector<ScanRange> expect;
    auto cells = toUint64Vector({1153277837650709461});
    expect.reserve(cells.size());
    for (uint64_t cellId : cells) {
      expect.emplace_back(cellId);
    }

    EXPECT_EQ(expect, got);
  }
}

TEST(intersects, lineString) {
  geo::RegionCoverParams rc(0, 30, 8);
  {
    bool pointsOnly = false;
    geo::GeoIndex geoIndex(rc, pointsOnly);
    auto line = Geography::fromWKT("LINESTRING(68.9 48.9,76.1 35.5,125.7 28.2)").value();
    auto got = geoIndex.intersects(line);

    std::vector<ScanRange> expect;
    auto cells = toUint64Vector({3765009288481734656,
                                 3818771009033469952,
                                 3909124476557590528,
                                 3928264774973915136,
                                 4017210867614482432,
                                 4053239664633446400,
                                 4089268461652410368,
                                 4773815605012725760});
    expect.reserve(cells.size());
    for (uint64_t cellId : cells) {
      expect.emplace_back(cellId);
    }

    EXPECT_EQ(expect, got);
  }
  {
    bool pointsOnly = true;  // The indexed geo column only has points;
    geo::GeoIndex geoIndex(rc, pointsOnly);
    auto line = Geography::fromWKT("POINT(1.0 1.0)").value();
    auto got = geoIndex.intersects(line);

    std::vector<ScanRange> expect;
    auto cells = toUint64Vector({3765009288481734656,
                                 3818771009033469952,
                                 3909124476557590528,
                                 3928264774973915136,
                                 4017210867614482432,
                                 4053239664633446400,
                                 4089268461652410368,
                                 4773815605012725760});
    expect.reserve(cells.size());
    for (uint64_t cellId : cells) {
      expect.emplace_back(cellId);
    }

    EXPECT_EQ(expect, got);
  }
}

TEST(intersects, polygon) {
  geo::RegionCoverParams rc(0, 30, 8);
  {
    bool pointsOnly = false;
    geo::GeoIndex geoIndex(rc, pointsOnly);
    auto point =
        Geography::fromWKT("POLYGON((102.5 33.5, 110.6 36.9,113.6 30.4,102.7 27.3,102.5 33.5))")
            .value();
    auto got = geoIndex.intersects(point);

    std::vector<ScanRange> expect;
    auto cells = toUint64Vector({3759379788947521536,
                                 3879094614979772416,
                                 3915809507254468608,
                                 3917005775905488896,
                                 3922635275439702016,
                                 3931642474694443008,
                                 3949656873203924992,
                                 3958664072458665984});
    expect.reserve(cells.size());
    for (uint64_t cellId : cells) {
      expect.emplace_back(cellId);
    }

    EXPECT_EQ(expect, got);
  }
  {
    bool pointsOnly = false;  // The indexed geo column only has points;
    geo::GeoIndex geoIndex(rc, pointsOnly);
    auto point =
        Geography::fromWKT("POLYGON((102.5 33.5, 110.6 36.9,113.6 30.4,102.7 27.3,102.5 33.5))")
            .value();
    auto got = geoIndex.intersects(point);

    std::vector<ScanRange> expect;
    auto cells = toUint64Vector({3759379788947521536,
                                 3879094614979772416,
                                 3915809507254468608,
                                 3917005775905488896,
                                 3922635275439702016,
                                 3931642474694443008,
                                 3949656873203924992,
                                 3958664072458665984});
    expect.reserve(cells.size());
    for (uint64_t cellId : cells) {
      expect.emplace_back(cellId);
    }

    EXPECT_EQ(expect, got);
  }
}

}  // namespace geo
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
