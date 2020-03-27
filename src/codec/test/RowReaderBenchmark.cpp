/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "dataman/SchemaWriter.h"
#include "dataman/RowWriter.h"
#include "dataman/RowReader.h"

using nebula::SchemaWriter;
using nebula::RowWriter;
using nebula::RowReader;

auto schemaAllInts = std::make_shared<SchemaWriter>();
auto schemaAllBools = std::make_shared<SchemaWriter>();
auto schemaAllStrings = std::make_shared<SchemaWriter>();
auto schemaAllDoubles = std::make_shared<SchemaWriter>();
auto schemaAllVids = std::make_shared<SchemaWriter>();
auto schemaAllTimestamps = std::make_shared<SchemaWriter>();
auto schemaMix = std::make_shared<SchemaWriter>();

static std::string dataAllBools;        // NOLINT
static std::string dataAllInts;         // NOLINT
static std::string dataAllDoubles;      // NOLINT
static std::string dataAllStrings;      // NOLINT
static std::string dataAllVids;         // NOLINT
static std::string dataAllTimestamps;	// NOLINT
static std::string dataMix;             // NOLINT


void prepareSchema() {
    for (int i = 0; i < 32; i++) {
        schemaAllInts->appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::cpp2::SupportedType::INT);
        schemaAllBools->appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::cpp2::SupportedType::BOOL);
        schemaAllStrings->appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::cpp2::SupportedType::STRING);
        schemaAllDoubles->appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::cpp2::SupportedType::DOUBLE);
        schemaAllVids->appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::cpp2::SupportedType::VID);
        schemaAllTimestamps->appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::cpp2::SupportedType::TIMESTAMP);
    }

    schemaMix->appendCol("col01", nebula::cpp2::SupportedType::BOOL)
             .appendCol("col02", nebula::cpp2::SupportedType::BOOL)
             .appendCol("col03", nebula::cpp2::SupportedType::BOOL)
             .appendCol("col04", nebula::cpp2::SupportedType::BOOL)
             .appendCol("col05", nebula::cpp2::SupportedType::INT)
             .appendCol("col06", nebula::cpp2::SupportedType::INT)
             .appendCol("col07", nebula::cpp2::SupportedType::INT)
             .appendCol("col08", nebula::cpp2::SupportedType::INT)
             .appendCol("col09", nebula::cpp2::SupportedType::STRING)
             .appendCol("col10", nebula::cpp2::SupportedType::STRING)
             .appendCol("col11", nebula::cpp2::SupportedType::STRING)
             .appendCol("col12", nebula::cpp2::SupportedType::STRING)
             .appendCol("col13", nebula::cpp2::SupportedType::FLOAT)
             .appendCol("col14", nebula::cpp2::SupportedType::FLOAT)
             .appendCol("col15", nebula::cpp2::SupportedType::FLOAT)
             .appendCol("col16", nebula::cpp2::SupportedType::FLOAT)
             .appendCol("col17", nebula::cpp2::SupportedType::DOUBLE)
             .appendCol("col18", nebula::cpp2::SupportedType::DOUBLE)
             .appendCol("col19", nebula::cpp2::SupportedType::DOUBLE)
             .appendCol("col20", nebula::cpp2::SupportedType::DOUBLE)
             .appendCol("col21", nebula::cpp2::SupportedType::VID)
             .appendCol("col22", nebula::cpp2::SupportedType::VID)
             .appendCol("col23", nebula::cpp2::SupportedType::VID)
             .appendCol("col24", nebula::cpp2::SupportedType::VID)
             .appendCol("col25", nebula::cpp2::SupportedType::TIMESTAMP)
             .appendCol("col26", nebula::cpp2::SupportedType::TIMESTAMP)
             .appendCol("col27", nebula::cpp2::SupportedType::TIMESTAMP)
             .appendCol("col28", nebula::cpp2::SupportedType::TIMESTAMP)
             .appendCol("col29", nebula::cpp2::SupportedType::INT)
             .appendCol("col30", nebula::cpp2::SupportedType::INT)
             .appendCol("col31", nebula::cpp2::SupportedType::INT)
             .appendCol("col32", nebula::cpp2::SupportedType::INT);
}


void prepareData() {
    RowWriter wInts(schemaAllInts);
    RowWriter wBools(schemaAllBools);
    RowWriter wDoubles(schemaAllDoubles);
    RowWriter wStrings(schemaAllStrings);
    RowWriter wVids(schemaAllVids);
    RowWriter wTimestamps(schemaAllTimestamps);
    RowWriter wMix(schemaMix);

    for (int i = 0; i < 32; i++) {
        wBools << true;
        wInts << i;
        wDoubles << 3.1415926;
        wStrings << "Hello World";
        wVids << 0xABCDABCDABCDABCD;
        wTimestamps << 1551331827;
    }

    wMix << true << false << true << false
         << 123 << 456 << 0xFFFFFFFF88888888 << 0xABCDABCDABCDABCD
         << "Hello" << "World" << "Back" << "Future"
         << 1.23 << 2.34 << 3.1415926 << 2.17
         << 1.23 << 2.34 << 3.1415926 << 2.17
         << 0xFFFFFFFF << 0xABABABABABABABAB << 0x0 << -1
         << 1551331827 << 1551331827 << 1551331827 << 1551331827
         << 0 << 1 << 2 << 3;

    dataAllBools = wBools.encode();
    dataAllInts = wInts.encode();
    dataAllDoubles = wDoubles.encode();
    dataAllStrings = wStrings.encode();
    dataAllVids = wVids.encode();
    dataAllTimestamps = wTimestamps.encode();
    dataMix = wMix.encode();
}


void readMix(int32_t iters) {
    for (int i = 0; i < iters; i++) {
        auto reader = RowReader::getRowReader(dataMix, schemaMix);
        bool bVal;
        int64_t iVal;
        folly::StringPiece sVal;
        float fVal;
        double dVal;

        reader->getBool(0, bVal);
        folly::doNotOptimizeAway(bVal);
        reader->getBool(1, bVal);
        folly::doNotOptimizeAway(bVal);
        reader->getBool(2, bVal);
        folly::doNotOptimizeAway(bVal);
        reader->getBool(3, bVal);
        folly::doNotOptimizeAway(bVal);
        reader->getInt(4, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(5, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(6, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(7, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getString(8, sVal);
        folly::doNotOptimizeAway(sVal);
        reader->getString(9, sVal);
        folly::doNotOptimizeAway(sVal);
        reader->getString(10, sVal);
        folly::doNotOptimizeAway(sVal);
        reader->getString(11, sVal);
        folly::doNotOptimizeAway(sVal);
        reader->getFloat(12, fVal);
        folly::doNotOptimizeAway(fVal);
        reader->getFloat(13, fVal);
        folly::doNotOptimizeAway(fVal);
        reader->getFloat(14, fVal);
        folly::doNotOptimizeAway(fVal);
        reader->getFloat(15, fVal);
        folly::doNotOptimizeAway(fVal);
        reader->getDouble(16, dVal);
        folly::doNotOptimizeAway(dVal);
        reader->getDouble(17, dVal);
        folly::doNotOptimizeAway(dVal);
        reader->getDouble(18, dVal);
        folly::doNotOptimizeAway(dVal);
        reader->getDouble(19, dVal);
        folly::doNotOptimizeAway(dVal);
        reader->getVid(20, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getVid(21, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getVid(22, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getVid(23, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(24, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(25, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(26, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(27, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(28, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(29, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(30, iVal);
        folly::doNotOptimizeAway(iVal);
        reader->getInt(31, iVal);
        folly::doNotOptimizeAway(iVal);
    }
}


#define READ_VALUE(T, SCHEMA, DATA, FN) \
    for (uint64_t i = 0; i < iters; i++) { \
        auto reader = RowReader::getRowReader(DATA, SCHEMA); \
        T val; \
        auto it = reader->begin(); \
        for (int j = 0; j < 32; ++j, ++it) { \
            it->get ## FN(val); \
            folly::doNotOptimizeAway(val); \
        } \
    }

#define READ_VALUE_RANDOMLY(T, SCHEMA, DATA, FN) \
    for (uint64_t i = 0; i < iters; i++) { \
        auto reader = RowReader::getRowReader(DATA, SCHEMA); \
        T val; \
        for (int j = 0; j < 32; j++) { \
            uint32_t idx = folly::Random::rand32(0, 32); \
            reader->get ## FN(idx, val); \
            folly::doNotOptimizeAway(val); \
        } \
    }


/*************************
 * Begining of benchmarks
 ************************/
BENCHMARK(read_bool_seq, iters) {
    READ_VALUE(bool, schemaAllBools, dataAllBools, Bool);
}
BENCHMARK(read_bool_rand, iters) {
    READ_VALUE_RANDOMLY(bool, schemaAllBools, dataAllBools, Bool);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(read_int_seq, iters) {
    READ_VALUE(int64_t, schemaAllInts, dataAllInts, Int);
}
BENCHMARK(read_int_rand, iters) {
    READ_VALUE_RANDOMLY(int64_t, schemaAllInts, dataAllInts, Int);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(read_double_seq, iters) {
    READ_VALUE(double, schemaAllDoubles, dataAllDoubles, Double);
}
BENCHMARK(read_double_rand, iters) {
    READ_VALUE_RANDOMLY(double, schemaAllDoubles, dataAllDoubles, Double);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(read_string_seq, iters) {
    READ_VALUE(folly::StringPiece, schemaAllStrings, dataAllStrings, String);
}
BENCHMARK(read_string_rand, iters) {
    READ_VALUE_RANDOMLY(folly::StringPiece, schemaAllStrings, dataAllStrings, String);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(read_vid_seq, iters) {
    READ_VALUE(int64_t, schemaAllVids, dataAllVids, Vid);
}
BENCHMARK(read_vid_rand, iters) {
    READ_VALUE_RANDOMLY(int64_t, schemaAllVids, dataAllVids, Vid);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(read_timestamp_seq, iters) {
    READ_VALUE(int64_t, schemaAllTimestamps, dataAllTimestamps, Int);
}
BENCHMARK(read_timestamp_rand, iters) {
    READ_VALUE_RANDOMLY(int64_t, schemaAllTimestamps, dataAllTimestamps, Int);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(read_mix, iters) {
    readMix(iters);
}
/*************************
 * End of benchmarks
 ************************/


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    prepareSchema();
    prepareData();

    folly::runBenchmarks();
    return 0;
}

/*
Benchmarked in VirtualBox running on i7-8650
============================================================================
RowReaderBenchmark.cpp                          relative  time/iter  iters/s
============================================================================
read_bool_seq                                                1.20us  831.66K
read_bool_rand                                               7.84us  127.58K
----------------------------------------------------------------------------
read_int_seq                                                 1.20us  834.45K
read_int_rand                                                8.50us  117.60K
----------------------------------------------------------------------------
read_double_seq                                              1.19us  837.64K
read_double_rand                                             8.10us  123.53K
----------------------------------------------------------------------------
read_string_seq                                              1.38us  724.65K
read_string_rand                                             8.60us  116.32K
----------------------------------------------------------------------------
read_vid_seq                                                 1.20us  831.74K
read_vid_rand                                                8.14us  122.84K
----------------------------------------------------------------------------
read_mix                                                     8.07us  123.86K
============================================================================
*/

