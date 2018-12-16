/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "dataman/SchemaWriter.h"
#include "dataman/RowWriter.h"
#include "dataman/RowReader.h"

using namespace nebula;

SchemaWriter schemaAllInts;
SchemaWriter schemaAllBools;
SchemaWriter schemaAllStrings;
SchemaWriter schemaAllDoubles;
SchemaWriter schemaAllVids;
SchemaWriter schemaMix;

std::string dataAllBools;
std::string dataAllInts;
std::string dataAllDoubles;
std::string dataAllStrings;
std::string dataAllVids;
std::string dataMix;


void prepareSchema() {
    for (int i = 0; i < 32; i++) {
        schemaAllInts.appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::storage::cpp2::SupportedType::INT);
        schemaAllBools.appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::storage::cpp2::SupportedType::BOOL);
        schemaAllStrings.appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::storage::cpp2::SupportedType::STRING);
        schemaAllDoubles.appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::storage::cpp2::SupportedType::DOUBLE);
        schemaAllVids.appendCol(
            folly::stringPrintf("col%02d", i),
            nebula::storage::cpp2::SupportedType::VID);
    }

    schemaMix.appendCol("col01",nebula::storage::cpp2::SupportedType::BOOL)
             .appendCol("col02",nebula::storage::cpp2::SupportedType::BOOL)
             .appendCol("col03",nebula::storage::cpp2::SupportedType::BOOL)
             .appendCol("col04",nebula::storage::cpp2::SupportedType::BOOL)
             .appendCol("col05",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col06",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col07",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col08",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col09",nebula::storage::cpp2::SupportedType::STRING)
             .appendCol("col10",nebula::storage::cpp2::SupportedType::STRING)
             .appendCol("col11",nebula::storage::cpp2::SupportedType::STRING)
             .appendCol("col12",nebula::storage::cpp2::SupportedType::STRING)
             .appendCol("col13",nebula::storage::cpp2::SupportedType::FLOAT)
             .appendCol("col14",nebula::storage::cpp2::SupportedType::FLOAT)
             .appendCol("col15",nebula::storage::cpp2::SupportedType::FLOAT)
             .appendCol("col16",nebula::storage::cpp2::SupportedType::FLOAT)
             .appendCol("col17",nebula::storage::cpp2::SupportedType::DOUBLE)
             .appendCol("col18",nebula::storage::cpp2::SupportedType::DOUBLE)
             .appendCol("col19",nebula::storage::cpp2::SupportedType::DOUBLE)
             .appendCol("col20",nebula::storage::cpp2::SupportedType::DOUBLE)
             .appendCol("col21",nebula::storage::cpp2::SupportedType::VID)
             .appendCol("col22",nebula::storage::cpp2::SupportedType::VID)
             .appendCol("col23",nebula::storage::cpp2::SupportedType::VID)
             .appendCol("col24",nebula::storage::cpp2::SupportedType::VID)
             .appendCol("col25",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col26",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col27",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col28",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col29",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col30",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col31",nebula::storage::cpp2::SupportedType::INT)
             .appendCol("col32",nebula::storage::cpp2::SupportedType::INT);
}


void prepareData() {
    RowWriter wInts(&schemaAllInts);
    RowWriter wBools(&schemaAllBools);
    RowWriter wDoubles(&schemaAllDoubles);
    RowWriter wStrings(&schemaAllStrings);
    RowWriter wVids(&schemaAllVids);
    RowWriter wMix(&schemaMix);

    for (int i = 0; i < 32; i++) {
        wBools << true;
        wInts << i;
        wDoubles << 3.1415926;
        wStrings << "Hello World";
        wVids << 0xABCDABCDABCDABCD;
    }

    wMix << true << false << true << false
         << 123 << 456 << 0xFFFFFFFF88888888 << 0xABCDABCDABCDABCD
         << "Hello" << "World" << "Back" << "Future"
         << 1.23 << 2.34 << 3.1415926 << 2.17
         << 1.23 << 2.34 << 3.1415926 << 2.17
         << 0xFFFFFFFF << 0xABABABABABABABAB << 0x0 << -1
         << 0 << 1 << 2 << 3 << 4 << 5 << 6 << 7;

    dataAllBools = wBools.encode();
    dataAllInts = wInts.encode();
    dataAllDoubles = wDoubles.encode();
    dataAllStrings = wStrings.encode();
    dataAllVids = wVids.encode();
    dataMix = wMix.encode();
}


void readMix(int32_t iters) {
    for (int i = 0; i < iters; i++) {
        RowReader reader(&schemaMix, dataMix);
        bool bVal;
        int64_t iVal;
        folly::StringPiece sVal;
        float fVal;
        double dVal;

        reader.getBool(0, bVal);
        folly::doNotOptimizeAway(bVal);
        reader.getBool(1, bVal);
        folly::doNotOptimizeAway(bVal);
        reader.getBool(2, bVal);
        folly::doNotOptimizeAway(bVal);
        reader.getBool(3, bVal);
        folly::doNotOptimizeAway(bVal);
        reader.getInt(4, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(5, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(6, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(7, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getString(8, sVal);
        folly::doNotOptimizeAway(sVal);
        reader.getString(9, sVal);
        folly::doNotOptimizeAway(sVal);
        reader.getString(10, sVal);
        folly::doNotOptimizeAway(sVal);
        reader.getString(11, sVal);
        folly::doNotOptimizeAway(sVal);
        reader.getFloat(12, fVal);
        folly::doNotOptimizeAway(fVal);
        reader.getFloat(13, fVal);
        folly::doNotOptimizeAway(fVal);
        reader.getFloat(14, fVal);
        folly::doNotOptimizeAway(fVal);
        reader.getFloat(15, fVal);
        folly::doNotOptimizeAway(fVal);
        reader.getDouble(16, dVal);
        folly::doNotOptimizeAway(dVal);
        reader.getDouble(17, dVal);
        folly::doNotOptimizeAway(dVal);
        reader.getDouble(18, dVal);
        folly::doNotOptimizeAway(dVal);
        reader.getDouble(19, dVal);
        folly::doNotOptimizeAway(dVal);
        reader.getVid(20, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getVid(21, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getVid(22, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getVid(23, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(24, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(25, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(26, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(27, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(28, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(29, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(30, iVal);
        folly::doNotOptimizeAway(iVal);
        reader.getInt(31, iVal);
        folly::doNotOptimizeAway(iVal);
    }
}


#define READ_VALUE(T, SCHEMA, DATA, FN) \
    for (uint64_t i = 0; i < iters; i++) { \
        RowReader reader(&SCHEMA, DATA); \
        T val; \
        auto it = reader.begin(); \
        for (int j = 0; j < 32; ++j, ++it) { \
            it->get ## FN (val); \
            folly::doNotOptimizeAway(val); \
        } \
    }

#define READ_VALUE_RANDOMLY(T, SCHEMA, DATA, FN) \
    for (uint64_t i = 0; i < iters; i++) { \
        RowReader reader(&SCHEMA, DATA); \
        T val; \
        for (int j = 0; j < 32; j++) { \
            uint32_t idx = folly::Random::rand32(0, 32); \
            reader.get ## FN (idx, val); \
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

