/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "dataman/SchemaWriter.h"
#include "dataman/RowWriter.h"

using namespace nebula;
using namespace nebula::storage;

SchemaWriter schemaAllInts;
SchemaWriter schemaAllBools;
SchemaWriter schemaAllStrings;
SchemaWriter schemaAllDoubles;
SchemaWriter schemaAllVids;
SchemaWriter schemaMix;

void prepareSchema() {
    for (int i = 0; i < 32; i++) {
        schemaAllInts.appendCol(folly::stringPrintf("col%02d", i),
                                cpp2::SupportedType::INT);
        schemaAllBools.appendCol(folly::stringPrintf("col%02d", i),
                                 cpp2::SupportedType::BOOL);
        schemaAllStrings.appendCol(folly::stringPrintf("col%02d", i),
                                   cpp2::SupportedType::STRING);
        schemaAllDoubles.appendCol(folly::stringPrintf("col%02d", i),
                                   cpp2::SupportedType::DOUBLE);
        schemaAllVids.appendCol(folly::stringPrintf("col%02d", i),
                                cpp2::SupportedType::VID);
    }

    schemaMix.appendCol("col01", cpp2::SupportedType::BOOL)
             .appendCol("col02", cpp2::SupportedType::BOOL)
             .appendCol("col03", cpp2::SupportedType::BOOL)
             .appendCol("col04", cpp2::SupportedType::BOOL)
             .appendCol("col05", cpp2::SupportedType::INT)
             .appendCol("col06", cpp2::SupportedType::INT)
             .appendCol("col07", cpp2::SupportedType::INT)
             .appendCol("col08", cpp2::SupportedType::INT)
             .appendCol("col09", cpp2::SupportedType::STRING)
             .appendCol("col10", cpp2::SupportedType::STRING)
             .appendCol("col11", cpp2::SupportedType::STRING)
             .appendCol("col12", cpp2::SupportedType::STRING)
             .appendCol("col13", cpp2::SupportedType::FLOAT)
             .appendCol("col14", cpp2::SupportedType::FLOAT)
             .appendCol("col15", cpp2::SupportedType::FLOAT)
             .appendCol("col16", cpp2::SupportedType::FLOAT)
             .appendCol("col17", cpp2::SupportedType::DOUBLE)
             .appendCol("col18", cpp2::SupportedType::DOUBLE)
             .appendCol("col19", cpp2::SupportedType::DOUBLE)
             .appendCol("col20", cpp2::SupportedType::DOUBLE)
             .appendCol("col21", cpp2::SupportedType::VID)
             .appendCol("col22", cpp2::SupportedType::VID)
             .appendCol("col23", cpp2::SupportedType::VID)
             .appendCol("col24", cpp2::SupportedType::VID)
             .appendCol("col25", cpp2::SupportedType::INT)
             .appendCol("col26", cpp2::SupportedType::INT)
             .appendCol("col27", cpp2::SupportedType::INT)
             .appendCol("col28", cpp2::SupportedType::INT)
             .appendCol("col29", cpp2::SupportedType::INT)
             .appendCol("col30", cpp2::SupportedType::INT)
             .appendCol("col31", cpp2::SupportedType::INT)
             .appendCol("col32", cpp2::SupportedType::INT);
}


void writeMix(SchemaProviderIf* schema, int32_t iters) {
    for (int32_t i = 0; i < iters; i++) {
        RowWriter writer(schema);
        writer << true << false << true << false
               << 123 << 456 << 0xFFFFFFFF88888888 << 0xABCDABCDABCDABCD
               << "Hello" << "World" << "Back" << "Future"
               << 1.23 << 2.34 << 3.1415926 << 2.17
               << 1.23 << 2.34 << 3.1415926 << 2.17
               << 0xFFFFFFFF << 0xABABABABABABABAB << 0x0 << -1
               << 0 << 1 << 2 << 3 << 4 << 5 << 6 << 7;
        folly::doNotOptimizeAway(writer);
    }
}


template<typename T>
void writeValues(SchemaProviderIf* schema, T val, int32_t iters) {
    for (int32_t i = 0; i < iters; i++) {
        RowWriter writer(schema);
        for (int j = 0; j < 32; j++) {
            writer << val;
        }
        folly::doNotOptimizeAway(writer);
    }
}


/*************************
 * Begining of benchmarks
 ************************/
BENCHMARK(bool_no_schema, iters) {
    writeValues(nullptr, true, iters);
}
BENCHMARK(bool_with_schema, iters) {
    writeValues(&schemaAllBools, true, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(int_no_schema, iters) {
    writeValues(nullptr, 101, iters);
}
BENCHMARK(int_with_schema, iters) {
    writeValues(&schemaAllInts, 101, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(double_no_schema, iters) {
    writeValues(nullptr, 3.1415926, iters);
}
BENCHMARK(double_with_schema, iters) {
    writeValues(&schemaAllDoubles, 3.1415926, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(vid_no_schema, iters) {
    writeValues(nullptr, 0xFFFFFFFFFFFFFFFF, iters);
}
BENCHMARK(vid_with_schema, iters) {
    writeValues(&schemaAllVids, 0xFFFFFFFFFFFFFFFF, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(string_no_schema, iters) {
    writeValues(nullptr, "Hello World!", iters);
}
BENCHMARK(string_with_schema, iters) {
    writeValues(&schemaAllStrings, "Hello World!", iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(mix_no_schema, iters) {
    writeMix(nullptr, iters);
}
BENCHMARK(mix_with_schema, iters) {
    writeMix(&schemaMix, iters);
}
/*************************
 * End of benchmarks
 ************************/


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    prepareSchema();

    folly::runBenchmarks();
    return 0;
}


/*
Benchmarked in VirtualBox running on i7-8650
============================================================================
RowWriterBenchmark.cpp                          relative  time/iter  iters/s
============================================================================
bool_no_schema                                              14.62us   68.42K
bool_with_schema                                             2.39us  418.81K
----------------------------------------------------------------------------
int_no_schema                                               14.70us   68.04K
int_with_schema                                              2.40us  416.92K
----------------------------------------------------------------------------
double_no_schema                                            14.58us   68.56K
double_with_schema                                           2.39us  418.15K
----------------------------------------------------------------------------
vid_no_schema                                               14.68us   68.12K
vid_with_schema                                              2.40us  415.85K
----------------------------------------------------------------------------
string_no_schema                                            15.83us   63.19K
string_with_schema                                           3.47us  288.44K
----------------------------------------------------------------------------
mix_no_schema                                               14.44us   69.24K
mix_with_schema                                              2.51us  397.92K
============================================================================
*/

