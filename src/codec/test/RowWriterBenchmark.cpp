/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <folly/Benchmark.h>
#include "codec/test/SchemaWriter.h"
#include "codec/test/RowWriterV1.h"
#include "codec/RowWriterV2.h"

using nebula::SchemaWriter;
using nebula::RowWriterV1;
using nebula::RowWriterV2;
using nebula::meta::cpp2::PropertyType;

SchemaWriter schemaShort;
SchemaWriter schemaLong;

const double e = 2.71828182845904523536028747135266249775724709369995;
const float pi = 3.14159265358979;
const std::string str = "Hello world!"; // NOLINT


void prepareSchema(SchemaWriter* schema, size_t numRepeats) {
    int32_t index = 1;
    for (size_t i = 0; i < numRepeats; i++) {
        schema->appendCol(folly::stringPrintf("col%02d", index++),
                          PropertyType::BOOL);
        schema->appendCol(folly::stringPrintf("col%02d", index++),
                          PropertyType::INT64);
        schema->appendCol(folly::stringPrintf("col%02d", index++),
                          PropertyType::TIMESTAMP);
        schema->appendCol(folly::stringPrintf("col%02d", index++),
                          PropertyType::FLOAT);
        schema->appendCol(folly::stringPrintf("col%02d", index++),
                          PropertyType::DOUBLE);
        schema->appendCol(folly::stringPrintf("col%02d", index++),
                          PropertyType::STRING);
    }
}


void writeDataV1(SchemaWriter* schema, int32_t iters) {
    for (int32_t i = 0; i < iters; i++) {
        RowWriterV1 writer(schema);
        for (size_t j = 0; j < schema->getNumFields() / 6; j++) {
            writer << true << j << 1551331827 << pi << e << str;
        }
        std::string encoded = writer.encode();
        folly::doNotOptimizeAway(encoded);
    }
}


void writeDataV2(SchemaWriter* schema, int32_t iters) {
    for (int32_t i = 0; i < iters; i++) {
        RowWriterV2 writer(schema);
        size_t idx = 0;
        for (size_t j = 0; j < schema->getNumFields() / 6; j++) {
            writer.set(idx++, true);
            writer.set(idx++, j);
            writer.set(idx++, 1551331827);
            writer.set(idx++, pi);
            writer.set(idx++, e);
            writer.set(idx++, str);
        }
        writer.finish();
        std::string encoded = writer.moveEncodedStr();
        folly::doNotOptimizeAway(encoded);
    }
}


/*************************
 * Begining of benchmarks
 ************************/
BENCHMARK(WriteShortRowV1, iters) {
    writeDataV1(&schemaShort, iters);
}

BENCHMARK_RELATIVE(WriteShortRowV2, iters) {
    writeDataV2(&schemaShort, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(WriteLongRowV1, iters) {
    writeDataV1(&schemaLong, iters);
}

BENCHMARK_RELATIVE(WriteLongRowV2, iters) {
    writeDataV2(&schemaLong, iters);
}
/*************************
 * End of benchmarks
 ************************/


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    prepareSchema(&schemaShort, 2);
    prepareSchema(&schemaLong, 24);

    folly::runBenchmarks();
    return 0;
}


/*
Benchmarked in WSL 1.0 running on i9-9880H
============================================================================
RowWriterBenchmark.cpp                          relative  time/iter  iters/s
============================================================================
WriteShortRowV1                                              1.57us  636.27K
WriteShortRowV2                                  131.25%     1.20us  835.11K
----------------------------------------------------------------------------
WriteLongRowV1                                               7.07us  141.35K
WriteLongRowV2                                    91.29%     7.75us  129.04K
============================================================================
*/

