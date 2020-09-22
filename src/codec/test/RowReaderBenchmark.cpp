/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include <folly/Benchmark.h>
#include "codec/test/SchemaWriter.h"
#include "codec/test/RowWriterV1.h"
#include "codec/RowWriterV2.h"
#include "codec/RowReaderWrapper.h"

using nebula::SchemaWriter;
using nebula::RowWriterV1;
using nebula::RowWriterV2;
using nebula::RowReader;
using nebula::RowReaderWrapper;
using nebula::meta::cpp2::PropertyType;

SchemaWriter schemaShort;
SchemaWriter schemaLong;

std::string dataShortV1;    // NOLINT
std::string dataShortV2;    // NOLINT
std::string dataLongV1;     // NOLINT
std::string dataLongV2;     // NOLINT

std::vector<size_t> shortRandom;
std::vector<size_t> longRandom;

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


std::string prepareV1Data(SchemaWriter* schema, size_t numRepeats) {
    RowWriterV1 writer(schema);

    for (size_t i = 0; i < numRepeats; i++) {
        writer << true << i << 1551331827 << pi << e << str;
    }

    return writer.encode();
}


std::string prepareV2Data(SchemaWriter* schema, size_t numRepeats) {
    RowWriterV2 writer(schema);

    ssize_t index = 0;
    for (size_t i = 0; i < numRepeats; i++) {
        writer.set(index++, true);
        writer.set(index++, i);
        writer.set(index++, 1551331827);
        writer.set(index++, pi);
        writer.set(index++, e);
        writer.set(index++, str);
    }
    writer.finish();

    return writer.moveEncodedStr();
}


std::vector<size_t> generateRandom(SchemaWriter* schema) {
    size_t numFields = schema->getNumFields();
    std::vector<size_t> randomList;

    for (size_t i = 0; i < numFields; i++) {
        randomList.push_back(i);
    }

    for (size_t i = 0; i < numFields; i++) {
        size_t idx = folly::Random::rand32(numFields);
        if (idx != i) {
            auto temp = randomList[i];
            randomList[i] = randomList[idx];
            randomList[idx] = temp;
        }
    }

    return randomList;
}


void sequentialRead(SchemaWriter* schema, const std::string& encoded, size_t iters) {
    auto reader = RowReaderWrapper::getRowReader(schema, encoded);
    DCHECK_EQ(reader->readerVer(), ((encoded[0] & 0x18) >> 3) + 1);

    for (size_t i = 0; i < iters; i++) {
        for (size_t j = 0; j < schema->getNumFields(); j++) {
            auto v = reader->getValueByIndex(j);
            folly::doNotOptimizeAway(v);
        }
    }
}


void randomRead(SchemaWriter* schema,
                const std::string& encoded,
                const std::vector<size_t>& randomList,
                size_t iters) {
    auto reader = RowReaderWrapper::getRowReader(schema, encoded);
    DCHECK_EQ(reader->readerVer(), ((encoded[0] & 0x18) >> 3) + 1);

    for (size_t i = 0; i < iters; i++) {
        for (size_t j = 0; j < randomList.size(); j++) {
            auto v = reader->getValueByIndex(randomList[j]);
            folly::doNotOptimizeAway(v);
        }
    }
}


void sequentialTest(SchemaWriter* schema,
                    const std::string& encodedV1,
                    const std::string& encodedV2) {
    auto reader1 = RowReaderWrapper::getRowReader(schema, encodedV1);
    DCHECK_EQ(reader1->readerVer(), ((encodedV1[0] & 0x18) >> 3) + 1);
    auto reader2 = RowReaderWrapper::getRowReader(schema, encodedV2);
    DCHECK_EQ(reader2->readerVer(), ((encodedV2[0] & 0x18) >> 3) + 1);

    for (size_t i = 0; i < schema->getNumFields(); i++) {
        auto v1 = reader1->getValueByIndex(i);
        auto v2 = reader2->getValueByIndex(i);
        EXPECT_EQ(v1, v2);
    }
}


void randomTest(SchemaWriter* schema,
                const std::string& encodedV1,
                const std::string& encodedV2,
                const std::vector<size_t>& randomList) {
    auto reader1 = RowReaderWrapper::getRowReader(schema, encodedV1);
    DCHECK_EQ(reader1->readerVer(), ((encodedV1[0] & 0x18) >> 3) + 1);
    auto reader2 = RowReaderWrapper::getRowReader(schema, encodedV2);
    DCHECK_EQ(reader2->readerVer(), ((encodedV2[0] & 0x18) >> 3) + 1);

    for (size_t i = 0; i < randomList.size(); i++) {
        auto v1 = reader1->getValueByIndex(randomList[i]);
        auto v2 = reader2->getValueByIndex(randomList[i]);
        EXPECT_EQ(v1, v2);
    }
}


/*************************
 * Begining of Tests
 ************************/
TEST(RowReader, SequentialShort) {
    sequentialTest(&schemaShort, dataShortV1, dataShortV2);
}

TEST(RowReader, SequentialLong) {
    sequentialTest(&schemaLong, dataLongV1, dataLongV2);
}

TEST(RowReader, RandomShort) {
    randomTest(&schemaShort, dataShortV1, dataShortV2, shortRandom);
}

TEST(RowReader, RandomLong) {
    randomTest(&schemaLong, dataLongV1, dataLongV2, longRandom);
}
/*************************
 * End of Tests
 ************************/


/*************************
 * Begining of benchmarks
 ************************/
BENCHMARK(seq_read_short_v1, iters) {
    sequentialRead(&schemaShort, dataShortV1, iters);
}
BENCHMARK_RELATIVE(seq_read_short_v2, iters) {
    sequentialRead(&schemaShort, dataShortV2, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(seq_read_long_v1, iters) {
    sequentialRead(&schemaLong, dataLongV1, iters);
}
BENCHMARK_RELATIVE(seq_read_long_v2, iters) {
    sequentialRead(&schemaLong, dataLongV2, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(random_read_short_v1, iters) {
    randomRead(&schemaShort, dataShortV1, shortRandom, iters);
}
BENCHMARK_RELATIVE(random_read_short_v2, iters) {
    randomRead(&schemaShort, dataShortV2, shortRandom, iters);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(random_read_long_v1, iters) {
    randomRead(&schemaLong, dataLongV1, longRandom, iters);
}
BENCHMARK_RELATIVE(random_read_long_v2, iters) {
    randomRead(&schemaLong, dataLongV2, longRandom, iters);
}
/*************************
 * End of benchmarks
 ************************/


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    prepareSchema(&schemaShort, 2);
    prepareSchema(&schemaLong, 24);

    dataShortV1 = prepareV1Data(&schemaShort, 2);
    dataLongV1 = prepareV1Data(&schemaLong, 24);
    dataShortV2 = prepareV2Data(&schemaShort, 2);
    dataLongV2 = prepareV2Data(&schemaLong, 24);

    shortRandom = generateRandom(&schemaShort);
    longRandom = generateRandom(&schemaLong);

    if (FLAGS_benchmark) {
        folly::runBenchmarks();
        return 0;
    } else {
        return RUN_ALL_TESTS();
    }
}

/*
Benchmarked in WSL 1.0 running on i9-9880H
============================================================================
RowReaderBenchmark.cpp                          relative  time/iter  iters/s
============================================================================
seq_read_short_v1                                            1.14us  878.08K
seq_read_short_v2                                184.82%   616.19ns    1.62M
----------------------------------------------------------------------------
seq_read_long_v1                                            15.10us   66.23K
seq_read_long_v2                                 189.05%     7.99us  125.20K
----------------------------------------------------------------------------
random_read_short_v1                                         1.26us  794.68K
random_read_short_v2                             180.33%   697.83ns    1.43M
----------------------------------------------------------------------------
random_read_long_v1                                         15.81us   63.24K
random_read_long_v2                              194.92%     8.11us  123.27K
============================================================================
*/

