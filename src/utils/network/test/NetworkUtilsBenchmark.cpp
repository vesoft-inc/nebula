/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <folly/Benchmark.h>
#include "common/network/NetworkUtils.h"

using nebula::network::NetworkUtils;

std::string intToIp(uint32_t ip) {
    std::deque<std::string> parts;
    for (int i = 0; i < 4; i++) {
        try {
            parts.emplace_front(folly::to<std::string>(ip & 0x000000FF));
            ip >>= 8;
        } catch (const std::exception& ex) {
            LOG(FATAL) << "Something is wrong: " << ex.what();
        }
    }

    return folly::join(".", parts);
}


BENCHMARK_DRAW_LINE();

BENCHMARK(shared_intToIp, iters) {
    uint32_t ipInt = 0x44332211;
    for (uint32_t i = 0; i < iters; i++) {
        std::string ipStr = intToIp(ipInt);
        folly::doNotOptimizeAway(&ipStr);
    }
}
BENCHMARK_RELATIVE(intToIPv4, iters) {
    uint32_t ipInt = 0x44332211;
    for (uint32_t i = 0; i < iters; i++) {
        std::string ipStr = NetworkUtils::intToIPv4(ipInt);
        folly::doNotOptimizeAway(&ipStr);
    }
}

BENCHMARK_DRAW_LINE();


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);

    folly::runBenchmarks();
    return 0;
}


/*
 Tested on Intel Core i5-4300U (4 cores) with 8GB RAM
*/
