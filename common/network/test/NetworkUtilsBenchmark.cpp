/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "network/NetworkUtils.h"

using namespace vesoft::network;

bool ipToInt(const std::string& ipStr, uint32_t& ip) {
    std::vector<std::string> parts;
    folly::split(".", ipStr, parts, true);
    if (parts.size() != 4) {
        return false;
    }

    ip = 0;
    for (auto& s : parts) {
        ip <<= 8;
        try {
            ip |= folly::to<uint8_t>(s);
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Invalid ip string: \"" << ipStr << "\"";
            return false;
        }
    }

    return true;
}


std::string intToIp(uint32_t ip) {
    std::deque<std::string> parts;
    for (int i = 0; i < 4; i++) {
        try {
            parts.emplace_front(std::move(folly::to<std::string>(ip & 0x000000FF)));
            ip >>= 8;
        } catch (const std::exception& ex) {
            LOG(FATAL) << "Something is wrong: " << ex.what();
        }
    }

    return folly::join(".", parts);
}


BENCHMARK_DRAW_LINE();

BENCHMARK(shared_ipToInt, iters) {
    std::string ipStr("10.20.30.40");
    uint32_t ipInt;
    for (uint32_t i = 0; i < iters; i++) {
        ipToInt(ipStr, ipInt);
        folly::doNotOptimizeAway(ipInt);
    }
}
BENCHMARK_RELATIVE(ipv4ToInt, iters) {
    std::string ipStr("10.20.30.40");
    uint32_t ipInt;
    for (uint32_t i = 0; i < iters; i++) {
        NetworkUtils::ipv4ToInt(ipStr, ipInt);
        folly::doNotOptimizeAway(ipInt);
    }
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
