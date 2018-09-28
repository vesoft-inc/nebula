/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "network/NetworkUtils.h"
#include <netdb.h>
#include <arpa/inet.h>
#include "proc/ProcAccessor.h"

namespace vesoft {
namespace network {

static const int32_t kMaxHostNameLen = 256;

std::string NetworkUtils::getHostname() {
    char hn[kMaxHostNameLen];

    if (gethostname(hn, kMaxHostNameLen) < 0) {
        LOG(ERROR) << "gethostname error : " << strerror(errno);
        return "";
    }

    return std::string(hn);
}


std::vector<std::string> NetworkUtils::getLocalIPs(bool ipv6) {
    std::vector<std::string> ipList;

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = ipv6 ? AF_INET6 : AF_INET;
    hints.ai_socktype = SOCK_STREAM;    // TCP socket
    hints.ai_flags = AI_PASSIVE || AI_NUMERICHOST;

    std::string hostname = getHostname();
    if (hostname == "") {
        LOG(ERROR) << "getLocalIPs failed: Failed to get the hostname";
        return ipList;
    }

    struct addrinfo* servInfo;
    int status = getaddrinfo(hostname.data(), NULL, &hints, &servInfo);
    if (status != 0) {
        LOG(ERROR) << "getaddrinfo error: " << gai_strerror(status);
        return ipList;
    }

    int addrStrLen = ipv6 ? INET6_ADDRSTRLEN : INET_ADDRSTRLEN;
    char ipStr[addrStrLen];
    const char* pIpStr = ipStr;
    for (struct addrinfo* p = servInfo; p != NULL; p = p->ai_next) {
        struct in_addr *addr;
        if (p->ai_family == AF_INET) {
            struct sockaddr_in *ipv = (struct sockaddr_in*)p->ai_addr;
            addr = &(ipv->sin_addr);
        } else {
            struct sockaddr_in6 *ipv6_addr = (struct sockaddr_in6*)p->ai_addr;
            addr = (struct in_addr*) &(ipv6_addr->sin6_addr);
        }
        inet_ntop(p->ai_family, addr, ipStr, addrStrLen);
        if (strcmp(ipStr, "127.0.0.1") != 0) {
            ipList.emplace_back(pIpStr);
        }
    }

    freeaddrinfo(servInfo);
    return std::move(ipList);
}


bool NetworkUtils::getDynamicPortRange(uint16_t& low, uint16_t& high) {
    FILE* pipe = popen("cat /proc/sys/net/ipv4/ip_local_port_range", "r");
    if (!pipe) {
        LOG(ERROR) << "Failed to open /proc/sys/net/ipv4/ip_local_port_range: "
                   << strerror(errno);
        return false;
    }

    fscanf(pipe, "%hu %hu", &low, &high);

    if (pclose(pipe) < 0) {
        LOG(ERROR) << "Failed to close the pipe: " << strerror(errno);
        return false;
    }

    return true;
}


std::unordered_set<uint16_t> NetworkUtils::getPortsInUse() {
    static const std::regex regex("[^:]+:[^:]+:([0-9A-F]+).+");
    proc::ProcAccessor accessor("/proc/net/tcp");
    std::unordered_set<uint16_t> inUse;
    std::smatch sm;
    while (accessor.next(regex, sm)) {
        inUse.emplace(std::stoul(sm[1].str(), NULL, 16));
    }
    return std::move(inUse);
}


uint16_t NetworkUtils::getAvailablePort() {
    uint16_t low = 0;
    uint16_t high = 0;

    CHECK(getDynamicPortRange(low, high))
        << "Failed to get the dynamic port range";
    VLOG(1) << "Dynamic port range is [" << low << ", " << high << "]";

    std::unordered_set<uint16_t> portsInUse = getPortsInUse();
    uint16_t port = 0;
    do {
        port = folly::Random::rand32(low, static_cast<int32_t>(high) + 1);
    } while (portsInUse.find(port) != portsInUse.end());

    return port;
}


bool NetworkUtils::ipv4ToInt(const std::string& ipStr, uint32_t& ip) {
    std::vector<folly::StringPiece> parts;
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


std::string NetworkUtils::intToIPv4(uint32_t ip) {
    static const std::vector<std::string> kDict{
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
        "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
        "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36",
        "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48",
        "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
        "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72",
        "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84",
        "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96",
        "97", "98", "99", "100", "101", "102", "103", "104", "105", "106",
        "107", "108", "109", "110", "111", "112", "113", "114", "115", "116",
        "117", "118", "119", "120", "121", "122", "123", "124", "125", "126",
        "127", "128", "129", "130", "131", "132", "133", "134", "135", "136",
        "137", "138", "139", "140", "141", "142", "143", "144", "145", "146",
        "147", "148", "149", "150", "151", "152", "153", "154", "155", "156",
        "157", "158", "159", "160", "161", "162", "163", "164", "165", "166",
        "167", "168", "169", "170", "171", "172", "173", "174", "175", "176",
        "177", "178", "179", "180", "181", "182", "183", "184", "185", "186",
        "187", "188", "189", "190", "191", "192", "193", "194", "195", "196",
        "197", "198", "199", "200", "201", "202", "203", "204", "205", "206",
        "207", "208", "209", "210", "211", "212", "213", "214", "215", "216",
        "217", "218", "219", "220", "221", "222", "223", "224", "225", "226",
        "227", "228", "229", "230", "231", "232", "233", "234", "235", "236",
        "237", "238", "239", "240", "241", "242", "243", "244", "245", "246",
        "247", "248", "249", "250", "251", "252", "253", "254", "255"
    };

    auto& f1 = kDict[ip & 0x000000FF];
    auto& f2 = kDict[(ip >> 8) & 0x000000FF];
    auto& f3 = kDict[(ip >> 16) & 0x000000FF];
    auto& f4 = kDict[(ip >> 24) & 0x000000FF];

    char buf[16];
    char* pt = buf;
    strcpy(pt, f4.c_str());
    pt += f4.size();
    *pt++ = '.';
    strcpy(pt, f3.c_str());
    pt += f3.size();
    *pt++ = '.';
    strcpy(pt, f2.c_str());
    pt += f2.size();
    *pt++ = '.';
    strcpy(pt, f1.c_str());
    pt += f1.size();

    return buf;
}

}  // namespace network
}  // namespace vesoft

