/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/network/NetworkUtils.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"

namespace nebula {
namespace network {

static const int32_t kMaxHostNameLen = 256;

// TODO(liuyu) this only works in ipv4
std::string NetworkUtils::getHostname() {
  char hn[kMaxHostNameLen];

  if (gethostname(hn, kMaxHostNameLen) < 0) {
    LOG(ERROR) << "gethostname error : " << strerror(errno);
    return "";
  }

  return std::string(hn);
}

StatusOr<std::string> NetworkUtils::getIPFromDevice(const std::string& device) {
  if (device == "any") {
    return "0.0.0.0";
  }
  auto result = listDeviceAndIPs();
  if (!result.ok()) {
    return std::move(result).status();
  }
  auto iter = std::find_if(result.value().begin(),
                           result.value().end(),
                           [&](const auto& deviceAndIp) { return deviceAndIp.first == device; });
  if (iter == result.value().end()) {
    return Status::Error("No IP address found for `%s'", device.c_str());
  }
  return iter->second;
}

StatusOr<std::vector<std::string>> NetworkUtils::listIPs() {
  auto result = listDeviceAndIPs();
  if (!result.ok()) {
    return std::move(result).status();
  }
  auto getval = [](const auto& entry) { return entry.second; };
  std::vector<std::string> ips;
  ips.resize(result.value().size());
  std::transform(result.value().begin(), result.value().end(), ips.begin(), getval);
  return ips;
}

StatusOr<std::vector<std::pair<std::string, std::string>>> NetworkUtils::listDeviceAndIPs() {
  struct ifaddrs* iflist;
  std::vector<std::pair<std::string, std::string>> dev2ips;
  if (::getifaddrs(&iflist) != 0) {
    return Status::Error("%s", ::strerror(errno));
  }
  for (auto* ifa = iflist; ifa != nullptr; ifa = ifa->ifa_next) {
    char ip[INET6_ADDRSTRLEN];
    if (ifa->ifa_addr->sa_family == AF_INET) {
      auto* addr = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
      ::inet_ntop(AF_INET, &(addr->sin_addr), ip, INET_ADDRSTRLEN);
    } else if (ifa->ifa_addr->sa_family == AF_INET6) {
      auto* addr = reinterpret_cast<struct sockaddr_in6*>(ifa->ifa_addr);
      ::inet_ntop(AF_INET6, &(addr->sin6_addr), ip, INET6_ADDRSTRLEN);
    } else {
      continue;
    }
    dev2ips.emplace_back(ifa->ifa_name, ip);
  }
  ::freeifaddrs(iflist);
  if (dev2ips.empty()) {
    return Status::Error("No IP devices found");
  }
  return dev2ips;
}

bool NetworkUtils::getDynamicPortRange(uint16_t& low, uint16_t& high) {
  // Note: this function is capable of getting the dynamic port range even for IPv6
  FILE* pipe = popen("cat /proc/sys/net/ipv4/ip_local_port_range", "r");
  if (!pipe) {
    LOG(ERROR) << "Failed to open /proc/sys/net/ipv4/ip_local_port_range: " << strerror(errno);
    return false;
  }

  if (fscanf(pipe, "%hu %hu", &low, &high) != 2) {
    LOG(ERROR) << "Failed to read from /proc/sys/net/ipv4/ip_local_port_range";
    // According to ICANN, the port range is divided into three sections
    //
    // Well-known ports: 0 to 1023 (used for system services)
    // Registered/user ports: 1024 to 49151
    // Dynamic/private ports: 49152 to 65535
    low = 49152;
    high = 65535;
  }

  if (pclose(pipe) < 0) {
    LOG(ERROR) << "Failed to close the pipe: " << strerror(errno);
    return false;
  }

  return true;
}

std::unordered_set<uint16_t> NetworkUtils::getPortsInUse() {
  static const std::regex regex("[^:]+:[^:]+:([0-9A-F]+).+");
  std::unordered_set<uint16_t> inUse;
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/tcp", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/tcp6", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/udp", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/udp6", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/raw", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }
  {
    fs::FileUtils::FileLineIterator iter("/proc/net/raw6", &regex);
    while (iter.valid()) {
      auto& sm = iter.matched();
      inUse.emplace(std::stoul(sm[1].str(), nullptr, 16));
      ++iter;
    }
  }

  return inUse;
}

uint16_t NetworkUtils::getAvailablePort() {
  uint16_t low = 0;
  uint16_t high = 0;

  CHECK(getDynamicPortRange(low, high)) << "Failed to get the dynamic port range";
  VLOG(1) << "Dynamic port range is [" << low << ", " << high << "]";

  std::unordered_set<uint16_t> portsInUse = getPortsInUse();
  uint16_t port = 0;
  while (true) {
    // NOTE
    // The availability of port number *outside* the ephemeral port range is
    // relatively stable for the binding purpose.
    port = folly::Random::rand32(1025, low);
    if (portsInUse.find(port) != portsInUse.end()) {
      continue;
    }
    if (portsInUse.find(port + 1) == portsInUse.end()) {
      break;
    }
  }

  return port;
}

StatusOr<std::vector<HostAddr>> NetworkUtils::resolveHost(const std::string& host, int32_t port) {
  std::vector<HostAddr> addrs;
  struct addrinfo hints, *res, *rp;
  ::memset(&hints, 0, sizeof(struct addrinfo));

  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_ADDRCONFIG;

  if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0) {
    return Status::Error("host not found:%s", host.c_str());
  }

  for (rp = res; rp != nullptr; rp = rp->ai_next) {
    char ip[INET6_ADDRSTRLEN];
    switch (rp->ai_family) {
      case AF_INET: {
        auto address = (reinterpret_cast<struct sockaddr_in*>(rp->ai_addr))->sin_addr.s_addr;
        ::inet_ntop(AF_INET, &address, ip, INET_ADDRSTRLEN);
        break;
      }
      case AF_INET6: {
        auto address = (reinterpret_cast<struct sockaddr_in6*>(rp->ai_addr))->sin6_addr;
        ::inet_ntop(AF_INET6, &address, ip, INET6_ADDRSTRLEN);
        break;
      }
      default:
        continue;
    }
    addrs.emplace_back(ip, port);
  }

  freeaddrinfo(res);

  if (addrs.empty()) {
    return Status::Error("host not found: %s", host.c_str());
  }

  return addrs;
}

std::string NetworkUtils::intToIPv4(uint32_t ip) {
  static const std::vector<std::string> kDict{
      "0",   "1",   "2",   "3",   "4",   "5",   "6",   "7",   "8",   "9",   "10",  "11",  "12",
      "13",  "14",  "15",  "16",  "17",  "18",  "19",  "20",  "21",  "22",  "23",  "24",  "25",
      "26",  "27",  "28",  "29",  "30",  "31",  "32",  "33",  "34",  "35",  "36",  "37",  "38",
      "39",  "40",  "41",  "42",  "43",  "44",  "45",  "46",  "47",  "48",  "49",  "50",  "51",
      "52",  "53",  "54",  "55",  "56",  "57",  "58",  "59",  "60",  "61",  "62",  "63",  "64",
      "65",  "66",  "67",  "68",  "69",  "70",  "71",  "72",  "73",  "74",  "75",  "76",  "77",
      "78",  "79",  "80",  "81",  "82",  "83",  "84",  "85",  "86",  "87",  "88",  "89",  "90",
      "91",  "92",  "93",  "94",  "95",  "96",  "97",  "98",  "99",  "100", "101", "102", "103",
      "104", "105", "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116",
      "117", "118", "119", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129",
      "130", "131", "132", "133", "134", "135", "136", "137", "138", "139", "140", "141", "142",
      "143", "144", "145", "146", "147", "148", "149", "150", "151", "152", "153", "154", "155",
      "156", "157", "158", "159", "160", "161", "162", "163", "164", "165", "166", "167", "168",
      "169", "170", "171", "172", "173", "174", "175", "176", "177", "178", "179", "180", "181",
      "182", "183", "184", "185", "186", "187", "188", "189", "190", "191", "192", "193", "194",
      "195", "196", "197", "198", "199", "200", "201", "202", "203", "204", "205", "206", "207",
      "208", "209", "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "220",
      "221", "222", "223", "224", "225", "226", "227", "228", "229", "230", "231", "232", "233",
      "234", "235", "236", "237", "238", "239", "240", "241", "242", "243", "244", "245", "246",
      "247", "248", "249", "250", "251", "252", "253", "254", "255"};

  auto& f1 = kDict[ip & 0x000000FF];
  auto& f2 = kDict[(ip >> 8) & 0x000000FF];
  auto& f3 = kDict[(ip >> 16) & 0x000000FF];
  auto& f4 = kDict[(ip >> 24) & 0x000000FF];

  char buf[16];
  char* pt = buf;
  strcpy(pt, f4.c_str());  // NOLINT
  pt += f4.size();
  *pt++ = '.';
  strcpy(pt, f3.c_str());  // NOLINT
  pt += f3.size();
  *pt++ = '.';
  strcpy(pt, f2.c_str());  // NOLINT
  pt += f2.size();
  *pt++ = '.';
  strcpy(pt, f1.c_str());  // NOLINT
  pt += f1.size();

  return buf;
}

StatusOr<std::vector<HostAddr>> NetworkUtils::toHosts(const std::string& peersStr) {
  std::vector<HostAddr> hosts;
  std::vector<std::string> peers;
  folly::split(",", peersStr, peers, true);
  hosts.reserve(peers.size());
  for (auto& peerStr : peers) {
    auto addrPort = folly::trimWhitespace(peerStr);
    std::string addr;
    int32_t port;

    // Handle strings that may contain IPv6 addresses
    auto lastColonPos = addrPort.rfind(':');
    if (lastColonPos == folly::StringPiece::npos) {
      return Status::Error("Bad peer format: %s", addrPort.start());
    }

    try {
      port = folly::to<int32_t>(addrPort.subpiece(lastColonPos + 1));
    } catch (const std::exception& ex) {
      return Status::Error("Bad port number, error: %s", ex.what());
    }

    addr = addrPort.subpiece(0, lastColonPos).toString();
    // remove the square brackets if present in IPv6 addresses
    if (!addr.empty() && addr.front() == '[' && addr.back() == ']') {
      addr = addr.substr(1, addr.size() - 2);
    }

    hosts.emplace_back(std::move(addr), port);
  }
  return hosts;
}

std::string NetworkUtils::toHostsStr(const std::vector<HostAddr>& hosts) {
  std::string hostsString;
  for (auto& host : hosts) {
    if (!hostsString.empty()) {
      hostsString.append(", ");
    }
    // Whether the host is IPv6 address
    if (host.host.find(':') != std::string::npos) {
      // if IPv6, add square brackets
      hostsString += folly::stringPrintf("[%s]:%d", host.host.c_str(), host.port);
    } else {
      // IPv4 or hostname
      hostsString += folly::stringPrintf("%s:%d", host.host.c_str(), host.port);
    }
  }
  return hostsString;
}

Status NetworkUtils::validateHostOrIp(const std::string& hostOrIp) {
  if (hostOrIp.empty()) {
    return Status::Error("local_ip is empty, need to config it through config file.");
  }
  const std::regex ipv4(
      "((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}"
      "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)");
  const std::regex ipv6(
      "([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]{1,4}|:)|"
      "(([0-9a-fA-F]{1,4}:){1,6}:)|"
      "(:(([0-9a-fA-F]{1,4}:){1,6}))");
  if (std::regex_match(hostOrIp, ipv4) || std::regex_match(hostOrIp, ipv6)) {
    const std::regex loopbackOrAny(
        "^127(?:\\.[0-9]+){0,2}\\.[0-9]+$|^(?:0*\\:)*?:?0*1$|"
        "([0:]+1)|((0\\.){3}0)");
    if (std::regex_match(hostOrIp, loopbackOrAny)) {
      return Status::OK();
    }
    auto ipsStatus = listIPs();
    NG_RETURN_IF_ERROR(ipsStatus);
    const auto& ips = ipsStatus.value();
    auto result = std::find(ips.begin(), ips.end(), hostOrIp);
    if (result == ips.end()) {
      return Status::Error("%s is not a valid ip in current host, candidates: %s",
                           hostOrIp.c_str(),
                           folly::join(",", ips).c_str());
    }
  } else {
    NG_RETURN_IF_ERROR(resolveHost(hostOrIp, 0));
  }
  return Status::OK();
}
}  // namespace network
}  // namespace nebula
