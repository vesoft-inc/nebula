/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_NETWORK_NETWORKUTILS_H_
#define COMMON_NETWORK_NETWORKUTILS_H_

#include "base/Base.h"

namespace nebula {
namespace network {

class NetworkUtils final {
public:
    NetworkUtils() = delete;

    static std::string getHostname();

    static std::vector<std::string> getLocalIPs(bool ipv6 = false);

    // Get the local dynamic port range [low, high], only works for IPv4
    static bool getDynamicPortRange(uint16_t& low, uint16_t& high);
    // Get all ports that are currently in use
    static std::unordered_set<uint16_t> getPortsInUse();
    // Get a dynamic port that is not in use
    static uint16_t getAvailablePort();

    // Convert an IPv4 address (in the form of xx.xx.xx.xx) to an 32-bit
    // integer
    // Return true if succeeded.
    // Return false if the given address is invalid
    static bool ipv4ToInt(const std::string& ipStr, uint32_t& ip);
    // Convert the given 32-bit integer to an IPv4 address string
    // (in the form of xx.xx.xx.xx)
    static std::string intToIPv4(uint32_t ip);

private:
};

}  // namespace network
}  // namespace nebula

#endif  // COMMON_NETWORK_NETWORKUTILS_H_

