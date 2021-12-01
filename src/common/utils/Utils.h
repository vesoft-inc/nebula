/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_UTILS_UTILS_H_
#define COMMON_UTILS_UTILS_H_

#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"

namespace nebula {
class Utils final {
 public:
  // Calculate the admin service address based on the storage service address
  static HostAddr getAdminAddrFromStoreAddr(HostAddr storeAddr) {
    if (storeAddr == HostAddr("", 0)) {
      return storeAddr;
    }
    return HostAddr(storeAddr.host, storeAddr.port - 1);
  }

  static HostAddr getStoreAddrFromAdminAddr(HostAddr adminAddr) {
    if (adminAddr == HostAddr("", 0)) {
      return adminAddr;
    }
    return HostAddr(adminAddr.host, adminAddr.port + 1);
  }

  // Calculate the raft service address based on the storage service address
  static HostAddr getRaftAddrFromStoreAddr(const HostAddr& srvcAddr) {
    if (srvcAddr == HostAddr("", 0)) {
      return srvcAddr;
    }
    return HostAddr(srvcAddr.host, srvcAddr.port + 1);
  }

  static HostAddr getStoreAddrFromRaftAddr(const HostAddr& raftAddr) {
    if (raftAddr == HostAddr("", 0)) {
      return raftAddr;
    }
    return HostAddr(raftAddr.host, raftAddr.port - 1);
  }

  static HostAddr getInternalAddrFromStoreAddr(HostAddr adminAddr) {
    if (adminAddr == HostAddr("", 0)) {
      return adminAddr;
    }
    return HostAddr(adminAddr.host, adminAddr.port - 2);
  }

  static std::string getMacAddr() {
    char line[500];       // Read with fgets().
    char ipAddress[100];  // Obviously more space than necessary, just illustrating here.
    int hwType;
    int flags;
    char macAddress[100];
    char mask[100];
    char device[100];

    FILE* fp = fopen("/proc/net/arp", "r");
    fgets(line, sizeof(line), fp);  // Skip the first line (column headers).
    while (fgets(line, sizeof(line), fp)) {
      // Read the data.
      sscanf(line, "%s 0x%x 0x%x %s %s %s\n", ipAddress, &hwType, &flags, macAddress, mask, device);

      // Do stuff with it.
    }

    fclose(fp);
  }
};
}  // namespace nebula
#endif  // COMMON_UTILS_UTILS_H_
