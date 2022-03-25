/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_COMMON_H_
#define KVSTORE_COMMON_H_

#include <folly/Function.h>
#include <rocksdb/slice.h>

#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"
#include "common/thrift/ThriftTypes.h"
#include "common/utils/Types.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace kvstore {

/**
 * @brief Wrapper of rocksdb compaction filter function
 */
class KVFilter {
 public:
  KVFilter() = default;
  virtual ~KVFilter() = default;

  /**
   * @brief Whether remove the key during compaction
   *
   * @param spaceId
   * @param key
   * @param val
   * @return true Key will not be removed
   * @return false Key will be removed
   */
  virtual bool filter(GraphSpaceID spaceId,
                      const folly::StringPiece& key,
                      const folly::StringPiece& val) const = 0;
};

/**
 * @brief Part peer, including normal peer and learner peer.
 *
 */
struct Peer {
  // Mainly used in balancing to keep the status which is not reflected in the meta service.
  // Then when restarting, we could restore the balancing progress.
  enum class Status : uint8_t {
    kNormalPeer = 0,    // normal raft peer, kept in the meta part info
    kLearner = 1,       // learner peer for catching up data, and not kept in the meta
    kPromotedPeer = 2,  // promoted from learner to normal peer, but not kept in the meta
    kDeleted = 3,       // indicate this peer should be removed, will not exist in the persist data
    kMax = 4,           // a special value, used as a delimiter when deserializing.
  };

  HostAddr addr;
  Status status;

  Peer() : addr(), status(Status::kNormalPeer) {}
  Peer(HostAddr a, Status s) : addr(a), status(s) {}

  std::string toString() const {
    return addr.toString() + "," + std::to_string(static_cast<int>(status));
  }

  void fromString(const std::string& str) {
    auto pos = str.find(",");
    if (pos == std::string::npos) {
      LOG(INFO) << "Parse peer failed:" << str;
      return;
    }
    addr = HostAddr::fromString(str.substr(0, pos));

    int s = std::stoi(str.substr(pos + 1));
    if (s >= static_cast<int>(Status::kMax) || s < 0) {
      LOG(INFO) << "Parse peer status failed:" << str;
      return;
    }

    status = static_cast<Status>(s);
  }

  bool operator==(const Peer& rhs) const {
    return addr == rhs.addr && status == rhs.status;
  }

  bool operator!=(const Peer& rhs) const {
    return !(*this == rhs);
  }

  static Peer nullPeer() {
    return Peer();
  }
};

inline std::ostream& operator<<(std::ostream& os, const Peer& peer) {
  return os << peer.toString();
}

/**
 * @brief Peers for one partition, it should handle serializing and deserializing.
 *
 */
struct Peers {
 private:
  std::map<HostAddr, Peer> peers;

 public:
  Peers() {}
  explicit Peers(const std::vector<HostAddr>& addrs) {  // from normal peers
    for (auto& addr : addrs) {
      peers[addr] = Peer(addr, Peer::Status::kNormalPeer);
    }
  }
  explicit Peers(const std::vector<Peer>& ps) {
    for (auto& p : ps) {
      peers[p.addr] = p;
    }
  }
  explicit Peers(std::map<HostAddr, Peer> ps) : peers(std::move(ps)) {}

  void addOrUpdate(const Peer& peer) {
    peers[peer.addr] = peer;
  }

  bool get(const HostAddr& addr, Peer* peer) {
    auto it = peers.find(addr);
    if (it == peers.end()) {
      return false;
    }

    if (peer != nullptr) {
      *peer = it->second;
    }
    return true;
  }

  void remove(const HostAddr& addr) {
    peers.erase(addr);
  }

  size_t size() {
    return peers.size();
  }

  std::map<HostAddr, Peer> getPeers() {
    return peers;
  }

  std::string toString() const {
    std::stringstream os;
    os << "version:1,"
       << "count:" << peers.size() << "\n";
    for (const auto& [_, p] : peers) {
      os << p << "\n";
    }
    return os.str();
  }

  static std::pair<int, int> extractHeader(const std::string& header) {
    auto pos = header.find(":");
    if (pos == std::string::npos) {
      LOG(INFO) << "Parse part peers header error:" << header;
      return {0, 0};
    }
    int version = std::stoi(header.substr(pos + 1));
    pos = header.find(":", pos + 1);
    if (pos == std::string::npos) {
      LOG(INFO) << "Parse part peers header error:" << header;
      return {0, 0};
    }
    int count = std::stoi(header.substr(pos + 1));

    return {version, count};
  }

  static Peers fromString(const std::string& str) {
    Peers peers;
    std::vector<std::string> lines;
    folly::split("\n", str, lines, true);

    if (lines.size() < 1) {
      LOG(INFO) << "Bad format peers data, emtpy data";
      return peers;
    }

    auto [version, count] = extractHeader(lines[0]);
    if (version != 1) {
      LOG(INFO) << "Wrong peers format version:" << version;
      return peers;
    }

    if (count + 1 != static_cast<int>(lines.size())) {
      LOG(INFO) << "Header count: " << count
                << " does not match real count:" << static_cast<int>(lines.size()) - 1;
      return peers;
    }

    // skip header
    for (size_t i = 1; i < lines.size(); ++i) {
      auto& line = lines[i];

      Peer peer;
      peer.fromString(line);
      peers.addOrUpdate(peer);
    }

    return peers;
  }
};  // namespace kvstore

inline std::ostream& operator<<(std::ostream& os, const Peers& peers) {
  return os << peers.toString();
}

using KV = std::pair<std::string, std::string>;
using KVCallback = folly::Function<void(nebula::cpp2::ErrorCode code)>;
using NewLeaderCallback = folly::Function<void(HostAddr nLeader)>;

/**
 * @brief folly::StringPiece to rocksdb::Slice
 */
inline rocksdb::Slice toSlice(const folly::StringPiece& str) {
  return rocksdb::Slice(str.begin(), str.size());
}

using KVMap = std::unordered_map<std::string, std::string>;
using KVArrayIterator = std::vector<KV>::const_iterator;

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_COMMON_H_
