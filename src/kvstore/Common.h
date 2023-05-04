/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_COMMON_H_
#define KVSTORE_COMMON_H_

#include <folly/Function.h>
#include <rocksdb/slice.h>

#include <sstream>

#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"
#include "common/thrift/ThriftTypes.h"
#include "common/time/WallClock.h"
#include "common/utils/Types.h"
#include "interface/gen-cpp2/common_types.h"

DECLARE_int64(balance_expired_sesc);

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
   * @param level
   * @param spaceId
   * @param key
   * @param val
   * @return true Key will not be removed
   * @return false Key will be removed
   */
  virtual bool filter(int level,
                      GraphSpaceID spaceId,
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
  explicit Peer(HostAddr a) : addr(a), status(Status::kNormalPeer) {}
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
  int64_t createdTime;

 public:
  Peers() {
    createdTime = time::WallClock::fastNowInSec();
  }
  explicit Peers(const std::vector<HostAddr>& addrs) {  // from normal peers
    for (auto& addr : addrs) {
      peers[addr] = Peer(addr, Peer::Status::kNormalPeer);
    }
    createdTime = time::WallClock::fastNowInSec();
  }
  explicit Peers(const std::vector<Peer>& ps) {
    for (auto& p : ps) {
      peers[p.addr] = p;
    }
    createdTime = time::WallClock::fastNowInSec();
  }
  explicit Peers(std::map<HostAddr, Peer> ps) : peers(std::move(ps)) {
    createdTime = time::WallClock::fastNowInSec();
  }

  void addOrUpdate(const Peer& peer) {
    peers[peer.addr] = peer;
  }

  bool exist(const HostAddr& addr) const {
    return peers.find(addr) != peers.end();
  }

  bool get(const HostAddr& addr, Peer* peer) const {
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

  size_t size() const {
    return peers.size();
  }

  std::map<HostAddr, Peer> getPeers() {
    return peers;
  }

  bool allNormalPeers() const {
    for (const auto& [addr, peer] : peers) {
      if (peer.status == Peer::Status::kDeleted) {
        continue;
      }

      if (peer.status != Peer::Status::kNormalPeer) {
        return false;
      }
    }
    return true;
  }

  bool isExpired() const {
    return time::WallClock::fastNowInSec() - createdTime > FLAGS_balance_expired_sesc;
  }

  void setCreatedTime(int64_t time) {
    createdTime = time;
  }

  std::string toString() const {
    std::stringstream os;
    os << "version:1,"
       << "count:" << peers.size() << ",ts:" << createdTime << "\n";
    for (const auto& [_, p] : peers) {
      os << p << "\n";
    }
    return os.str();
  }

  static std::tuple<int, int, int64_t> extractHeader(const std::string& header) {
    std::vector<std::string> fields;
    folly::split(":", header, fields, true);
    if (fields.size() != 4) {
      LOG(INFO) << "Parse part peers header error:" << header;
      return {0, 0, 0};
    }

    int version = std::stoi(fields[1]);
    int count = std::stoi(fields[2]);

    int64_t createdTime;
    std::istringstream iss(fields[3]);
    iss >> createdTime;

    return {version, count, createdTime};
  }

  static Peers fromString(const std::string& str) {
    Peers peers;
    std::vector<std::string> lines;
    folly::split("\n", str, lines, true);

    if (lines.size() < 1) {
      LOG(INFO) << "Bad format peers data, empty data";
      return peers;
    }

    auto [version, count, createdTime] = extractHeader(lines[0]);
    if (version != 1) {
      LOG(INFO) << "Wrong peers format version:" << version;
      return peers;
    }

    if (count + 1 != static_cast<int>(lines.size())) {
      LOG(INFO) << "Header count: " << count
                << " does not match real count:" << static_cast<int>(lines.size()) - 1;
      return peers;
    }

    peers.setCreatedTime(createdTime);

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

class MergeableAtomicOpResult {
 public:
  nebula::cpp2::ErrorCode code;
  std::string batch;  // batched result, like before.
  std::list<std::string> readSet;
  std::list<std::string> writeSet;
};

using MergeableAtomicOp = folly::Function<MergeableAtomicOpResult(void)>;

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_COMMON_H_
