/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "kvstore/Common.h"

namespace nebula {
namespace kvstore {

TEST(KvStoreCommonTest, PeerTest) {
  Peer p;
  ASSERT_EQ(p, Peer::nullPeer());
}

TEST(KvStoreCommonTest, PeersTest) {
  // null peers
  Peers nullPeers;
  auto nullPeers1 = Peers::fromString(nullPeers.toString());
  ASSERT_EQ(nullPeers1.size(), 0);

  // one peer
  Peers onePeer;
  HostAddr addr1("1", 1);
  onePeer.addOrUpdate(Peer(addr1, Peer::Status::kPromotedPeer));
  auto onePeer1 = Peers::fromString(onePeer.toString());
  ASSERT_EQ(onePeer1.size(), 1);
  Peer p1;
  onePeer.get(addr1, &p1);
  ASSERT_EQ(p1.addr, addr1);

  // many peers
  std::vector<HostAddr> addrs;
  size_t peerCount = 10;
  for (size_t i = 1; i <= peerCount; ++i) {
    addrs.push_back(HostAddr(std::to_string(i), static_cast<int>(i)));
  }
  Peers manyPeers(addrs);

  HostAddr laddr(std::to_string(peerCount + 1), peerCount + 1);
  manyPeers.addOrUpdate(Peer(laddr, Peer::Status::kLearner));  // add one learner

  ASSERT_EQ(manyPeers.size(), peerCount + 1);
  Peers manyPeers1 = Peers::fromString(manyPeers.toString());
  ASSERT_EQ(manyPeers1.size(), peerCount + 1);
  for (size_t i = 1; i <= peerCount; ++i) {
    HostAddr addr(std::to_string(i), static_cast<int>(i));
    Peer p;
    manyPeers.get(addr, &p);
    ASSERT_EQ(p, Peer(addr, Peer::Status::kNormalPeer));
  }

  Peer p;
  manyPeers.get(laddr, &p);
  ASSERT_EQ(p, Peer(laddr, Peer::Status::kLearner));
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
