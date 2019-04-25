/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {

TEST(ActiveHostsManTest, NormalTest) {
    auto now = time::TimeUtils::nowInSeconds();
    ActiveHostsMan ahMan(1, 1);
    ahMan.updateHostInfo(HostAddr(0, 0), HostInfo(now));
    ahMan.updateHostInfo(HostAddr(0, 1), HostInfo(now));
    ahMan.updateHostInfo(HostAddr(0, 2), HostInfo(now));

    ASSERT_EQ(3, ahMan.getActiveHosts().size());
    ahMan.updateHostInfo(HostAddr(0, 0), HostInfo(now + 2));
    ASSERT_EQ(3, ahMan.getActiveHosts().size());
    ASSERT_EQ(HostInfo(now + 2),  ahMan.hostsMap_[HostAddr(0, 0)]);

    sleep(3);
    ASSERT_EQ(1, ahMan.getActiveHosts().size());
    ASSERT_EQ(HostInfo(now + 2),  ahMan.hostsMap_[HostAddr(0, 0)]);
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


