/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include <gtest/gtest.h>
#include "signal/SignalUtils.h"

void exit_signal_handler(int signal_num);

namespace nebula {
namespace signal {

TEST(SignalUtils, signalTest) {
    SYSTEM_SIGNAL_SETUP("signalTest");
    ::raise(SIGTERM);
}
}   // namespace signal
}   // namespace nebula

void exit_signal_handler(int signal_num) {
    ASSERT_EQ(signal_num, SIGTERM);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

