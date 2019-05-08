/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */


#include "meta/processors/HBProcessor.h"

DEFINE_int32(expired_hosts_check_interval_sec, 20,
             "Check the expired hosts at the interval");
DEFINE_int32(expired_threshold_sec, 10 * 60,
             "Hosts will be expired in this time if no heartbeat received");

namespace nebula {
namespace meta {


}  // namespace meta
}  // namespace nebula

