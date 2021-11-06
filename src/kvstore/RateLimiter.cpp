/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/RateLimiter.h"

DEFINE_bool(skip_wait_in_rate_limiter,
            false,
            "skip the waiting of first second in rate limiter in CI");
