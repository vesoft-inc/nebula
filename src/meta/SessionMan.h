/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SESSIONMAN_H_
#define META_SESSIONMAN_H_

#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

// sessionId, std::pair<start_time, update_time>>
using sessionRec = std::unordered_map<int64_t, std::pair<int64_t, int64_t>>;

/**
 * This class manages global session for meta server.
 */
class SessionMan {
public:
    // Global session manager
    // Graphd addr -> map<sessionId, std::pair<start_time, update_time>>
    static std::unordered_map<std::string, sessionRec> globalSessions_;
};

}   // namespace meta
}   // namespace nebula

#endif   // META_SESSIONMAN_H_
