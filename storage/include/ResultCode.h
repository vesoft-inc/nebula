/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_RESULTCODE_H_
#define STORAGE_RESULTCODE_H_

#include "base/Base.h"

namespace vesoft {
namespace vgraph {
namespace storage {

using KV = std::pair<std::string, std::string>;

enum ResultCode {
    SUCCESSED           =   0,
    ERR_UNKNOWN         =  -1,
    ERR_PART_NOT_FOUND =  -2,
    ERR_KEY_NOT_FOUND   =  -3,
    ERR_SPACE_NOT_FOUND =  -4,
};

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft
#endif  // STORAGE_RESULTCODE_H_

