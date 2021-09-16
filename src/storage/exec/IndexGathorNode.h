/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#pragma once
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {
class IndexGathorNode : public IterateNode<nullptr_t> {
 public:
  IndexGathorNode();

 private:
};
}  // namespace storage

}  // namespace nebula
