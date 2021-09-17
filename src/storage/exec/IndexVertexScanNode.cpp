/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/exec/IndexVertexScanNode.h"

namespace nebula {
namespace storage {

IndexVertexScanNode::IndexVertexScanNode(RuntimeContext* context,
                                         IndexID indexId,
                                         const std::vector<cpp2::IndexColumnHint>& clolumnHint)
    : IndexScanNode(context, indexId, clolumnHint) {}

}  // namespace storage
}  // namespace nebula
