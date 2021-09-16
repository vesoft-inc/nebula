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
// nebula::cpp2::ErrorCode IndexVertexScanNode::execute(PartitionID partId, const nullptr_t&) {
//   std::unique_ptr<kvstore::KVIterator> iter;
//   if (constrain_->isRange()) {
//     auto start_key = constraint_->getStartKey();
//     auto end_key = constrain_->getEndkey();
//     iter.reset(context_->env()->kvstore_->range());
//   } else {
//     auto prefix = constrain_->getPrefix();
//     iter.reset(context_->env()->kvstore_->prefix());
//   }
//   return nebula::cpp2::ErrorCode::SUCCEEDED;
// }
// bool IndexVertexScanNode::next(Row& row){

// };

}  // namespace storage
}  // namespace nebula
