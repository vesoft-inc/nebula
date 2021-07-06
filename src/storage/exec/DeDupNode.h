
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_DEDUPNODE_H_
#define STORAGE_EXEC_DEDUPNODE_H_

#include "common/base/Base.h"
#include "storage/exec/FilterNode.h"

namespace nebula {
namespace storage {

// DedupNode will used dedup the result set based on the given fields
template<typename T>
class DeDupNode : public IterateNode<T> {
public:
    using RelNode<T>::execute;

    explicit DeDupNode(nebula::DataSet* resultSet, const std::vector<size_t>& pos)
        : resultSet_(resultSet)
        , pos_(pos) {}

    nebula::cpp2::ErrorCode execute(PartitionID partId) override {
        auto ret = RelNode<T>::execute(partId);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
        dedup(resultSet_->rows, pos_);
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    static void dedup(std::vector<Row>& rows, const std::vector<size_t>& pos) {
        std::sort(rows.begin(), rows.end(), [&pos](auto& l, auto& r) {
            for (const auto& p : pos) {
                if (l.values[p] != r.values[p]) {
                    return l.values[p] < r.values[p];
                }
            }
            return false;
        });
        rows.erase(std::unique(rows.begin(), rows.end(), [&pos](auto& l, auto& r) {
            for (const auto& p : pos) {
                if (l.values[p] != r.values[p]) {
                    return false;
                }
            }
            return true;
        }), rows.end());
    }

private:
    nebula::DataSet*    resultSet_;
    std::vector<size_t> pos_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_DEDUPNODE_H_
