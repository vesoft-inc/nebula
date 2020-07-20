/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_DATAJOINEXECUTOR_H_
#define EXEC_QUERY_DATAJOINEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class DataJoinExecutor final : public Executor {
public:
    class HashTable final {
    public:
        using Table = std::vector<std::multimap<List, const LogicalRow*>>;

        explicit HashTable(size_t bucketSize) : bucketSize_(bucketSize) {
            table_ =
                std::vector<std::multimap<List, const LogicalRow*>>(bucketSize);
        }

        void add(List key, const LogicalRow* row) {
            auto hash = std::hash<List>()(key);
            auto bucket = hash % bucketSize_;
            table_[bucket].emplace(std::move(key), row);
        }

        auto get(List& key) const {
            auto hash = std::hash<List>()(key);
            auto bucket = hash % bucketSize_;
            return table_[bucket].equal_range(key);
        }

        void clear() {
            bucketSize_ = 0;
            table_.clear();
        }

    private:
        size_t  bucketSize_{0};
        Table   table_;
    };

    DataJoinExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("DataJoinExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    folly::Future<Status> doInnerJoin();

    void buildHashTable(const std::vector<Expression*>& hashKeys, Iterator* iter);

    void probe(const std::vector<Expression*>& probeKeys, Iterator* probeiter,
               JoinIter* resultIter);

private:
    bool                         exchange_{false};
    std::unique_ptr<HashTable>   hashTable_;
};
}  // namespace graph
}  // namespace nebula
#endif
