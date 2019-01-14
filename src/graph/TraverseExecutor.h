/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_TRAVERSEEXECUTOR_H_
#define GRAPH_TRAVERSEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"

namespace nebula {
namespace graph {

class ResultSchema final {
public:
    struct Column {
        std::string                             name_;
        // TODO(dutor) type
    };

    void addColumn(std::string name) {
        columns_.emplace_back();
        columns_.back().name_ = std::move(name);
    }

    std::string toString() const {
        std::string buf;
        buf.reserve(256);
        for (auto &column : columns_) {
            if (column.name_.empty()) {
                buf += "NULL";
            } else {
                buf += column.name_;
            }
            buf += "\t";
        }
        if (!buf.empty()) {
            buf.resize(buf.size() - 1);
        }
        return buf;
    }

private:
    std::vector<Column>                         columns_;
};


class TraverseExecutor : public Executor {
public:
    explicit TraverseExecutor(ExecutionContext *ectx) : Executor(ectx) {}

    using TraverseRecord = std::vector<VariantType>;
    using TraverseRecords = std::vector<TraverseRecord>;
    using OnResult = std::function<void(TraverseRecords)>;

    virtual void feedResult(TraverseRecords records) = 0;

    /**
     * `onResult_' must be set except for the right most executor
     * inside the chain of pipeline.
     *
     * For any TraverseExecutor, if `onResult_' is set, it means that
     * some other executor depends on its execution result. Otherwise,
     * it means that this executor is the right most one, whose results must
     * be cached during its execution and are to be used to fill `ExecutionResponse'
     * upon `setupResponse()'s invoke.
     */
    void setOnResult(OnResult onResult) {
        onResult_ = std::move(onResult);
    }

    virtual ResultSchema* resultSchema() const {
        return resultSchema_.get();
    }

    virtual void setInputResultSchema(ResultSchema *schema) {
        inputResultSchema_ = schema;
    }

protected:
    std::unique_ptr<TraverseExecutor> makeTraverseExecutor(Sentence *sentence);

protected:
    OnResult                                    onResult_;
    std::unique_ptr<ResultSchema>               resultSchema_;
    ResultSchema                               *inputResultSchema_{nullptr};
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_TRAVERSEEXECUTOR_H_
