/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_TRAVERSEEXECUTOR_H_
#define GRAPH_TRAVERSEEXECUTOR_H_

#include "base/Base.h"
#include "graph/Executor.h"
#include "graph/InterimResult.h"
#include "meta/SchemaProviderIf.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

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


class Collector final {
public:
    static Status collect(VariantType &var, RowWriter *writer);

    static OptVariantType getProp(const meta::SchemaProviderIf *schema,
                                  const std::string &prop,
                                  const RowReader *reader);

    static Status getSchema(const std::vector<VariantType> &vals,
                            const std::vector<std::string> &colNames,
                            const std::vector<nebula::cpp2::SupportedType> &colTypes,
                            SchemaWriter *outputSchema);
};

class YieldClauseWrapper final {
public:
    explicit YieldClauseWrapper(const YieldClause *clause) {
        clause_ = clause;
    }

    Status prepare(const InterimResult *inputs,
                   const VariableHolder *varHolder,
                   std::vector<YieldColumn*> &yields);

private:
    bool needAllPropsFromInput(const YieldColumn *col,
                               const InterimResult *inputs,
                               std::vector<YieldColumn*> &yields);

    StatusOr<bool> needAllPropsFromVar(const YieldColumn *col,
                                       const VariableHolder *varHolder,
                                       std::vector<YieldColumn*> &yields);

private:
    const YieldClause              *clause_;
    std::unique_ptr<YieldColumns>   yieldColsHolder_;
};

class TraverseExecutor : public Executor {
public:
    explicit TraverseExecutor(ExecutionContext *ectx) : Executor(ectx) {}

    using OnResult = std::function<void(std::unique_ptr<InterimResult>)>;

    virtual void feedResult(std::unique_ptr<InterimResult> result) = 0;

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

    static std::unique_ptr<TraverseExecutor>
    makeTraverseExecutor(Sentence *sentence, ExecutionContext *ectx);

protected:
    std::unique_ptr<TraverseExecutor> makeTraverseExecutor(Sentence *sentence);

protected:
    OnResult                                    onResult_;
};

}   // namespace graph
}   // namespace nebula


#endif  // GRAPH_TRAVERSEEXECUTOR_H_
