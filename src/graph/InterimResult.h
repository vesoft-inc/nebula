/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_INTERIMRESULT_H_
#define GRAPH_INTERIMRESULT_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowSetWriter.h"
#include "dataman/SchemaWriter.h"

/**
 * The intermediate form of execution result, used in pipeline and variable.
 */


namespace nebula {
namespace graph {

class InterimResult final {
public:
    InterimResult() = default;
    ~InterimResult() = default;
    InterimResult(const InterimResult &) = default;
    InterimResult& operator=(const InterimResult &) = default;
    InterimResult(InterimResult &&) = default;
    InterimResult& operator=(InterimResult &&) = default;

    explicit InterimResult(std::unique_ptr<RowSetWriter> rsWriter);
    explicit InterimResult(std::vector<VertexID> vids);

    std::shared_ptr<const meta::SchemaProviderIf> schema() const {
        return rsReader_->schema();
    }

    StatusOr<std::vector<VertexID>> getVIDs(const std::string &col) const;

    std::vector<cpp2::RowValue> getRows() const;
    // TODO(dutor) iterating interfaces on rows and columns

private:
    std::unique_ptr<RowSetReader>               rsReader_;
    std::unique_ptr<RowSetWriter>               rsWriter_;
    std::vector<VertexID>                       vids_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INTERIMRESULT_H_
