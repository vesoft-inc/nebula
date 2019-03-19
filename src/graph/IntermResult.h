/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_INTERMRESULT_H_
#define GRAPH_INTERMRESULT_H_

#include "base/Base.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowSetWriter.h"
#include "dataman/SchemaWriter.h"

/**
 * The intermediate form of execution result, used in pipeline and variable.
 */


namespace nebula {
namespace graph {

class IntermResult final {
public:
    IntermResult() = default;
    ~IntermResult() = default;
    IntermResult(const IntermResult &) = default;
    IntermResult& operator=(const IntermResult &) = default;
    IntermResult(IntermResult &&) = default;
    IntermResult& operator=(IntermResult &&) = default;

    explicit IntermResult(RowSetWriter *rows);
    explicit IntermResult(std::vector<VertexID> vids);

    std::vector<VertexID> getVIDs(const std::string &col) const;

    // TODO(dutor) iterating interfaces on rows and columns

private:
    std::unique_ptr<RowSetReader>               rsReader_;
    std::string                                 data_;
    std::vector<VertexID>                       vids_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_INTERMRESULT_H_
