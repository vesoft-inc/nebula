/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/ShowAllHostExecutor.h"
#include "storage/StorageServiceHandler.h"


namespace nebula {
namespace graph {

//using namespace meta;

ShowAllHostExecutor::ShowAllHostExecutor(Sentence *sentence,
                                         ExecutionContext *ectx) : Executor(ectx) {
    sentence_ = static_cast<ShowAllHostSentence*>(sentence);
}


Status ShowAllHostExecutor::prepare() {
    return Status::OK();
}


void ShowAllHostExecutor::execute() {
    //TODO(YT) when StorageClient fininshed, then implement this interface
    #if 0
    resp_ = std::make_unique<cpp2::ExecutionResponse>();
    GraphSpaceID space = 0;
    do {
        std::vector<std::string> header{"Host"};
        resp_->set_column_names(std::move(header));
        auto &items = meta::HostManager::get(space)->allHosts();
        std::vector<cpp2::RowValue> rows;
        for (auto &item : items) {
            std::vector<cpp2::ColumnValue> row;
            row.resize(1);
            row[0].set_str(NetworkUtils::intToIPv4(item.first));

             rows.emplace_back();
            rows.back().set_columns(std::move(row));
        }
        resp_->set_rows(std::move(rows));
    } while (false);

     DCHECK(onFinish_);
    onFinish_();
    #endif
}


void ShowAllHostExecutor::setupResponse(cpp2::ExecutionResponse &resp) {
    resp = std::move(*resp_);
}

}   // namespace graph
}   // namespace nebula
