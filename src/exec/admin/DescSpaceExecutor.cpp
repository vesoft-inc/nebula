/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/DescSpaceExecutor.h"
#include "planner/Admin.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> DescSpaceExecutor::execute() {
    return descSpace().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> DescSpaceExecutor::descSpace() {
    dumpLog();

    auto *dsNode = asNode<DescSpace>(node());
    return ectx()->getMetaClient()->getSpace(dsNode->getSpaceName())
        .via(runner())
        .then([this](StatusOr<meta::cpp2::SpaceItem> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            DataSet dataSet;
            dataSet.colNames = {"ID",
                                "Name",
                                "Partition number",
                                "Replica Factor",
                                "Vid Size",
                                "Charset",
                                "Collate"};
            auto &spaceItem = resp.value();
            auto &properties = spaceItem.get_properties();
            std::vector<Row> rows;
            std::vector<Value> columns;
            Row row;
            columns.emplace_back(spaceItem.get_space_id());
            columns.emplace_back(properties.get_space_name());
            columns.emplace_back(properties.get_partition_num());
            columns.emplace_back(properties.get_replica_factor());
            columns.emplace_back(properties.get_vid_size());
            columns.emplace_back(properties.get_charset_name());
            columns.emplace_back(properties.get_collate_name());
            row.columns = std::move(columns);
            rows.emplace_back(row);
            dataSet.rows = rows;
            finish(Value(std::move(dataSet)));
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
