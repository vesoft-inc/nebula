/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/QueryEdgePropsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

void QueryEdgePropsProcessor::prepareSchemata(
                                     const std::vector<cpp2::PropDef>& props,
                                     EdgeType edgeType,
                                     SchemaProviderIf*& edgeSchema,
                                     cpp2::Schema& respEdge,
                                     int32_t& schemaVer) {
    edgeSchema = schemaMan_->edgeSchema(spaceId_, edgeType);
    CHECK_NOTNULL(edgeSchema);
    schemaVer = schemaMan_->version();
    respEdge.columns.reserve(props.size());
    for (auto& prop : props) {
        switch (prop.owner) {
            case cpp2::PropOwner::SOURCE:
            case cpp2::PropOwner::DEST: {
                VLOG(1) << "Ignore tag request.";
                break;
            }
            case cpp2::PropOwner::EDGE: {
                const auto* ftype = edgeSchema->getFieldType(prop.name, schemaVer);
                if (ftype != nullptr) {
                    respEdge.columns.emplace_back(
                            columnDef(std::move(prop.get_name()), ftype->type));
                }
                break;
            }
        }
    };
}


kvstore::ResultCode QueryEdgePropsProcessor::collectEdgesProps(
                                       PartitionID partId,
                                       const cpp2::EdgeKey& edgeKey,
                                       SchemaProviderIf* edgeSchema,
                                       SchemaProviderIf* respEdgeSchema,
                                       RowSetWriter& rsWriter) {
    auto prefix = KeyUtils::prefix(partId, edgeKey.src, edgeKey.edge_type, edgeKey.dst, edgeKey.ranking);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
    // Only use the latest version.
    if (iter && iter->valid()) {
        RowWriter writer;
        auto key = iter->key();
        UNUSED(key);
        auto val = iter->val();
        // TODO pass filter
        output(edgeSchema, val, 0, respEdgeSchema, writer);
        iter->next();
        rsWriter.addRow(writer);
    }
    return ret;
}

void QueryEdgePropsProcessor::process(const cpp2::EdgePropRequest& req) {
    time::Duration dur;
    spaceId_ = req.get_space_id();
    auto edgeType = req.get_edge_type();
    int32_t schemaVer = 0;
    cpp2::Schema respEdge;
    SchemaProviderIf* edgeSchema;
    prepareSchemata(req.get_return_columns(), edgeType,
                    edgeSchema, respEdge, schemaVer);

    std::unique_ptr<SchemaProviderIf> respEdgeSchema(new ResultSchemaProvider(respEdge));
    RowSetWriter rsWriter;
    std::for_each(req.get_edges().begin(), req.get_edges().end(), [&](auto& partE) {
        auto partId = partE.first;
        kvstore::ResultCode ret;
        std::for_each(partE.second.begin(), partE.second.end(), [&](auto& edgeKey) {
            ret = collectEdgesProps(partId, edgeKey, edgeSchema,
                                    respEdgeSchema.get(), rsWriter);
        });
        // TODO handle failures
        cpp2::ResultCode thriftRet;
        thriftRet.code = to(ret);
        thriftRet.part_id = partId;
        resp_.codes.emplace_back(std::move(thriftRet));
    });
    resp_.data = std::move(rsWriter.data());
    resp_.schema = std::move(respEdge);
    resp_.latency_in_ms = dur.elapsedInMSec();
    onFinished();
}

}  // namespace storage
}  // namespace nebula
