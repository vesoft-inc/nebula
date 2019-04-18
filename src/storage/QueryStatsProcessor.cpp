/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/QueryStatsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"


namespace nebula {
namespace storage {

void QueryStatsProcessor::calcResult(std::vector<PropContext>&& props) {
    RowWriter writer;
    decltype(resp_.schema) s;
    decltype(resp_.schema.columns) cols;
    for (auto& prop : props) {
        switch (prop.prop_.stat) {
            case cpp2::StatType::SUM: {
                switch (prop.sum_.which()) {
                    case 0:
                        writer << boost::get<int64_t>(prop.sum_);
                        cols.emplace_back(
                                columnDef(std::move(prop.prop_.name),
                                          nebula::cpp2::SupportedType::INT));
                        break;
                    case 1:
                        writer << boost::get<double>(prop.sum_);
                        cols.emplace_back(
                                columnDef(std::move(prop.prop_.name),
                                          nebula::cpp2::SupportedType::DOUBLE));
                        break;
                }
                break;
            }
            case cpp2::StatType::COUNT: {
                writer << prop.count_;
                cols.emplace_back(
                            columnDef(std::move(prop.prop_.name),
                                      nebula::cpp2::SupportedType::INT));
                break;
            }
            case cpp2::StatType::AVG: {
                switch (prop.sum_.which()) {
                    case 0:
                        writer << static_cast<double>(boost::get<int64_t>(prop.sum_))
                                  / prop.count_;
                        break;
                    case 1:
                        writer << boost::get<double>(prop.sum_) / prop.count_;
                        break;
                }
                cols.emplace_back(
                        columnDef(std::move(prop.prop_.name),
                                  nebula::cpp2::SupportedType::DOUBLE));
                break;
            }
        }
    }
    s.set_columns(std::move(cols));
    resp_.set_schema(std::move(s));
    resp_.set_data(std::move(writer.encode()));
}

kvstore::ResultCode QueryStatsProcessor::processVertex(PartitionID partId,
                                                       VertexID vId) {
    for (auto& tc : this->tagContexts_) {
        auto ret = this->collectVertexProps(partId, vId, tc.tagId_, tc.props_, &collector_);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }

    if (!this->edgeContext_.props_.empty()) {
         return this->collectEdgeProps(partId,
                                       vId,
                                       this->edgeContext_.edgeType_,
                                       this->edgeContext_.props_,
                                       [&, this] (RowReader* reader,
                                                  folly::StringPiece key,
                                                  const std::vector<PropContext>& props) {
                                           this->collectProps(reader, key, props, &collector_);
                                       });
    }
    return kvstore::ResultCode::SUCCEEDED;
}


void QueryStatsProcessor::onProcessFinished(int32_t retNum) {
    std::vector<PropContext> props;
    props.reserve(retNum);
    for (auto& tc : this->tagContexts_) {
        for (auto& prop : tc.props_) {
            props.emplace_back(std::move(prop));
        }
    }
    for (auto& prop : this->edgeContext_.props_) {
        props.emplace_back(std::move(prop));
    }
    std::sort(props.begin(), props.end(), [](auto& l, auto& r){
        return l.retIndex_ < r.retIndex_;
    });
    calcResult(std::move(props));
}

}  // namespace storage
}  // namespace nebula
