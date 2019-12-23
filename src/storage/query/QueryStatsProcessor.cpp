/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/QueryStatsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
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
    resp_.set_data(writer.encode());
}


kvstore::ResultCode QueryStatsProcessor::processVertex(PartitionID partId,
                                                       VertexID vId) {
    FilterContext fcontext;
    for (auto& tc : tagContexts_) {
        auto ret = this->collectVertexProps(partId,
                                            vId,
                                            tc.tagId_,
                                            tc.props_,
                                            &fcontext,
                                            &collector_);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }

    for (auto& ec : this->edgeContexts_) {
        auto edgeType = ec.first;
        auto& props = ec.second;
        if (!props.empty()) {
            auto r = this->collectEdgeProps(partId, vId, edgeType, props, &fcontext,
                                            [&, this](RowReader* reader, folly::StringPiece key,
                                                      const std::vector<PropContext>& p) {
                                                this->collectProps(reader, key,  p, &fcontext,
                                                                   &collector_);
                                            });
            if (r != kvstore::ResultCode::SUCCEEDED) {
                return r;
            }
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}


void QueryStatsProcessor::onProcessFinished(int32_t retNum) {
    std::vector<PropContext> props;
    props.reserve(retNum);
    for (auto& tc : this->tagContexts_) {
        for (auto& prop : tc.props_) {
            if (prop.returned_) {
                props.emplace_back(std::move(prop));
            }
        }
    }

    for (auto& ec : this->edgeContexts_) {
        auto p = ec.second;
        for (auto& prop : p) {
            CHECK(prop.returned_);
            props.emplace_back(std::move(prop));
        }
    }
    std::sort(props.begin(), props.end(), [](auto& l, auto& r){
        return l.retIndex_ < r.retIndex_;
    });
    calcResult(std::move(props));
}

}  // namespace storage
}  // namespace nebula
