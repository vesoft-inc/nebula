/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/Executor.h"
#include "parser/TraverseSentences.h"
#include "parser/MutateSentences.h"
#include "parser/MaintainSentences.h"
#include "parser/AdminSentences.h"
#include "graph/GoExecutor.h"
#include "graph/UseExecutor.h"
#include "graph/PipeExecutor.h"
#include "graph/CreateTagExecutor.h"
#include "graph/CreateEdgeExecutor.h"
#include "graph/AlterTagExecutor.h"
#include "graph/AlterEdgeExecutor.h"
#include "graph/DropTagExecutor.h"
#include "graph/DropEdgeExecutor.h"
#include "graph/DescribeTagExecutor.h"
#include "graph/DescribeEdgeExecutor.h"
#include "graph/CreateTagIndexExecutor.h"
#include "graph/CreateEdgeIndexExecutor.h"
#include "graph/DropTagIndexExecutor.h"
#include "graph/DropEdgeIndexExecutor.h"
#include "graph/DescribeTagIndexExecutor.h"
#include "graph/DescribeEdgeIndexExecutor.h"
#include "graph/RebuildTagIndexExecutor.h"
#include "graph/RebuildEdgeIndexExecutor.h"
#include "graph/InsertVertexExecutor.h"
#include "graph/InsertEdgeExecutor.h"
#include "graph/AssignmentExecutor.h"
#include "graph/ShowExecutor.h"
#include "graph/CreateSpaceExecutor.h"
#include "graph/DescribeSpaceExecutor.h"
#include "graph/DropSpaceExecutor.h"
#include "graph/YieldExecutor.h"
#include "graph/DownloadExecutor.h"
#include "graph/OrderByExecutor.h"
#include "graph/IngestExecutor.h"
#include "graph/AdminJobExecutor.h"
#include "graph/ConfigExecutor.h"
#include "graph/FetchVerticesExecutor.h"
#include "graph/FetchEdgesExecutor.h"
#include "graph/ConfigExecutor.h"
#include "graph/SetExecutor.h"
#include "graph/LookupExecutor.h"
#include "graph/MatchExecutor.h"
#include "graph/BalanceExecutor.h"
#include "graph/DeleteVerticesExecutor.h"
#include "graph/DeleteEdgesExecutor.h"
#include "graph/UpdateVertexExecutor.h"
#include "graph/UpdateEdgeExecutor.h"
#include "graph/FindPathExecutor.h"
#include "graph/LimitExecutor.h"
#include "graph/GroupByExecutor.h"
#include "graph/ReturnExecutor.h"
#include "graph/CreateSnapshotExecutor.h"
#include "graph/DropSnapshotExecutor.h"
#include "graph/UserExecutor.h"
#include "graph/PrivilegeExecutor.h"

namespace nebula {
namespace graph {

Executor::Executor(ExecutionContext *ectx, const std::string &statsName) {
    ectx_ = ectx;
    if (!statsName.empty()) {
        stats_ = std::make_unique<stats::Stats>("graph", statsName);
    }

    onVariableVariantGet_ = [this] (const std::string& var) -> OptVariantType {
        if (ectx_ == nullptr || ectx_->variableHolder() == nullptr) {
            return Status::Error( "var %s not defined.", var.c_str());
        }
        const InterimResult* interim = ectx_->variableHolder()->get(var);
        if (interim == nullptr) {
            return Status::Error( "var %s not defined.", var.c_str());
        }
        auto status = interim->getRows();
        if (!status.ok()) {
            return status.status();
        }
        std::vector<cpp2::RowValue> rows = std::move(status).value();
        if (rows.size() != 1) {
            return Status::Error(
                "var %s row_size: %zu != 1",
                var.c_str(), rows.size());
        }
        auto& row = rows[0];
        if (row.get_columns().size() != 1) {
            return Status::Error(
                "var %s col_size: %zu != 1",
                var.c_str(), row.get_columns().size());
        }
        auto& value = row.get_columns().front();

        switch (value.getType()) {
            case cpp2::ColumnValue::Type::id:
                return value.get_id();
            case cpp2::ColumnValue::Type::integer:
                return value.get_integer();
            case cpp2::ColumnValue::Type::double_precision:
                return value.get_double_precision();
            case cpp2::ColumnValue::Type::bool_val:
                return value.get_bool_val();
            case cpp2::ColumnValue::Type::str:
                return value.get_str();
            case cpp2::ColumnValue::Type::timestamp:
                return value.get_timestamp();
            default:
                LOG(ERROR) << "Not Support type: " << value.getType();
                return Status::Error("Not Support type: %d", static_cast<int>(value.getType()));
        }
    };
}

std::unique_ptr<Executor> Executor::makeExecutor(Sentence *sentence) {
    auto kind = sentence->kind();
    std::unique_ptr<Executor> executor;
    switch (kind) {
        case Sentence::Kind::kGo:
            executor = std::make_unique<GoExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kUse:
            executor = std::make_unique<UseExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kPipe:
            executor = std::make_unique<PipeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateTag:
            executor = std::make_unique<CreateTagExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateEdge:
            executor = std::make_unique<CreateEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAlterTag:
            executor = std::make_unique<AlterTagExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAlterEdge:
            executor = std::make_unique<AlterEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeTag:
            executor = std::make_unique<DescribeTagExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeEdge:
            executor = std::make_unique<DescribeEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropTag:
             executor = std::make_unique<DropTagExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kDropEdge:
             executor = std::make_unique<DropEdgeExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kCreateTagIndex:
            executor = std::make_unique<CreateTagIndexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateEdgeIndex:
            executor = std::make_unique<CreateEdgeIndexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeTagIndex:
            executor = std::make_unique<DescribeTagIndexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeEdgeIndex:
            executor = std::make_unique<DescribeEdgeIndexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropTagIndex:
             executor = std::make_unique<DropTagIndexExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kDropEdgeIndex:
             executor = std::make_unique<DropEdgeIndexExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kRebuildTagIndex:
             executor = std::make_unique<RebuildTagIndexExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kRebuildEdgeIndex:
             executor = std::make_unique<RebuildEdgeIndexExecutor>(sentence, ectx());
             break;
        case Sentence::Kind::kInsertVertex:
            executor = std::make_unique<InsertVertexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kInsertEdge:
            executor = std::make_unique<InsertEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kShow:
            executor = std::make_unique<ShowExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAssignment:
            executor = std::make_unique<AssignmentExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateSpace:
            executor = std::make_unique<CreateSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropSpace:
            executor = std::make_unique<DropSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDescribeSpace:
            executor = std::make_unique<DescribeSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kYield:
            executor = std::make_unique<YieldExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDownload:
            executor = std::make_unique<DownloadExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kOrderBy:
            executor = std::make_unique<OrderByExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kIngest:
            executor = std::make_unique<IngestExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kConfig:
            executor = std::make_unique<ConfigExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::KGroupBy:
            executor = std::make_unique<GroupByExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kFetchVertices:
            executor = std::make_unique<FetchVerticesExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kFetchEdges:
            executor = std::make_unique<FetchEdgesExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kSet:
            executor = std::make_unique<SetExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kMatch:
            executor = std::make_unique<MatchExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kLookup:
            executor = std::make_unique<LookupExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kBalance:
            executor = std::make_unique<BalanceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDeleteVertex:
            executor = std::make_unique<DeleteVerticesExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDeleteEdges:
            executor = std::make_unique<DeleteEdgesExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kUpdateVertex:
            executor = std::make_unique<UpdateVertexExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kUpdateEdge:
            executor = std::make_unique<UpdateEdgeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kFindPath:
            executor = std::make_unique<FindPathExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kLimit:
            executor = std::make_unique<LimitExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kReturn:
            executor = std::make_unique<ReturnExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateSnapshot:
            executor = std::make_unique<CreateSnapshotExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropSnapshot:
            executor = std::make_unique<DropSnapshotExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAdmin:
            executor = std::make_unique<AdminJobExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateUser:
            executor = std::make_unique<CreateUserExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropUser:
            executor = std::make_unique<DropUserExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kAlterUser:
            executor = std::make_unique<AlterUserExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kChangePassword:
            executor = std::make_unique<ChangePasswordExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kGrant:
            executor = std::make_unique<GrantExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kRevoke:
            executor = std::make_unique<RevokeExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kUnknown:
            LOG(ERROR) << "Sentence kind unknown";
            return nullptr;
        default:
            LOG(ERROR) << "Sentence kind illegal: " << kind;
            return nullptr;
    }
    return executor;
}

std::string Executor::valueTypeToString(nebula::cpp2::ValueType type) {
    switch (type.type) {
        case nebula::cpp2::SupportedType::BOOL:
            return "bool";
        case nebula::cpp2::SupportedType::INT:
            return "int";
        case nebula::cpp2::SupportedType::DOUBLE:
            return "double";
        case nebula::cpp2::SupportedType::STRING:
            return "string";
        case nebula::cpp2::SupportedType::TIMESTAMP:
            return "timestamp";
        default:
            return "unknown";
    }
}

Status Executor::writeVariantType(RowWriter &writer, const VariantType &value) {
    switch (value.which()) {
        case VAR_INT64:
            writer << boost::get<int64_t>(value);
            break;
        case VAR_DOUBLE:
            writer << boost::get<double>(value);
            break;
        case VAR_BOOL:
            writer << boost::get<bool>(value);
            break;
        case VAR_STR:
            writer << boost::get<std::string>(value);
            break;
        default:
            LOG(ERROR) << "Unknown value type: " << static_cast<uint32_t>(value.which());
            return Status::Error("Unknown value type: %d", value.which());
    }
    return Status::OK();
}

bool Executor::checkValueType(const nebula::cpp2::ValueType &type, const VariantType &value) {
    switch (value.which()) {
        case VAR_INT64:
            return nebula::cpp2::SupportedType::INT == type.type ||
                   nebula::cpp2::SupportedType::TIMESTAMP == type.type;
        case VAR_DOUBLE:
            return nebula::cpp2::SupportedType::DOUBLE == type.type;
        case VAR_BOOL:
            return nebula::cpp2::SupportedType::BOOL == type.type;
        case VAR_STR:
            return nebula::cpp2::SupportedType::STRING == type.type ||
                   nebula::cpp2::SupportedType::TIMESTAMP == type.type;
        // TODO: Other type
    }

    return false;
}

StatusOr<cpp2::ColumnValue> Executor::toColumnValue(const VariantType& value,
                                                    cpp2::ColumnValue::Type type) const {
    cpp2::ColumnValue colVal;
    try {
        if (type == cpp2::ColumnValue::Type::__EMPTY__) {
            switch (value.which()) {
                case VAR_INT64:
                    colVal.set_integer(boost::get<int64_t>(value));
                    break;
                case VAR_DOUBLE:
                    colVal.set_double_precision(boost::get<double>(value));
                    break;
                case VAR_BOOL:
                    colVal.set_bool_val(boost::get<bool>(value));
                    break;
                case VAR_STR:
                    colVal.set_str(boost::get<std::string>(value));
                    break;
                default:
                    LOG(ERROR) << "Wrong Type: " << value.which();
                    return Status::Error("Wrong Type: %d", value.which());
            }
            return colVal;
        }
        switch (type) {
            case cpp2::ColumnValue::Type::id:
                colVal.set_id(boost::get<int64_t>(value));
                break;
            case cpp2::ColumnValue::Type::integer:
                colVal.set_integer(boost::get<int64_t>(value));
                break;
            case cpp2::ColumnValue::Type::timestamp:
                colVal.set_timestamp(boost::get<int64_t>(value));
                break;
            case cpp2::ColumnValue::Type::double_precision:
                colVal.set_double_precision(boost::get<double>(value));
                break;
            case cpp2::ColumnValue::Type::bool_val:
                colVal.set_bool_val(boost::get<bool>(value));
                break;
            case cpp2::ColumnValue::Type::str:
                colVal.set_str(boost::get<std::string>(value));
                break;
            default:
                LOG(ERROR) << "Wrong Type: " << static_cast<int32_t>(type);
                return Status::Error("Wrong Type: %d", static_cast<int32_t>(type));
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception caught: " << e.what();
        return Status::Error("Wrong Type: %d", static_cast<int32_t>(type));
    }
    return colVal;
}

OptVariantType Executor::toVariantType(const cpp2::ColumnValue& value) const {
    switch (value.getType()) {
        case cpp2::ColumnValue::Type::id:
            return value.get_id();
        case cpp2::ColumnValue::Type::integer:
            return value.get_integer();
        case cpp2::ColumnValue::Type::bool_val:
            return value.get_bool_val();
        case cpp2::ColumnValue::Type::double_precision:
            return value.get_double_precision();
        case cpp2::ColumnValue::Type::str:
            return value.get_str();
        case cpp2::ColumnValue::Type::timestamp:
            return value.get_timestamp();
        default:
            break;
    }

    LOG(ERROR) << "Unknown ColumnType: " << static_cast<int32_t>(value.getType());
    return Status::Error("Unknown ColumnType: %d", static_cast<int32_t>(value.getType()));
}

void Executor::doError(Status status, uint32_t count) const {
    stats::Stats::addStatsValue(stats_.get(), false, duration().elapsedInUSec(), count);
    DCHECK(onError_);
    onError_(std::move(status));
}

void Executor::doFinish(ProcessControl pro, uint32_t count) const {
    stats::Stats::addStatsValue(stats_.get(), true, duration().elapsedInUSec(), count);
    DCHECK(onFinish_);
    onFinish_(pro);
}
}   // namespace graph
}   // namespace nebula
