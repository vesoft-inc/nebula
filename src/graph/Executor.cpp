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
#include "graph/InsertVertexExecutor.h"
#include "graph/InsertEdgeExecutor.h"
#include "graph/AssignmentExecutor.h"
#include "graph/ShowExecutor.h"
#include "graph/AddHostsExecutor.h"
#include "graph/RemoveHostsExecutor.h"
#include "graph/CreateSpaceExecutor.h"
#include "graph/DropSpaceExecutor.h"
#include "graph/YieldExecutor.h"

namespace nebula {
namespace graph {

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
        case Sentence::Kind::kAddHosts:
            executor = std::make_unique<AddHostsExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kRemoveHosts:
            executor = std::make_unique<RemoveHostsExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kCreateSpace:
            executor = std::make_unique<CreateSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kDropSpace:
            executor = std::make_unique<DropSpaceExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kYield:
            executor = std::make_unique<YieldExecutor>(sentence, ectx());
            break;
        case Sentence::Kind::kUnknown:
            LOG(FATAL) << "Sentence kind unknown";
            break;
        default:
            LOG(FATAL) << "Sentence kind illegal: " << kind;
            break;
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

nebula::cpp2::SupportedType Executor::columnTypeToSupportedType(ColumnType type) {
    switch (type) {
        case BOOL:
            return nebula::cpp2::SupportedType::BOOL;
        case INT:
            return nebula::cpp2::SupportedType::INT;
        case DOUBLE:
            return nebula::cpp2::SupportedType::DOUBLE;
        case STRING:
            return nebula::cpp2::SupportedType::STRING;
        case TIMESTAMP:
            return nebula::cpp2::SupportedType::TIMESTAMP;
        default:
            return nebula::cpp2::SupportedType::UNKNOWN;
    }
}

}   // namespace graph
}   // namespace nebula
