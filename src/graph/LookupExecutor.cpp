/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/LookupExecutor.h"

namespace nebula {
namespace graph {

using apache::thrift::FragileConstructor::FRAGILE;

LookupExecutor::LookupExecutor(Sentence *sentence, ExecutionContext *ectx)
    : TraverseExecutor(ectx) {
    sentence_ = dynamic_cast<LookupSentence*>(sentence);
}


Status LookupExecutor::prepare() {
    return Status::OK();
}


Status LookupExecutor::prepareClauses() {
    DCHECK(sentence_ != nullptr);
    Status status = Status::OK();

    status = prepareFrom();
    if (!status.ok()) {
        return status;
    }
    status = prepareYieldClause();
    if (!status.ok()) {
        return status;
    }
    status = prepareWhere();
    if (!status.ok()) {
        return status;
    }
    return status;
}


Status LookupExecutor::prepareFrom() {
    Status status = Status::IndexNotFound();
    from_ = sentence_->from();
    if (from_ == nullptr) {
        return Status::Error("From clause shall never be null");
    }

    spaceId_ = ectx()->rctx()->session()->space();
    auto* mc = ectx()->getMetaClient();
    auto eRet = mc->getEdgeIndex(spaceId_, *from_).get();
    if (eRet.ok()) {
        isEdge_ = true;
        indexId_ = eRet.value().get_index_id();
        auto fields = eRet.value().get_fields().get_fields();
        auto iter = fields.find(*from_);
        if (iter != fields.end()) {
            fields_ = iter->second;
            return Status::OK();
        }
    }

    auto tRet = mc->getTagIndex(spaceId_, *from_).get();
    if (tRet.ok()) {
        isEdge_ = false;
        indexId_ = tRet.value().get_index_id();
        auto fields = tRet.value().get_fields().get_fields();
        auto iter = fields.find(*from_);
        if (iter != fields.end()) {
            fields_ = iter->second;
            return Status::OK();
        }
    }
    return status;
}


Status LookupExecutor::prepareYieldClause() {
    Status status = Status::OK();
    const auto* yieldClause = sentence_->yieldClause();
    if (yieldClause == nullptr) {
        return status;
    }
    auto* mc = ectx()->getMetaClient();
    std::shared_ptr<const meta::SchemaProviderIf> schema;
    if (isEdge_) {
        auto ret = mc->getEdgeTypeByNameFromCache(spaceId_, *from_);
        if (!ret.ok()) {
            return ret.status();
        }
        auto ver = mc->getNewestEdgeVerFromCache(spaceId_, ret.value());
        if (!ver.ok()) {
            return ver.status();
        }
        auto sc = mc->getEdgeSchemaFromCache(spaceId_, ret.value(), ver.value());
        if (!sc.ok()) {
            return sc.status();
        }
        schema = sc.value();
    } else {
        auto ret = mc->getTagIDByNameFromCache(spaceId_, *from_);
        if (!ret.ok()) {
            return ret.status();
        }
        auto ver = mc->getNewestTagVerFromCache(spaceId_, ret.value());
        if (!ver.ok()) {
            return ver.status();
        }
        auto sc = mc->getTagSchemaFromCache(spaceId_, ret.value(), ver.value());
        if (!sc.ok()) {
            return sc.status();
        }
        schema = sc.value();
    }

    const auto cols = yieldClause->columns();

    for (auto* col : cols) {
        status = col->expr()->prepare();
        if (!status.ok()) {
            return status;
        }
        if (col->alias() != nullptr) {
            Status::SyntaxError("Do not support alias in lookup statement");
        }
        if (col->expr()->isAliasExpression()) {
            auto alias = *dynamic_cast<AliasPropertyExpression *>(col->expr())->alias();
            if (*from_ != alias) {
                Status::SyntaxError("schema name error : %s", alias.c_str());
            }
            auto prop = *dynamic_cast<AliasPropertyExpression *>(col->expr())->prop();
            auto type = schema->getFieldType(prop);
            if (type == CommonConstants::kInvalidValueType()) {
                return Status::Error(folly::stringPrintf("Column %s not found in %s",
                                                         prop.c_str(),
                                                         from_->c_str()));
            }
            nebula::cpp2::ColumnDef column;
            column.set_name(std::move(prop).c_str());
            column.set_type(std::move(type));
            returnCols_.emplace_back(std::move(column));
        }
    }
    return status;
}


Status LookupExecutor::prepareWhere() {
    from_ = sentence_->from();
    Status status = Status::OK();
    auto *clause = sentence_->whereClause();

    if (clause == nullptr) {
        return Status::SyntaxError("Must specify where clause");
    }

    auto *filter = clause->filter();
    status = prepareExpression(filter);
    if (!status.ok()) {
        return status;
    }

    /**
     * step 1 : Check operatorList_ , Support only :
     * Only equivalent query for the first N columns are supported
     * Or a range query for the first column
     **/
     bool isRange = false;
    if (operatorList_[0].second == RelationalExpression::Operator::EQ) {
        for (auto& op : operatorList_) {
            if (op.second != RelationalExpression::Operator::EQ) {
                return Status::SyntaxError("Syntax unsupported");
            }
        }
    } else {
        for (auto& op : operatorList_) {
            if (op.second == RelationalExpression::Operator::EQ) {
                return Status::SyntaxError("Syntax unsupported");
            }
        }
        if (operatorList_.size() > 2) {
            return Status::SyntaxError("Syntax unsupported");
        } else if (operatorList_.size() == 2) {
            if (operatorList_[0].first.first != operatorList_[1].first.first) {
                return Status::SyntaxError("Syntax unsupported");
            }
        }
        isRange = true;
    }

    /**
     * step 2 : Check whether the condition column exists in the index
     **/
    for (auto & op : operatorList_) {
        auto it = std::find_if(fields_.begin(), fields_.end(),
                               [&op] (const auto& field) {
                                   return field.get_name() == op.first.first;
                               });
        if (it == fields_.end()) {
            return Status::SyntaxError("The column was not found in the index : %s" ,
                                       op.first.first.c_str());
        }
    }

    /**
     * step 3 : Check if it's the first N columns
     **/
    if (isRange) {
        if (operatorList_[0].first.first != fields_[0].get_name()) {
            return Status::SyntaxError("The range query only "
                                       "supports the first column in the index");
        }
    } else {
        auto opSize = operatorList_.size();
        for (auto& col : fields_) {
            auto it = std::find_if(operatorList_.begin(), operatorList_.end(),
                                   [&col] (const auto& op) {
                                       return col.get_name() == op.first.first;
                                   });
            if (it == operatorList_.end()) {
                return Status::SyntaxError("The column was not found in the index : %s" ,
                                           col.get_name().c_str());
            }
            if (--opSize == 0) {
                break;
            }
        }
    }

    /**
     * step 4 : setup index hints
     **/

    return setupIndexHint(isRange);
}


Status LookupExecutor::setupIndexHint(bool isRange) {
    Status status = Status::OK();
    indexHint_.set_index_id(indexId_);
    indexHint_.set_is_range(isRange);
    std::vector<nebula::cpp2::IndexHintItem> items;
    if (isRange) {
        if (operatorList_.size() == 1) {
            auto ret = getIndexHintItem(operatorList_[0].first.second,
                                        operatorList_[0].second,
                                        fields_[0].get_type().get_type());
            if (!ret.ok()) {
                return Status::Error("Index hint error");
            }
            items.emplace_back(ret.value());
        } else {
            auto ret = getIndexHintItem(fields_[0].get_type().get_type());
            if (!ret.ok()) {
                return Status::Error("Index hint error");
            }
            items.emplace_back(ret.value());
        }
    } else {
        auto opSize = operatorList_.size();
        for (auto& field : fields_) {
            if (opSize-- == 0) {
                break;
            }
            auto it = std::find_if(operatorList_.begin(), operatorList_.end(),
                                   [&field] (const auto& op) {
                                       return field.get_name() == op.first.first;
                                   });
            if (it == operatorList_.end()) {
                return Status::SyntaxError("The column was not found in the index : %s" ,
                                           field.get_name().c_str());
            }
            nebula::cpp2::IndexHintItem item;
            auto ret = getValue(it->first.second, field.get_type().get_type());
            if (!ret.ok()) {
                return ret.status();
            }
            item.set_first_str(ret.value());
            item.set_type(field.get_type().get_type());
            items.emplace_back(std::move(item));
        }
    }
    indexHint_.set_hint_items(std::move(items));
    return status;
}

StatusOr<nebula::cpp2::IndexHintItem>
LookupExecutor::prepareIndexHint(VariantType v,
                                 nebula::cpp2::SupportedType t,
                                 bool isEqual,
                                 bool isGreater) {
    nebula::cpp2::IndexHintItem item;
    item.set_type(t);
    switch (t) {
        case nebula::cpp2::SupportedType::DOUBLE :
        case nebula::cpp2::SupportedType::FLOAT : {
            if (v.which() != VAR_INT64 && v.which() != VAR_DOUBLE) {
                return Status::Error("data type error");
            }
            if (isGreater) {
                auto begin = isEqual ?
                             folly::to<std::string>(boost::get<double>(v) + 0.00001) :
                             folly::to<std::string>(boost::get<double>(v));
                auto end = folly::to<std::string>(std::numeric_limits<double>::max());
                item.set_first_str(std::move(begin));
                item.set_second_str(std::move(end));
            } else {
                auto begin = folly::to<std::string>(std::numeric_limits<double>::min());
                auto end = isEqual ?
                           folly::to<std::string>(boost::get<double>(v) - 0.00001) :
                           folly::to<std::string>(boost::get<double>(v));
                item.set_first_str(std::move(begin));
                item.set_second_str(std::move(end));
            }
            break;
        }
        case nebula::cpp2::SupportedType::INT : {
            if (v.which() != VAR_INT64) {
                return Status::Error("data type error");
            }
            if (isGreater) {
                auto begin = isEqual ?
                             folly::to<std::string>(boost::get<int64_t>(v) + 1) :
                             folly::to<std::string>(boost::get<int64_t>(v));
                auto end = folly::to<std::string>(std::numeric_limits<int64_t>::max());
                item.set_first_str(std::move(begin));
                item.set_second_str(std::move(end));
            } else {
                auto begin = folly::to<std::string>(std::numeric_limits<int64_t>::min());
                auto end = isEqual ?
                           folly::to<std::string>(boost::get<int64_t>(v) - 1) :
                           folly::to<std::string>(boost::get<int64_t>(v));
                item.set_first_str(std::move(begin));
                item.set_second_str(std::move(end));
            }
            break;
        }
        case nebula::cpp2::SupportedType::TIMESTAMP : {
            if (v.which() != VAR_INT64 && v.which() != VAR_STR) {
                return Status::Error("data type error");
            }
            auto ret = toTimestamp(v);
            if (!ret.ok()) {
                return ret.status();
            }
            if (isGreater) {
                auto begin = isEqual ?
                             folly::to<std::string>(ret.value() + 1) :
                             folly::to<std::string>(ret.value());
                auto end = folly::to<std::string>(std::numeric_limits<int64_t>::max());
                item.set_first_str(std::move(begin));
                item.set_second_str(std::move(end));
            } else {
                auto begin = folly::to<std::string>(std::numeric_limits<int64_t>::min());
                auto end = isEqual ?
                           folly::to<std::string>(ret.value() - 1) :
                           folly::to<std::string>(ret.value());
                item.set_first_str(std::move(begin));
                item.set_second_str(std::move(end));
            }
            break;
        }
        default :
            return Status::Error("Unsupported operation type");
    }
    return item;
}

StatusOr<nebula::cpp2::IndexHintItem>
LookupExecutor::getIndexHintItem(VariantType v,
                                 RelationalExpression::Operator op,
                                 nebula::cpp2::SupportedType t) {
    StatusOr<nebula::cpp2::IndexHintItem> ret;
    switch (op) {
        case RelationalExpression::Operator::GT : {
            ret = prepareIndexHint(v, t, true, true);
            break;
        }
        case RelationalExpression::Operator::LT : {
            ret = prepareIndexHint(v, t, true, false);
            break;
        }
        case RelationalExpression::Operator::GE: {
            ret = prepareIndexHint(v, t, false, true);
            break;
        }
        case RelationalExpression::Operator::LE: {
            ret = prepareIndexHint(v, t, false, false);
            break;
        }
        default :
            return Status::Error("Unsupported operation");
    }
    return ret;
}

StatusOr<nebula::cpp2::IndexHintItem>
LookupExecutor::getIndexHintItem(nebula::cpp2::SupportedType t) {
    nebula::cpp2::IndexHintItem item;
    item.set_type(t);
    auto firstOp = operatorList_[0].second;
    auto secondOp = operatorList_[1].second;
    if ((firstOp == RelationalExpression::Operator::GT ||
         firstOp == RelationalExpression::Operator::GE) &&
             (secondOp == RelationalExpression::Operator::LT ||
              secondOp == RelationalExpression::Operator::LE)) {
        auto endRet = getIndexHintItem(operatorList_[0].first.second,
                                         firstOp,
                                         t);
        if (!endRet.ok()) {
            return Status::Error("Unsupported operation");
        }
        auto beginRet = getIndexHintItem(operatorList_[1].first.second,
                                       secondOp,
                                       t);
        if (!beginRet.ok()) {
            return Status::Error("Unsupported operation");
        }
        item.set_first_str(beginRet.value().get_second_str());
        item.set_second_str(endRet.value().get_first_str());
    } else if ((firstOp == RelationalExpression::Operator::LT ||
                firstOp == RelationalExpression::Operator::LE) &&
               (secondOp == RelationalExpression::Operator::GT ||
                secondOp == RelationalExpression::Operator::GE)) {
        auto beginRet = getIndexHintItem(operatorList_[0].first.second,
                                         firstOp,
                                         t);
        if (!beginRet.ok()) {
            return Status::Error("Unsupported operation");
        }
        auto endRet = getIndexHintItem(operatorList_[1].first.second,
                                       secondOp,
                                       t);
        if (!endRet.ok()) {
            return Status::Error("Unsupported operation");
        }
        item.set_first_str(beginRet.value().get_second_str());
        item.set_second_str(endRet.value().get_first_str());
    } else {
        return Status::Error("Unsupported operation");
    }
    return item;
}

StatusOr<std::string> LookupExecutor::getValue(VariantType v, nebula::cpp2::SupportedType t) {
    switch (t) {
        case nebula::cpp2::SupportedType::BOOL: {
            if (v.which() != VAR_BOOL) {
                break;
            }
            return folly::to<std::string>(boost::get<bool>(v));
        }
        case nebula::cpp2::SupportedType::INT: {
            if (v.which() != VAR_INT64) {
                break;
            }
            std::string raw;
            raw.reserve(sizeof(int64_t));
            auto val = folly::Endian::big(boost::get<int64_t>(v));
            raw.append(reinterpret_cast<const char*>(&val), sizeof(int64_t));
            return raw;
        }
        case nebula::cpp2::SupportedType::TIMESTAMP: {
            auto ret = toTimestamp(v);
            if (!ret.ok()) {
                return ret.status();
            }
            std::string raw;
            raw.reserve(sizeof(int64_t));
            auto val = folly::Endian::big(ret.value());
            raw.append(reinterpret_cast<const char*>(&val), sizeof(int64_t));
            return raw;
        }
        case nebula::cpp2::SupportedType::FLOAT:
        case nebula::cpp2::SupportedType::DOUBLE: {
            if (v.which() != VAR_INT64 && v.which() != VAR_DOUBLE) {
                break;
            }
            return folly::to<std::string>(boost::get<double>(v));
        }
        case nebula::cpp2::SupportedType::STRING: {
            if (v.which() != VAR_STR) {
                break;
            }
            return boost::get<std::string>(v);
        }
        default:
            return Status::Error("Unsupported data type");
    }
    return Status::Error("data type error");
}

StatusOr<std::string> LookupExecutor::prepareAPE(const Expression* expr) {
    auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(expr);
    const auto* alias = aExpr->alias();
    if (*alias != *from_) {
        return Status::SyntaxError("Alias error : %s ", alias);
    }
    return aExpr->prop()->c_str();
}

StatusOr<VariantType> LookupExecutor::preparePE(const Expression* expr) {
    auto* pExpr = dynamic_cast<const PrimaryExpression*>(expr);
    auto value = pExpr->eval();
    if (!value.ok()) {
        return Status::SyntaxError("Expression error : %s", pExpr->toString().data());
    }
    return value.value();
}

Status LookupExecutor::prepareRE(const RelationalExpression* expr) {
    Status status = Status::OK();
    std::string prop;
    VariantType v;
    auto op = expr->op();
    if (op == RelationalExpression::Operator::NE) {
        return Status::SyntaxError("'!=' not supported yet");
    }
    auto* left = expr->left();
    auto* right = expr->right();
    if (left->kind() == nebula::Expression::kAliasProp) {
        auto pRet = prepareAPE(left);
        if (!pRet.ok()) {
            return pRet.status();
        }
        prop = pRet.value();
        if (right->kind() != nebula::Expression::kPrimary) {
            return Status::SyntaxError("Expression error : %s", right->toString().data());
        }
        auto ret = preparePE(right);
        if (!ret.ok()) {
            return ret.status();
        }
        v = ret.value();
    } else if (left->kind() == nebula::Expression::kPrimary) {
        auto ret = preparePE(right);
        if (!ret.ok()) {
            return ret.status();
        }
        v = ret.value();
        if (right->kind() != nebula::Expression::kAliasProp) {
            return Status::SyntaxError("Expression error : %s", right->toString().data());
        }
        auto pRet = prepareAPE(left);
        if (!pRet.ok()) {
            return pRet.status();
        }
        prop = pRet.value();
        auto opRet = reverseOperator(op);
        if (!opRet.ok()) {
            return opRet.status();
        }
        op = opRet.value();
    } else {
        return Status::SyntaxError("Expression error : %s", expr->toString().data());
    }
    operatorList_.emplace_back(std::make_pair(std::make_pair(std::move(prop), std::move(v)), op));
    return status;
}

StatusOr<RelationalExpression::Operator>
LookupExecutor::reverseOperator(RelationalExpression::Operator op) {
    switch (op) {
        case RelationalExpression::Operator::LT : {
            return RelationalExpression::Operator::GT;
        }
        case RelationalExpression::Operator::GT : {
            return RelationalExpression::Operator::LT;
        }
        case RelationalExpression::Operator::LE : {
            return RelationalExpression::Operator::GT;
        }
        case RelationalExpression::Operator::GE : {
            return RelationalExpression::Operator::LE;
        }
        case RelationalExpression::Operator::EQ : {
            return RelationalExpression::Operator::EQ;
        }
        default :
            return Status::Error("Error relational expression");
    }
    return Status::Error("Error relational expression");
}

Status LookupExecutor::prepareLE(const LogicalExpression* expr) {
    Status status = Status::OK();
    if (expr->op() != LogicalExpression::Operator::AND) {
        return Status::SyntaxError("Operations other than 'AND' are not supported");
    }
    auto* left = expr->left();
    status = prepareExpression(left);
    if (!status.ok()) {
        return status;
    }

    auto* right = expr->right();
    status = prepareExpression(right);
    if (!status.ok()) {
        return status;
    }
    return status;
}

Status LookupExecutor::prepareExpression(const Expression* expr) {
    Status status = Status::OK();
    if (expr->kind() == nebula::Expression::kLogical) {
        auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
        status = prepareLE(lExpr);
    } else if (expr->kind() == nebula::Expression::kRelational) {
        auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
        status = prepareRE(rExpr);
    } else {
        return Status::SyntaxError("Operations other than 'AND' are not supported");
    }
    return status;
}

void LookupExecutor::execute() {
    FLOG_INFO("Executing Find : %s", sentence_->toString().c_str());
    auto status = prepareClauses();
    if (!status.ok()) {
        doError(std::move(status), ectx()->getGraphStats()->getLookupStats());
        return;
    }
    scanIndex();
}

void LookupExecutor::scanIndex() {
    // TODO(sky) : call storage client api and create result set.
    //             depend PR #1459 , #1437, #1360
}

}   // namespace graph
}   // namespace nebula
