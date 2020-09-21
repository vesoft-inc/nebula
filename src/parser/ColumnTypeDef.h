/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_COLUMNTYPEDEF_H_
#define PARSER_COLUMNTYPEDEF_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
struct ColumnTypeDef {
    explicit ColumnTypeDef(meta::cpp2::PropertyType t, int16_t l = 0) : type(t), typeLen(l) {}

    std::string toString() const {
        switch (this->type) {
            case meta::cpp2::PropertyType::BOOL:
            case meta::cpp2::PropertyType::INT8:
            case meta::cpp2::PropertyType::INT16:
            case meta::cpp2::PropertyType::INT32:
            case meta::cpp2::PropertyType::INT64:
            case meta::cpp2::PropertyType::TIMESTAMP:
            case meta::cpp2::PropertyType::TIME:
            case meta::cpp2::PropertyType::VID:
            case meta::cpp2::PropertyType::FLOAT:
            case meta::cpp2::PropertyType::DOUBLE:
            case meta::cpp2::PropertyType::STRING:
            case meta::cpp2::PropertyType::DATE:
            case meta::cpp2::PropertyType::DATETIME:
            case meta::cpp2::PropertyType::UNKNOWN:
                return meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(this->type);
            case meta::cpp2::PropertyType::FIXED_STRING: {
                std::string str;
                str += meta::cpp2::_PropertyType_VALUES_TO_NAMES.at(this->type);
                str += "(";
                str += folly::to<std::string>(this->typeLen);
                str += ")";
                return str;
            }
        }
        return "";
    }

    meta::cpp2::PropertyType  type;
    int16_t                   typeLen;
};
}  // namespace nebula
#endif  // PARSER_COLUMNTYPEDEF_H_
