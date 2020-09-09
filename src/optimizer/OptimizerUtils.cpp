/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptimizerUtils.h"
namespace nebula {
namespace graph {

Value OptimizerUtils::boundValue(const meta::cpp2::ColumnDef& col,
                                 BoundValueOperator op,
                                 const Value& v) {
    switch (op) {
        case BoundValueOperator::GREATER_THAN : {
            return boundValueWithGT(col, v);
        }
        case BoundValueOperator::LESS_THAN : {
            return boundValueWithLT(col, v);
        }
        case BoundValueOperator::MAX : {
            return boundValueWithMax(col, v);
        }
        case BoundValueOperator::MIN : {
            return boundValueWithMin(col, v);
        }
    }
    return Value(NullType::BAD_TYPE);
}

Value OptimizerUtils::boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            if (v.getInt() == std::numeric_limits<int64_t>::max()) {
                return v;
            } else {
                return v + 1;
            }
        }
        case Value::Type::FLOAT : {
            if (v.getFloat() > 0.0) {
                if (v.getFloat() == std::numeric_limits<double_t>::max()) {
                    return v;
                }
            } else if (v.getFloat() == 0.0) {
                return Value(std::numeric_limits<double_t>::min());
            } else {
                if (v.getFloat() == -std::numeric_limits<double_t>::min()) {
                    return Value(0.0);
                }
            }
            return v.getFloat() + 0.0000000000000001;
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return Value(NullType::BAD_TYPE);
            }
            std::vector<unsigned char> bytes(v.getStr().begin(), v.getStr().end());
            bytes.resize(*col.get_type_length());
            for (size_t i = bytes.size();; i--) {
                if (i > 0) {
                    if (bytes[i-1]++ != 255) break;
                } else {
                    return Value(std::string(*col.get_type_length(), '\377'));
                }
            }
            return Value(std::string(bytes.begin(), bytes.end()));
        }
        case Value::Type::DATE : {
            if (Date(std::numeric_limits<int16_t>::max(), 12, 31) == v.getDate()) {
                return v.getDate();
            } else if (Date() == v.getDate()) {
                return Date(0, 1, 2);
            }
            auto d = v.getDate();
            if (d.day < 31) {
                d.day += 1;
            } else {
                d.day = 1;
                if (d.month < 12) {
                    d.month += 1;
                } else {
                    d.month = 1;
                    if (d.year < std::numeric_limits<int16_t>::max()) {
                        d.year += 1;
                    } else {
                        return v.getDate();
                    }
                }
            }
            return Value(d);
        }
        case Value::Type::DATETIME : {
            auto dt = v.getDateTime();
            // Ignore the time zone.
            if (dt.microsec < std::numeric_limits<int32_t>::max()) {
                dt.microsec = dt.microsec + 1;
            } else {
                dt.microsec = 1;
                if (dt.sec < 60) {
                    dt.sec += 1;
                } else {
                    dt.sec = 1;
                    if (dt.minute < 60) {
                        dt.minute += 1;
                    } else {
                        dt.minute = 1;
                        if (dt.hour < 24) {
                            dt.hour += 1;
                        } else {
                            dt.hour = 1;
                            if (dt.day < 31) {
                                dt.day += 1;
                            } else {
                                dt.day = 1;
                                if (dt.month < 12) {
                                    dt.month += 1;
                                } else {
                                    dt.month = 1;
                                    if (dt.year < std::numeric_limits<int16_t>::max()) {
                                        dt.year += 1;
                                    } else {
                                        return v.getDateTime();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Value(dt);
        }
        default : {
            return Value(NullType::BAD_TYPE);
        }
    }
}

Value OptimizerUtils::boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            if (v.getInt() == std::numeric_limits<int64_t>::min()) {
                return v;
            } else {
                return v - 1;
            }
        }
        case Value::Type::FLOAT : {
            if (v.getFloat() < 0.0) {
                if (v.getFloat() == -std::numeric_limits<double_t>::max()) {
                    return v;
                } else if (v.getFloat() == -std::numeric_limits<double_t>::min()) {
                    return Value(0.0);
                }
            } else if (v.getFloat() == 0.0) {
                return Value(-std::numeric_limits<double_t>::min());
            }
            return v.getFloat() - 0.0000000000000001;
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return Value(NullType::BAD_TYPE);
            }
            std::vector<unsigned char> bytes(v.getStr().begin(), v.getStr().end());
            bytes.resize(*col.get_type_length());
            for (size_t i = bytes.size();; i--) {
                if (i > 0) {
                    if (bytes[i-1]-- != 0) break;
                } else {
                    return Value(std::string(*col.get_type_length(), '\0'));
                }
            }
            return Value(std::string(bytes.begin(), bytes.end()));
        }
        case Value::Type::DATE : {
            if (Date() == v.getDate()) {
                return v.getDate();
            }
            auto d = v.getDate();
            if (d.day > 1) {
                d.day -= 1;
            } else {
                d.day = 31;
                if (d.month > 1) {
                    d.month -= 1;
                } else {
                    d.month = 12;
                    if (d.year > 1) {
                        d.year -= 1;
                    } else {
                        return v.getDate();
                    }
                }
            }
            return Value(d);
        }
        case Value::Type::DATETIME : {
            if (DateTime() == v.getDateTime()) {
                return v.getDateTime();
            }
            auto dt = v.getDateTime();
            if (dt.microsec > 1) {
                dt.microsec -= 1;
            } else {
                dt.microsec = std::numeric_limits<int32_t>::max();
                if (dt.sec > 1) {
                    dt.sec -= 1;
                } else {
                    dt.sec = 60;
                    if (dt.minute > 1) {
                        dt.minute -= 1;
                    } else {
                        dt.minute = 60;
                        if (dt.hour > 1) {
                            dt.hour -= 1;
                        } else {
                            dt.hour = 24;
                            if (dt.day > 1) {
                                dt.day -= 1;
                            } else {
                                dt.day = 31;
                                if (dt.month > 1) {
                                    dt.month -= 1;
                                } else {
                                    dt.month = 12;
                                    if (dt.year > 1) {
                                        dt.year -= 1;
                                    } else {
                                        return v.getDateTime();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Value(dt);
        }
        default :
            return Value(NullType::BAD_TYPE);
    }
}

Value OptimizerUtils::boundValueWithMax(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            return Value(std::numeric_limits<int64_t>::max());
        }
        case Value::Type::FLOAT : {
            return Value(std::numeric_limits<double>::max());
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return Value(NullType::BAD_TYPE);
            }
            return Value(std::string(*col.get_type_length(), '\377'));
        }
        case Value::Type::DATE : {
            Date d;
            d.year = std::numeric_limits<int16_t>::max();
            d.month = 12;
            d.day = 31;
            return Value(d);
        }
        case Value::Type::DATETIME : {
            DateTime dt;
            dt.year = std::numeric_limits<int16_t>::max();
            dt.month = 12;
            dt.day = 31;
            dt.hour = 24;
            dt.minute = 60;
            dt.sec = 60;
            dt.microsec = std::numeric_limits<int32_t>::max();
            dt.timezone = std::numeric_limits<int32_t>::max();
            return Value(dt);
        }
        default :
            return Value(NullType::BAD_TYPE);
    }
}

Value OptimizerUtils::boundValueWithMin(const meta::cpp2::ColumnDef& col, const Value& v) {
    auto type = SchemaUtil::propTypeToValueType(col.get_type());
    switch (type) {
        case Value::Type::INT : {
            return Value(std::numeric_limits<int64_t>::min());
        }
        case Value::Type::FLOAT : {
            return Value(-std::numeric_limits<double>::max());
        }
        case Value::Type::BOOL: {
            return v;
        }
        case Value::Type::STRING : {
            if (!col.__isset.type_length || col.get_type_length() == nullptr) {
                return Value(NullType::BAD_TYPE);
            }
            return Value(std::string(*col.get_type_length(), '\0'));
        }
        case Value::Type::DATE : {
            return Value(Date());
        }
        case Value::Type::DATETIME : {
            return Value(DateTime());
        }
        default :
            return Value(NullType::BAD_TYPE);
    }
}

}  // namespace graph
}  // namespace nebula
