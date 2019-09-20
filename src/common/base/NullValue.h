/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_NULLVALUE_H_
#define COMMON_BASE_NULLVALUE_H_

#include "base/Base.h"

namespace nebula {

/**
 * This is a class to represent an abnormal value. By default, it stands for
 * a Null value. But it could represent other abnormal values, such as MaM
 */
class NullValue final {
public:
    enum class NullType : uint8_t {
        NT_Null = 0,
        NT_NaN = 1,
        NT_BadType = 2
    };

    explicit NullValue(NullType type = NullType::NT_Null) : type_(type) {}

    NullType type() const {
        return type_;
    }

#define CHECK_NULL_TYPE_FUNC(NTYPE) \
    inline bool is ## NTYPE() const { \
        return type_ == NullType::NT_ ## NTYPE; \
    }

    CHECK_NULL_TYPE_FUNC(Null)      // bool isNull() const
    CHECK_NULL_TYPE_FUNC(NaN)       // bool isNaN() const
    CHECK_NULL_TYPE_FUNC(BadType)   // bool isBadType() const

#undef CHECK_NULL_TYPE_FUNC

private:
    NullType type_;
};

}  // namespace nebula

namespace std {

std::ostream& operator<<(std::ostream& os, const nebula::NullValue& v);

}  // namespace std

#endif  // COMMON_BASE_NULLVALUE_H_

