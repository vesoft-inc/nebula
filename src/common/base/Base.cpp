/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"

namespace nebula {

VariantType variantLT(VariantType& lhs, VariantType& rhs) {
    if (lhs.which() != rhs.which()) {
        return NullValue(NullValue::NullType::NT_BadType);
    }

    switch (lhs.which()) {
        case 0:
            return boost::get<int64_t>(lhs) < boost::get<int64_t>(rhs);
        case 1:
            return boost::get<double>(lhs) < boost::get<double>(rhs);
        case 2:
            return boost::get<bool>(lhs) < boost::get<bool>(rhs);
        case 3:
            return boost::get<std::string>(lhs) < boost::get<std::string>(rhs);
        case 4:
            return NullValue();
    }

    LOG(FATAL) << "Should not reach here";
}


VariantType variantLE(VariantType& lhs, VariantType& rhs) {
    if (lhs.which() != rhs.which()) {
        return NullValue(NullValue::NullType::NT_BadType);
    }

    switch (lhs.which()) {
        case 0:
            return boost::get<int64_t>(lhs) <= boost::get<int64_t>(rhs);
        case 1:
            return boost::get<double>(lhs) <= boost::get<double>(rhs);
        case 2:
            return boost::get<bool>(lhs) <= boost::get<bool>(rhs);
        case 3:
            return boost::get<std::string>(lhs) <= boost::get<std::string>(rhs);
        case 4:
            return NullValue();
    }

    LOG(FATAL) << "Should not reach here";
}


VariantType variantGT(VariantType& lhs, VariantType& rhs) {
    if (lhs.which() != rhs.which()) {
        return NullValue(NullValue::NullType::NT_BadType);
    }

    switch (lhs.which()) {
        case 0:
            return boost::get<int64_t>(lhs) > boost::get<int64_t>(rhs);
        case 1:
            return boost::get<double>(lhs) > boost::get<double>(rhs);
        case 2:
            return boost::get<bool>(lhs) > boost::get<bool>(rhs);
        case 3:
            return boost::get<std::string>(lhs) > boost::get<std::string>(rhs);
        case 4:
            return NullValue();
    }

    LOG(FATAL) << "Should not reach here";
}


VariantType variantGE(VariantType& lhs, VariantType& rhs) {
    if (lhs.which() != rhs.which()) {
        return NullValue(NullValue::NullType::NT_BadType);
    }

    switch (lhs.which()) {
        case 0:
            return boost::get<int64_t>(lhs) >= boost::get<int64_t>(rhs);
        case 1:
            return boost::get<double>(lhs) >= boost::get<double>(rhs);
        case 2:
            return boost::get<bool>(lhs) >= boost::get<bool>(rhs);
        case 3:
            return boost::get<std::string>(lhs) >= boost::get<std::string>(rhs);
        case 4:
            return NullValue();
    }

    LOG(FATAL) << "Should not reach here";
}


VariantType variantEQ(VariantType& lhs, VariantType& rhs) {
    if (lhs.which() != rhs.which()) {
        return NullValue(NullValue::NullType::NT_BadType);
    }

    switch (lhs.which()) {
        case 0:
            return boost::get<int64_t>(lhs) == boost::get<int64_t>(rhs);
        case 1:
            return boost::get<double>(lhs) == boost::get<double>(rhs);
        case 2:
            return boost::get<bool>(lhs) == boost::get<bool>(rhs);
        case 3:
            return boost::get<std::string>(lhs) == boost::get<std::string>(rhs);
        case 4:
            return NullValue();
    }

    LOG(FATAL) << "Should not reach here";
}


VariantType variantNE(VariantType& lhs, VariantType& rhs) {
    if (lhs.which() != rhs.which()) {
        return NullValue(NullValue::NullType::NT_BadType);
    }

    switch (lhs.which()) {
        case 0:
            return boost::get<int64_t>(lhs) != boost::get<int64_t>(rhs);
        case 1:
            return boost::get<double>(lhs) != boost::get<double>(rhs);
        case 2:
            return boost::get<bool>(lhs) != boost::get<bool>(rhs);
        case 3:
            return boost::get<std::string>(lhs) != boost::get<std::string>(rhs);
        case 4:
            return NullValue();
    }

    LOG(FATAL) << "Should not reach here";
}


bool variantBoolLT(VariantType& lhs, VariantType& rhs) {
    auto r = variantLT(lhs, rhs);
    if (r.which() == 4) {
        // Null
        return false;
    }

    return boost::get<bool>(r);
}


bool variantBoolLE(VariantType& lhs, VariantType& rhs) {
    auto r = variantLE(lhs, rhs);
    if (r.which() == 4) {
        // Null
        return false;
    }

    return boost::get<bool>(r);
}


bool variantBoolGT(VariantType& lhs, VariantType& rhs) {
    auto r = variantGT(lhs, rhs);
    if (r.which() == 4) {
        // Null
        return false;
    }

    return boost::get<bool>(r);
}


bool variantBoolGE(VariantType& lhs, VariantType& rhs) {
    auto r = variantGE(lhs, rhs);
    if (r.which() == 4) {
        // Null
        return false;
    }

    return boost::get<bool>(r);
}


bool variantBoolEQ(VariantType& lhs, VariantType& rhs) {
    auto r = variantEQ(lhs, rhs);
    if (r.which() == 4) {
        // Null
        return false;
    }

    return boost::get<bool>(r);
}


bool variantBoolNE(VariantType& lhs, VariantType& rhs) {
    auto r = variantNE(lhs, rhs);
    if (r.which() == 4) {
        // Null
        return false;
    }

    return boost::get<bool>(r);
}


std::ostream& operator <<(std::ostream &os, const HostAddr &addr) {
    uint32_t ip = addr.first;
    uint32_t port = addr.second;
    os << folly::stringPrintf("[%u.%u.%u.%u:%u]",
                              (ip >> 24) & 0xFF,
                              (ip >> 16) & 0xFF,
                              (ip >> 8) & 0xFF,
                              ip & 0xFF, port);
    return os;
}


std::string versionString() {
    std::string version;
#if defined(NEBULA_BUILD_VERSION)
    version = folly::stringPrintf("%s, ", NEBULA_STRINGIFY(NEBULA_BUILD_VERSION));
#endif

#if defined(GIT_INFO_SHA)
    version += folly::stringPrintf("Git: %s", NEBULA_STRINGIFY(GIT_INFO_SHA));
#endif
    version += folly::stringPrintf(", Build Time: %s %s", __DATE__, __TIME__);
    version += "\nThis source code is licensed under Apache 2.0 License,"
               " attached with Common Clause Condition 1.0.";
    return version;
}

}   // namespace nebula
