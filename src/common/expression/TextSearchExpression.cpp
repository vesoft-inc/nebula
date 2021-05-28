/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/TextSearchExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

bool TextSearchArgument::operator==(const TextSearchArgument& rhs) const {
    return val_ == rhs.val_ &&
           op_ == rhs.op_ &&
           fuzziness_ == rhs.fuzziness_ &&
           limit_ == rhs.limit_ &&
           timeout_ == rhs.timeout_;
}

std::string TextSearchArgument::toString() const {
    std::string buf;
    buf.reserve(64);
    buf = from_ + "." + prop_ + ", ";
    buf += "\"" + val_ + "\"";
    if (fuzziness_ == -1) {
        buf += ", AUTO, ";
        buf += ((op_ == "or") ? "OR" : "AND");
    } else if (fuzziness_ > -1) {
        buf += ", ";
        buf += folly::stringPrintf("%d, ", fuzziness_);
        buf += ((op_ == "or") ? "OR" : "AND");
    }
    if (limit_ != -1) {
        buf += folly::stringPrintf(", %d", limit_);
    }
    if (timeout_ != -1) {
        buf += folly::stringPrintf(", %d", timeout_);
    }
    return buf;
}

bool TextSearchExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = dynamic_cast<const TextSearchExpression&>(rhs);
    return arg_ == r.arg_;
}

std::string TextSearchExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    switch (kind_) {
        case Kind::kTSWildcard: {
            buf = "WILDCARD(";
            break;
        }
        case Kind::kTSPrefix: {
            buf = "PREFIX(";
            break;
        }
        case Kind::kTSFuzzy: {
            buf = "FUZZY(";
            break;
        }
        case Kind::kTSRegexp: {
            buf = "REGEXP(";
            break;
        }
        default: {
            LOG(FATAL) << "Unimplemented";
        }
    }
    buf += arg_->toString();
    buf += ")";
    return buf;
}
}  // namespace nebula
