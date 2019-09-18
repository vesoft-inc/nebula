/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/IndexSentences.h"

namespace nebula {

std::string CreateTagIndexSentence::toString() const {
    return folly::stringPrintf("CREATE TAG INDEX %s", indexName_.get()->c_str());
}

std::string CreateEdgeIndexSentence::toString() const {
    return folly::stringPrintf("CREATE EDGE INDEX %s", indexName_.get()->c_str());
}

std::string DropTagIndexSentence::toString() const {
    return folly::stringPrintf("DROP TAG INDEX %s", indexName_.get()->c_str());
}

std::string DropEdgeIndexSentence::toString() const {
    return folly::stringPrintf("DROP EDGE INDEX %s", indexName_.get()->c_str());
}

std::string DescribeTagIndexSentence::toString() const {
    return folly::stringPrintf("DESCRIBE TAG INDEX %s", indexName_.get()->c_str());
}

std::string DescribeEdgeIndexSentence::toString() const {
    return folly::stringPrintf("DESCRIBE EDGE INDEX %s", indexName_.get()->c_str());
}

std::string RebuildTagIndexSentence::toString() const {
    return folly::stringPrintf("REBUILD TAG INDEX %s", indexName_.get()->c_str());
}

std::string RebuildEdgeIndexSentence::toString() const {
    return folly::stringPrintf("REBUILD EDGE INDEX %s", indexName_.get()->c_str());
}

}   // namespace nebula

