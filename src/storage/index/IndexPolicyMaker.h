/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INDEXPOLICYMAKER_H
#define STORAGE_INDEXPOLICYMAKER_H
#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "storage/CommonUtils.h"
#include "storage/index/IndexOptimizer.h"

namespace nebula {
namespace storage {

enum class PolicyType : uint8_t {
    SIMPLE_POLICY      = 0x01,
    OPTIMIZED_POLICY   = 0x02,
};

enum class PolicyScanType : uint8_t {
    SEEK_SCAN          = 0x01,
    PREFIX_SCAN        = 0x02,
    ACCURATE_SCAN      = 0x03,
};

using OperatorList = std::vector<std::pair<std::pair<std::string, VariantType>, bool>>;

class IndexPolicyMaker : IndexOptimizer{
public:
    virtual ~IndexPolicyMaker() = default;

protected:
    explicit IndexPolicyMaker(meta::SchemaManager *schemaMan)
        : IndexOptimizer()
        , schemaMan_(schemaMan) {}

    cpp2::ErrorCode policyPrepare(const std::string& filter);

    cpp2::ErrorCode policyGenerate();

private:
    cpp2::ErrorCode initPolicy();

    std::string prepareAPE(const Expression* expr);

    StatusOr<VariantType> preparePE(const Expression* expr);

    cpp2::ErrorCode prepareRFE(const Expression* expr);

    cpp2::ErrorCode prepareLE(const LogicalExpression* expr);

    cpp2::ErrorCode prepareExpression(const Expression* expr);

protected:
    std::unique_ptr<Expression> expr_{nullptr};
    meta::SchemaManager*        schemaMan_{nullptr};
    nebula::cpp2::IndexItem     index_;
    std::vector<VariantType>    policies_;
    PolicyType                  policyType_{PolicyType::OPTIMIZED_POLICY};
    PolicyScanType              policyScanType_{PolicyScanType::SEEK_SCAN};
    OperatorList                operatorList_;
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_INDEXPOLICYMAKER_H
