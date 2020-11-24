/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_ANONVARGENERATOR_H_
#define UTIL_ANONVARGENERATOR_H_

#include "common/base/Base.h"
#include "util/IdGenerator.h"
#include "context/Symbols.h"

namespace nebula {
namespace graph {
/**
 * An utility to generate an anonymous variable name.
 */
class AnonVarGenerator final {
public:
    explicit AnonVarGenerator(SymbolTable* symTable) {
        DCHECK(symTable != nullptr);
        idGen_ = std::make_unique<IdGenerator>();
        symTable_ = symTable;
    }

    std::string getVar() const {
        auto var = folly::stringPrintf("__VAR_%ld", idGen_->id());
        symTable_->newVariable(var);
        VLOG(1) << "Build anon var: " << var;
        return var;
    }

private:
    SymbolTable*                    symTable_{nullptr};
    std::unique_ptr<IdGenerator>    idGen_;
};
}  // namespace graph
}  // namespace nebula
#endif  // UTIL_ANONVARGENERATOR_H_
