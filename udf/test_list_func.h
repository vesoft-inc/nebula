#ifndef UDF_PROJECT_TEST_LIST_FUNC_H
#define UDF_PROJECT_TEST_LIST_FUNC_H

#include "src/common/function/GraphFunction.h"

class test_list_func: public GraphFunction {
public:
    char *name() override;

    std::vector<std::vector<nebula::Value::Type>> inputType() override;

    nebula::Value::Type returnType() override;

    size_t minArity() override;

    size_t maxArity() override;

    bool isPure() override;

    nebula::Value body(const std::vector<std::reference_wrapper<const nebula::Value>> &args) override;
};


#endif //UDF_PROJECT_TEST_LIST_FUNC_H
