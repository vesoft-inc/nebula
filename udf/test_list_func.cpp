#include "test_list_func.h"
#include "src/common/datatypes/List.h"

extern "C" GraphFunction *create() {
    return new test_list_func;
};
extern "C" void destroy(GraphFunction *function) {
    delete function;
};

char *test_list_func::name() {
    const char *name = "test_list_func2";
    return const_cast<char *>(name);
}


std::vector<std::vector<nebula::Value::Type>> test_list_func::inputType() {
    std::vector<nebula::Value::Type> vtp = {
            nebula::Value::Type::INT
    };
    std::vector<nebula::Value::Type> vtp2 = {
            nebula::Value::Type::STRING
    };
    std::vector<std::vector<nebula::Value::Type>> vvtp = {
            vtp, vtp2
    };
    return vvtp;
}

nebula::Value::Type test_list_func::returnType() {
    return nebula::Value::Type::LIST;
}

size_t test_list_func::minArity() {
    return 0;
}

size_t test_list_func::maxArity() {
    return 3;
}

bool test_list_func::isPure() {
    return true;
}

nebula::Value test_list_func::body(const std::vector<std::reference_wrapper<const nebula::Value>> &args) {
    switch (args[0].get().type()) {
        case nebula::Value::Type::NULLVALUE: {
            return nebula::Value::kNullValue;
        }
        case nebula::Value::Type::INT: {
            int64_t a = args[0].get().getInt();
            std::vector<nebula::Value> ret_values;
            ret_values.emplace_back(a);
            return nebula::List(ret_values);
        }
        case nebula::Value::Type::STRING: {
            auto a = args[0].get().getStr();
            std::vector<nebula::Value> ret_values;
            ret_values.emplace_back(a);
            return nebula::List(ret_values);
        }
        default: {
            return nebula::Value::kNullBadType;
        }
    }
}
