/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_STORAGE_CLIENT_VALUE_H_
#define NEBULA_STORAGE_CLIENT_VALUE_H_
namespace nebula {
namespace storage {
namespace client {
namespace value {
struct Value {
    enum class Type : uint8_t {
        NULLVALUE = 0,
        BOOL = 1,
        INT = 2,
        DOUBLE = 3,
        STRING = 4,
    };

    // Constructors
    Value() : type_(Type::NULLVALUE) {}

    Value(Value&& rhs) {
        clear();
        switch (rhs.type_) {
            case Type::NULLVALUE :
            {
                break;
            }
            case Type::BOOL :
            {
                type_ = Type::BOOL;
                new (std::addressof(value_.bVal)) bool(std::move(rhs).value_.bVal);       // NOLINT
                break;
            }
            case Type::INT :
            {
                type_ = Type::INT;
                new (std::addressof(value_.iVal)) int64_t(std::move(rhs).value_.iVal);   // NOLINT
                break;
            }
            case Type::DOUBLE :
            {
                type_ = Type::DOUBLE;
                new (std::addressof(value_.dVal)) double(std::move(rhs).value_.dVal);    // NOLINT
                break;
            }
            case Type::STRING :
            {
                type_ = Type::STRING;
                new (std::addressof(value_.sVal)) std::string(std::move(rhs).value_.sVal); // NOLINT
                break;
            }
        }
    }

    Value(const Value& rhs) {
        clear();
        switch (rhs.type_) {
            case Type::NULLVALUE :
            {
                break;
            }
            case Type::BOOL :
            {
                type_ = Type::BOOL;
                new (std::addressof(value_.bVal)) bool(rhs.value_.bVal);                 // NOLINT
                break;
            }
            case Type::INT :
            {
                type_ = Type::INT;
                new (std::addressof(value_.iVal)) int64_t(rhs.value_.iVal);             // NOLINT
                break;
            }
            case Type::DOUBLE :
            {
                type_ = Type::DOUBLE;
                new (std::addressof(value_.dVal)) double(rhs.value_.dVal);             // NOLINT
                break;
            }
            case Type::STRING :
            {
                type_ = Type::STRING;
                new (std::addressof(value_.sVal)) std::string(rhs.value_.sVal);       // NOLINT
                break;
            }
        }
    }

    explicit Value(const bool& v) {
        type_ = Type::BOOL;
        new (std::addressof(value_.bVal)) bool(v);                  // NOLINT
    }

    explicit Value(bool&& v) {
        type_ = Type::BOOL;
        new (std::addressof(value_.bVal)) bool(std::move(v));       // NOLINT
    }

    explicit Value(const int8_t& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(v);               // NOLINT
    }

    explicit Value(int8_t&& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(std::move(v));    // NOLINT
    }

    explicit Value(const int16_t& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(v);               // NOLINT
    }

    explicit Value(int16_t&& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(std::move(v));    // NOLINT
    }

    explicit Value(const int32_t& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(std::move(v));    // NOLINT
    }

    explicit Value(int32_t&& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(std::move(v));    // NOLINT
    }

    explicit Value(const int64_t& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(v);               // NOLINT
    }

    explicit Value(int64_t&& v) {
        type_ = Type::INT;
        new (std::addressof(value_.iVal)) int64_t(std::move(v));    // NOLINT
    }

    explicit Value(const double& v) {
        type_ = Type::DOUBLE;
        new (std::addressof(value_.dVal)) double(v);                // NOLINT
    }

    explicit Value(double&& v) {
        type_ = Type::DOUBLE;
        new (std::addressof(value_.dVal)) double(std::move(v));     // NOLINT
    }

    explicit Value(const std::string& v) {
        type_ = Type::STRING;
        new (std::addressof(value_.sVal)) std::string(v);
    }

    explicit Value(std::string&& v) {
        type_ = Type::STRING;
        new (std::addressof(value_.sVal)) std::string(std::move(v));
    }

    explicit Value(const char* v) {
        type_ = Type::STRING;
        new (std::addressof(value_.sVal)) std::string(v);
    }

    ~Value() { clear(); }

    Type type() const noexcept {
        return type_;
    }

    bool isNull() const {
        return type_ == Type::NULLVALUE;
    }
    bool isNumeric() const {
        return type_ == Type::INT || type_ == Type::DOUBLE;
    }
    bool isBool() const {
        return type_ == Type::BOOL;
    }
    bool isInt() const {
        return type_ == Type::INT;
    }
    bool isFloat() const {
        return type_ == Type::DOUBLE;
    }
    bool isStr() const {
        return type_ == Type::STRING;
    }

    const bool& getBool() const {
        return value_.bVal;
    }

    const int64_t& getInt() const {
        return value_.iVal;
    }

    const double& getDouble() const {
        return value_.dVal;
    }

    const std::string& getStr() const {
        return value_.sVal;
    }

    void clear() {
        switch (type_) {
            case Type::BOOL:
            {
                destruct(value_.bVal);
                break;
            }
            case Type::INT:
            {
                destruct(value_.iVal);
                break;
            }
            case Type::DOUBLE:
            {
                destruct(value_.dVal);
                break;
            }
            case Type::STRING:
            {
                destruct(value_.sVal);
                break;
            }
            default:
                break;
        }
        type_ = Type::NULLVALUE;
    }

    Value& operator=(Value&& rhs) {
        if (this == &rhs) { return *this; }
        clear();
        switch (rhs.type_) {
            case Type::NULLVALUE :
            {
                type_ = Type::NULLVALUE;
                break;
            }
            case Type::BOOL :
            {
                type_ = Type::BOOL;
                new (std::addressof(value_.bVal)) bool(std::move(rhs).value_.bVal);      // NOLINT
                break;
            }
            case Type::INT :
            {
                type_ = Type::INT;
                new (std::addressof(value_.iVal)) int64_t(std::move(rhs).value_.iVal);  // NOLINT
                break;
            }
            case Type::DOUBLE :
            {
                type_ = Type::DOUBLE;
                new (std::addressof(value_.dVal)) double(std::move(rhs).value_.dVal);  // NOLINT
                break;
            }
            case Type::STRING :
            {
                type_ = Type::STRING;
                new (std::addressof(value_.sVal)) std::string(std::move(rhs).value_.sVal); // NOLINT
                break;
            }
        }
        rhs.clear();
        return *this;
    }

    Value& operator=(const Value& rhs) {
        if (this == &rhs) { return *this; }
        clear();
        switch (rhs.type_) {
            case Type::NULLVALUE :
            {
                type_ = Type::NULLVALUE;
                break;
            }
            case Type::BOOL :
            {
                type_ = Type::BOOL;
                new (std::addressof(value_.bVal)) bool(rhs.value_.bVal);        // NOLINT
                break;
            }
            case Type::INT :
            {
                type_ = Type::INT;
                new (std::addressof(value_.iVal)) int64_t(rhs.value_.iVal);    // NOLINT
                break;
            }
            case Type::DOUBLE :
            {
                type_ = Type::DOUBLE;
                new (std::addressof(value_.dVal)) double(rhs.value_.dVal);    // NOLINT
                break;
            }
            case Type::STRING :
            {
                type_ = Type::STRING;
                new (std::addressof(value_.sVal)) std::string(rhs.value_.sVal);   // NOLINT
                break;
            }
        }
        return *this;
    }

private:
    Type type_;

    union Storage {
        bool                        bVal;
        int64_t                     iVal;
        double                      dVal;
        std::string                 sVal;

        Storage() {}
        ~Storage() {}
    } value_;

    template <class T>
    void destruct(T& val) {
        (&val)->~T();
    }
};

}   // namespace value
}   // namespace client
}   // namespace storage
}   // namespace nebula

#endif   // NEBULA_STORAGE_CLIENT_VALUE_H_
