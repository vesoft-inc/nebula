/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATAMAN_CODEC_SCHEMAPROVIDERIF_H_
#define DATAMAN_CODEC_SCHEMAPROVIDERIF_H_

#include <string>
#include <memory>
#include "base/ThriftTypes.h"
#include "datamanlite/Slice.h"

namespace nebula {
namespace dataman {
namespace codec {

enum class ValueType {
    UNKNOWN = 0,
    // Simple types
    BOOL = 1,
    INT = 2,
    VID = 3,
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,

    // Date time
    TIMESTAMP = 21,
    YEAR = 22,
    YEARMONTH = 23,
    DATE = 24,
    DATETIME = 25,

    // Graph specific
    PATH = 41,

    // Container types
    // LIST = 101,
    // SET = 102,
    // MAP = 103,      // The key type is always a STRING
    // STRUCT = 104,
};

class SchemaProviderIf {
public:
    // This is an interface class
    class Field {
    public:
        virtual ~Field() = default;

        virtual const char* getName() const = 0;
        virtual const ValueType& getType() const = 0;
        virtual bool isValid() const = 0;
        virtual bool hasDefault() const = 0;
        virtual std::string getDefaultValue() const = 0;
    };

    // Inherited classes do not need to implement the Iterator
    class Iterator final {
        friend class SchemaProviderIf;

    public:
        const Field& operator*() const {
            return *field_;
        }

        const Field* operator->() const {
            return field_.get();
        }

        Iterator& operator++() {
            if (field_) {
                index_++;
                field_ = schema_->field(index_);
            }
            return *this;
        }

        Iterator& operator+(uint16_t steps) {
            if (field_) {
                index_ += steps;
                field_ = schema_->field(index_);
            }
            return *this;
        }

        operator bool() const {
            return static_cast<bool>(field_);
        }

        bool operator==(const Iterator& rhs) const {
            return schema_ == rhs.schema_ &&
                   (index_ == rhs.index_ || (!field_ && !rhs.field_));
        }

    private:
        const SchemaProviderIf* schema_;
        size_t numFields_;
        int64_t index_;
        std::shared_ptr<const Field> field_;

    private:
        explicit Iterator(const SchemaProviderIf* schema,
                         int64_t idx = 0)
                : schema_(schema)
                , numFields_(schema_->getNumFields())
                , index_(idx) {
            field_ = schema_->field(index_);
        }
    };

public:
    virtual ~SchemaProviderIf() = default;

    virtual SchemaVer getVersion() const noexcept = 0;
    virtual size_t getNumFields() const noexcept = 0;

    virtual int64_t getFieldIndex(const Slice& name) const = 0;
    virtual const char* getFieldName(int64_t index) const = 0;

    virtual const ValueType& getFieldType(int64_t index) const = 0;
    virtual const ValueType& getFieldType(const Slice& name) const = 0;

    virtual std::shared_ptr<const Field> field(int64_t index) const = 0;
    virtual std::shared_ptr<const Field> field(const Slice& name) const = 0;

    /******************************************
     *
     * Iterator implementation
     *
     *****************************************/
    Iterator begin() const {
        return Iterator(this, 0);
    }


    Iterator end() const {
        return Iterator(this, getNumFields());
    }
};

}  // namespace codec
}  // namespace dataman
}  // namespace nebula

#endif  // DATAMAN_CODEC_SCHEMAPROVIDERIF_H_
