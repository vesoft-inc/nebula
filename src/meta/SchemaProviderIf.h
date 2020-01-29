/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SCHEMAPROVIDERIF_H_
#define META_SCHEMAPROVIDERIF_H_

#include "base/Base.h"
#include "gen-cpp2/common_constants.h"
#include "base/StatusOr.h"


namespace nebula {

using CommonConstants = nebula::cpp2::common_constants;

namespace meta {

class SchemaProviderIf {
public:
    // This is an interface class
    class Field {
    public:
        virtual ~Field() = default;

        virtual const char* getName() const = 0;
        virtual const nebula::cpp2::ValueType& getType() const = 0;
        virtual bool isValid() const = 0;
        virtual bool hasDefaultValue() const = 0;
        virtual VariantType getDefaultValue() const = 0;
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

    virtual int64_t getFieldIndex(const folly::StringPiece name) const = 0;
    virtual const char* getFieldName(int64_t index) const = 0;

    virtual const nebula::cpp2::ValueType& getFieldType(int64_t index) const = 0;
    virtual const nebula::cpp2::ValueType& getFieldType(const folly::StringPiece name)
        const = 0;

    virtual std::shared_ptr<const Field> field(int64_t index) const = 0;
    virtual std::shared_ptr<const Field> field(const folly::StringPiece name) const = 0;

    virtual nebula::cpp2::Schema toSchema() const = 0;

    virtual const StatusOr<VariantType> getDefaultValue(const folly::StringPiece name) const = 0;
    virtual const StatusOr<VariantType> getDefaultValue(int64_t index) const = 0;
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


}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAPROVIDERIF_H_
