/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SCHEMAPROVIDERIF_H_
#define META_SCHEMAPROVIDERIF_H_

#include "base/Base.h"
#include "gen-cpp2/storage_types.h"
#include "gen-cpp2/storage_constants.h"


namespace nebula {

using StorageConstants = nebula::storage::cpp2::storage_constants;

namespace meta {

class SchemaProviderIf {
public:
    // This is an interface class
    class Field {
    public:
        virtual ~Field() = default;

        virtual const char* getName() const = 0;
        virtual const storage::cpp2::ValueType& getType() const = 0;
        virtual bool isValid() const = 0;
    };

    // Inherited classes do not need to implement the Iterator
    class Iterator final {
        friend class SchemaProviderIf;

    public:
        const Field& operator*() const;
        const Field* operator->() const;

        Iterator& operator++();
        Iterator& operator+(uint16_t steps);

        operator bool() const;
        bool operator==(const Iterator& rhs) const;

    private:
        const SchemaProviderIf* schema_;
        size_t numFields_;
        int64_t index_;
        std::shared_ptr<const Field> field_;

        Iterator(const SchemaProviderIf* schema, int64_t idx = 0);
    };

public:
    virtual ~SchemaProviderIf() = default;

    virtual int32_t getVersion() const noexcept = 0;
    virtual size_t getNumFields() const noexcept = 0;

    virtual int64_t getFieldIndex(const folly::StringPiece name) const = 0;
    virtual const char* getFieldName(int64_t index) const = 0;

    virtual const storage::cpp2::ValueType& getFieldType(int64_t index) const = 0;
    virtual const storage::cpp2::ValueType& getFieldType(const folly::StringPiece name)
        const = 0;

    virtual std::shared_ptr<const Field> field(int64_t index) const = 0;
    virtual std::shared_ptr<const Field> field(const folly::StringPiece name) const = 0;

    Iterator begin() const;
    Iterator end() const;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_SCHEMAPROVIDERIF_H_
