/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef DATAMAN_SCHEMAPROVIDERIF_H_
#define DATAMAN_SCHEMAPROVIDERIF_H_

#include "base/Base.h"
#include "interface/gen-cpp2/storage_types.h"

namespace vesoft {
namespace vgraph {

class SchemaProviderIf {
public:
    // This is an interface class
    class Field {
    public:
        virtual ~Field() = default;

        virtual const char* getName() const = 0;
        virtual const cpp2::ValueType* getType() const = 0;
        virtual bool isValid() const = 0;
    };

    // Inherited classes do no tneed to implement the Iterator
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
        const int32_t schemaVer_;
        int32_t numFields_;
        int32_t index_;
        std::unique_ptr<Field> field_;

        Iterator(const SchemaProviderIf* schema, int32_t ver, int32_t idx = 0);
    };

public:
    virtual ~SchemaProviderIf() = default;

    virtual int32_t getLatestVer() const noexcept = 0;
    virtual int32_t getNumFields(int32_t ver) const noexcept = 0;

    virtual int32_t getFieldIndex(const folly::StringPiece name, int32_t ver)
        const = 0;
    virtual const char* getFieldName(int32_t index, int32_t ver) const = 0;


    virtual const cpp2::ValueType* getFieldType(int32_t index,
                                                int32_t ver) const = 0;
    virtual const cpp2::ValueType* getFieldType(const folly::StringPiece name,
                                                int32_t ver) const = 0;

    virtual std::unique_ptr<Field> field(int32_t index, int32_t ver) const = 0;
    virtual std::unique_ptr<Field> field(const folly::StringPiece name,
                                         int32_t ver) const = 0;

    Iterator begin(int32_t ver) const;
    Iterator end(int32_t ver) const;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // DATAMAN_SCHEMAPROVIDERIF_H_
