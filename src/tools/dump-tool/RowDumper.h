/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef ROW_DUMPER_H
#define ROW_DUMPER_H

#include <iostream>
#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "dataman/RowReader.h"
#include "gen-cpp2/meta_types.h"
#include "gen-cpp2/graph_types.h"
#include "kvstore/KVIterator.h"

namespace nebula {

using nebula::GraphSpaceID;
using nebula::meta::SchemaManager;
using RowFormat = std::pair<std::string, std::string>;

class RowDumper {
public:
    RowDumper() = default;
    ~RowDumper() = default;
    virtual bool init(kvstore::KVIterator *iter, const GraphSpaceID &spaceId,
        SchemaManager *schemaMngPtr) = 0;

    RowFormat dump();

protected:
    void dumpOneField(const nebula::cpp2::ValueType &type, const int64_t index);
    void dumpAllFields();

    virtual void dumpPrefix() = 0;

protected:
    kvstore::KVIterator *row_;
    std::shared_ptr<const meta::SchemaProviderIf> schema_;
    std::unique_ptr<RowReader> reader_;

    std::ostringstream nameStream_;
    std::ostringstream valStream_;
};

class TagDumper : public RowDumper {
public:
    bool init(kvstore::KVIterator *iter, const GraphSpaceID &spaceId,
        SchemaManager *schemaMngPtr) override;

protected:
    void dumpPrefix() override;
};


class EdgeDumper : public RowDumper {
public:
    bool init(kvstore::KVIterator *iter, const GraphSpaceID &spaceId,
        SchemaManager *schemaMngPtr) override;

protected:
    void dumpPrefix() override;
};


class RowDumperFactory {
public:
    static std::shared_ptr<RowDumper> createRowDumper(kvstore::KVIterator *iter,
        const GraphSpaceID &spaceId, SchemaManager *schemaMngPtr);
};

}  // namespace nebula


#endif  // ROW_DUMPER_H
