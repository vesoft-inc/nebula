/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "RowDumper.h"
#include "base/NebulaKeyUtils.h"
#include <iostream>

namespace nebula {

void RowDumper::dumpOneField(const cpp2::ValueType &type, const int64_t index) {
    switch (type.get_type()) {
        case cpp2::SupportedType::INT: {
            int64_t v;
            reader_->getInt<int64_t>(index, v);
            valStream_ << v;
            break;
        }
        case cpp2::SupportedType::VID: {
            int64_t v;
            reader_->getVid(index, v);
            valStream_ << v;
            break;
        }
        case cpp2::SupportedType::TIMESTAMP: {
            int64_t v;
            reader_->getTimestamp(index, v);
            valStream_ << v;
            break;
        }
        case cpp2::SupportedType::FLOAT: {
            float v;
            reader_->getFloat(index, v);
            valStream_ << v;
            break;
        }
        case cpp2::SupportedType::DOUBLE: {
            double v;
            reader_->getDouble(index, v);
            valStream_ << v;
            break;
        }
        case cpp2::SupportedType::STRING: {
            folly::StringPiece v;
            reader_->getString(index, v);
            valStream_ << v;
            break;
        }
        default: {
            LOG(INFO) << "Unsupport stats!";
            break;
        }
    }  // switch
}


void RowDumper::reset() {
    row_ = nullptr;
    schema_.reset();
    reader_.reset();

    nameStream_.str("");
    valStream_.str("");
}


void RowDumper::dumpAllFields() {
    auto fieldNum = schema_->getNumFields();
    for (decltype(fieldNum) i = 0; i < fieldNum; ++i) {
        // format name
        auto name = schema_->getFieldName(i);
        nameStream_ << ",\t";
        nameStream_ << name;

        // format value
        valStream_ << ",\t";
        auto type = schema_->getFieldType(i);
        dumpOneField(type, i);
    }
}


RowFormat RowDumper::dump() {
    dumpPrefix();

    if (schema_) {
        dumpAllFields();
    }

    LOG(INFO) << nameStream_.str();
    LOG(INFO) << valStream_.str();
    return RowFormat(nameStream_.str(), valStream_.str());
}


bool TagDumper::init(kvstore::KVIterator *iter, const GraphSpaceID &spaceId,
    SchemaManager *schemaMngPtr) {
    reset();

    row_ = iter;
    auto key = iter->key();
    auto val = iter->val();

    auto tagId = NebulaKeyUtils::getTagId(key);
    reader_ = RowReader::getTagPropReader(schemaMngPtr, val, spaceId, tagId);
    if (!reader_) {
        return false;
    }

    schema_ = schemaMngPtr->getTagSchema(spaceId, tagId, reader_->schemaVer());
    if (!schema_) {
        return false;
    }

    return true;
}


void TagDumper::dumpPrefix() {
    auto key = row_->key();

    nameStream_ << "id";
    auto srcId = NebulaKeyUtils::getVertexID(key);
    valStream_ << srcId;
}


bool EdgeDumper::init(kvstore::KVIterator *iter, const GraphSpaceID &spaceId,
    SchemaManager *schemaMngPtr) {
    reset();

    row_ = iter;
    auto key = iter->key();
    auto val = iter->val();
    if (val.empty()) {
        return true;
    }

    auto edgeType = NebulaKeyUtils::getEdgeType(key);
    edgeType = edgeType < 0 ? -edgeType : edgeType;
    reader_ = RowReader::getEdgePropReader(schemaMngPtr, val, spaceId, edgeType);
    if (!reader_) {
        return false;
    }

    schema_ = schemaMngPtr->getEdgeSchema(spaceId, edgeType, reader_->schemaVer());
    if (!schema_) {
        return false;
    }

    return true;
}


void EdgeDumper::dumpPrefix() {
    auto key = row_->key();

    nameStream_ << "id";

    auto srcId = NebulaKeyUtils::getSrcId(key);
    valStream_ << srcId;

    valStream_ << "->";

    auto dstId = NebulaKeyUtils::getDstId(key);
    valStream_ << dstId;

    valStream_ << ":";

    auto edgeType = NebulaKeyUtils::getEdgeType(key);
    valStream_ << edgeType;
}


RowDumper* RowDumperFactory::createRowDumper(kvstore::KVIterator *iter,
    const GraphSpaceID &spaceId, SchemaManager *schemaMngPtr) {
    auto key = iter->key();

    if (NebulaKeyUtils::isVertex(key)) {
        static TagDumper dumper;
        if (dumper.init(iter, spaceId, schemaMngPtr)) {
            return &dumper;
        }
    } else if (NebulaKeyUtils::isEdge(key)) {
        static EdgeDumper dumper;
        if (dumper.init(iter, spaceId, schemaMngPtr)) {
            return &dumper;
        }
    } else {
        // do nothing
        LOG(INFO) << "other key " << key;
    }

    return nullptr;
}

}  // namespace nebula
