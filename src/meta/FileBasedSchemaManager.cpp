/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/FileBasedSchemaManager.h"
#include <folly/Hash.h>

DEFINE_string(schema_file, "", "The full path of the schema file");

namespace nebula {
namespace meta {

// static
void FileBasedSchemaManager::init() {
    DCHECK(!FLAGS_schema_file.empty()) << "Schema file is required";

    Configuration conf;
    auto status = conf.parseFromFile(FLAGS_schema_file);
    CHECK(status.ok()) << status;

    CHECK(conf.forEachKey([&conf](const std::string& name) {
        auto space = toGraphSpaceID(name);
        Configuration spaceConf;
        CHECK(conf.fetchAsSubConf(name.c_str(), spaceConf).ok());
        readOneGraphSpace(space, spaceConf);
    }).ok());
}


// static
void FileBasedSchemaManager::readOneGraphSpace(GraphSpaceID space, const Configuration& conf) {
    Configuration edgeConf;
    if (conf.fetchAsSubConf("edges", edgeConf).ok()) {
        CHECK(edgeConf.forEachItem([space] (const std::string& name,
                                             const folly::dynamic& versions) {
            CHECK(versions.isArray());
            auto type = toEdgeType(name);
            for (auto& edge : versions) {
                auto schema = readSchema(edge);
                if (!schema) {
                    // Failed to read the schema
                    LOG(ERROR) << "Failed to read the edge schema " << edge;
                } else {
                    edgeSchemas_[std::make_pair(space, type)][schema->getVersion()] = schema;
                }
            }
        }).ok());
    }

    Configuration tagConf;
    if (conf.fetchAsSubConf("tags", tagConf).ok()) {
        CHECK(tagConf.forEachItem([space] (const std::string& name,
                                            const folly::dynamic& versions) {
            CHECK(versions.isArray());
            auto id = toTagID(name);
            for (auto& tag : versions) {
                auto schema = readSchema(tag);
                if (!schema) {
                    // Failed to read the schema
                    LOG(ERROR) << "Failed to read the tag schema " << tag;
                } else {
                    tagSchemas_[std::make_pair(space, id)][schema->getVersion()] = schema;
                }
            }
        }).ok());
    }
}


// static
std::shared_ptr<const SchemaProviderIf> FileBasedSchemaManager::readSchema(
        const folly::dynamic& fields) {
    CHECK(fields.isArray());

    std::shared_ptr<SchemaManager> schema(new SchemaManager());
    for (auto& item : fields) {
        CHECK(item.isString());

        std::vector<std::string> parts;
        folly::split(':', item.getString(), parts, true);
        CHECK_EQ(parts.size(), 2);

        folly::StringPiece fname = folly::trimWhitespace(parts[0]);
        folly::StringPiece type = folly::trimWhitespace(parts[1]);

        if (fname == "__version") {
            try {
                schema->ver_ = folly::to<int32_t>(type);
                if (schema->ver_ < 0) {
                    LOG(ERROR) << "Invalid schema version: " << schema->ver_;
                    return std::shared_ptr<SchemaProviderIf>();
                }
            } catch (const std::exception& ex) {
                LOG(ERROR) << "Failed to read the version value: " << ex.what();
                return std::shared_ptr<SchemaProviderIf>();
            }
        } else {
            cpp2::ValueType vtype;
            if (type == "bool") {
                vtype.set_type(cpp2::SupportedType::BOOL);
            } else if (type == "integer") {
                vtype.set_type(cpp2::SupportedType::INT);
            } else if (type == "vertexid") {
                vtype.set_type(cpp2::SupportedType::VID);
            } else if (type == "float") {
                vtype.set_type(cpp2::SupportedType::FLOAT);
            } else if (type == "double") {
                vtype.set_type(cpp2::SupportedType::DOUBLE);
            } else if (type == "string") {
                vtype.set_type(cpp2::SupportedType::STRING);
            } else if (type == "timestamp") {
                vtype.set_type(cpp2::SupportedType::TIMESTAMP);
            } else if (type == "year") {
                vtype.set_type(cpp2::SupportedType::YEAR);
            } else if (type == "yearmonth") {
                vtype.set_type(cpp2::SupportedType::YEARMONTH);
            } else if (type == "date") {
                vtype.set_type(cpp2::SupportedType::DATE);
            } else if (type == "datetime") {
                vtype.set_type(cpp2::SupportedType::DATETIME);
            } else if (type == "path") {
                vtype.set_type(cpp2::SupportedType::PATH);
            } else {
                LOG(ERROR) << "Unsupported type: \"" << type << "\"";
                return std::shared_ptr<SchemaProviderIf>();
            }

            schema->fields_.push_back(
                std::make_shared<SchemaManager::SchemaField>(fname.toString(),
                                                             std::move(vtype)));
            schema->fieldNameIndex_.emplace(fname.toString(),
                                            static_cast<int64_t>(schema->fields_.size() - 1));
        }
    }

    return std::static_pointer_cast<const SchemaProviderIf>(schema);
}


// static
GraphSpaceID FileBasedSchemaManager::toGraphSpaceID(const folly::StringPiece spaceName) {
    UNUSED(spaceName);
    // TODO(dutor) Fix this once the server based schema manager is merged in
    return 1;
    // return folly::hash::fnv32_buf(spaceName.start(), spaceName.size());
}


// static
TagID FileBasedSchemaManager::toTagID(const folly::StringPiece tagName) {
    return folly::hash::fnv32_buf(tagName.start(), tagName.size());
}


// static
EdgeType FileBasedSchemaManager::toEdgeType(const folly::StringPiece typeName) {
    return folly::hash::fnv32_buf(typeName.start(), typeName.size());
}

}  // namespace meta
}  // namespace nebula

