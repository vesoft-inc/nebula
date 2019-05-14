/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_FILEBASEDSCHEMAMANAGER_H_
#define META_FILEBASEDSCHEMAMANAGER_H_

#include "base/Base.h"
#include "meta/SchemaManager.h"
#include "meta/client/MetaClient.h"
#include "base/Configuration.h"

namespace nebula {
namespace meta {

class FileBasedSchemaManager final : public AdHocSchemaManager {
    friend class SchemaManager;
public:
    FileBasedSchemaManager() = default;

    void init(MetaClient *client = nullptr) override;

private:
    void readOneGraphSpace(GraphSpaceID space, const Configuration& conf);

    std::shared_ptr<const SchemaProviderIf> readSchema(const folly::dynamic& fields);

private:
    std::unique_ptr<MetaClient> client_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_FILEBASEDSCHEMAMANAGER_H_

