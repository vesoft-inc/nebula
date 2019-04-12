/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

    void init() override;

private:
    void readOneGraphSpace(GraphSpaceID space, const Configuration& conf);

    std::shared_ptr<const SchemaProviderIf> readSchema(const folly::dynamic& fields);

    GraphSpaceID toGraphSpaceID(folly::StringPiece spaceName) override;

private:
    std::unique_ptr<MetaClient> client_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_FILEBASEDSCHEMAMANAGER_H_

