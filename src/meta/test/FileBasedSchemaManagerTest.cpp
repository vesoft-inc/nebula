/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "meta/FileBasedSchemaManager.h"
#include "fs/TempFile.h"

DECLARE_string(schema_file);

namespace nebula {
namespace meta {

void prepareSchemaFile() {
    static fs::TempFile schemaFile("/tmp/file_based_schema_manager.XXXXXX");
    std::ofstream fs(schemaFile.path());
    fs << "{"
       << "  \"space_one\" : {"
       << "    \"edges\" : {"
       << "      \"friend\" : ["
       << "        ["
       << "          \"__version : 1\","
       << "          \"time : date\","
       << "          \"origin : vertexid\","
       << "          \"pending : bool\""
       << "        ]"
       << "      ],"
       << "      \"transfer\" : ["
       << "        ["
       << "          \"__version : 3\","
       << "          \"time : date\","
       << "          \"from : vertexid\","
       << "          \"to : vertexid\","
       << "          \"amount : integer\""
       << "        ]"
       << "      ]"
       << "    },"
       << "    \"tags\" : {"
       << "      \"person\" : ["
       << "        ["
       << "          \"__version : 1\","
       << "          \"name : string\","
       << "          \"birthday : date\","
       << "          \"employer : string\""
       << "        ],"
       << "        ["
       << "          \"__version : 2\","
       << "          \"name : string\","
       << "          \"birthday : date\","
       << "          \"employer : string\","
       << "          \"address : string\""
       << "        ]"
       << "      ]"
       << "    }"
       << "  },"
       << "  \"space_two\" : {"
       << "  }"
       << "}";
    fs.close();

    FLAGS_schema_file = schemaFile.path();
}


TEST(FileBasedSchemaManager, readFromSchemaFile) {
    auto schemaMan = std::make_unique<FileBasedSchemaManager>();
    schemaMan->init();
    EXPECT_EQ(1, schemaMan->getNewestEdgeSchemaVer("space_one", "friend"));
    EXPECT_EQ(3, schemaMan->getNewestEdgeSchemaVer("space_one", "transfer"));

    auto sm1 = schemaMan->getEdgeSchema("space_one", "friend");
    EXPECT_EQ(1, sm1->getVersion());
    EXPECT_EQ(3, sm1->getNumFields());
    EXPECT_EQ(2, sm1->getFieldIndex("pending"));
    EXPECT_EQ(nebula::cpp2::SupportedType::VID, sm1->getFieldType("origin").type);

    auto sm2 = schemaMan->getEdgeSchema("space_one", "transfer");
    EXPECT_EQ(3, sm2->getVersion());
    EXPECT_EQ(4, sm2->getNumFields());
    EXPECT_EQ(3, sm2->getFieldIndex("amount"));
    EXPECT_EQ(nebula::cpp2::SupportedType::DATE, sm2->getFieldType("time").type);

    EXPECT_EQ(2, schemaMan->getNewestTagSchemaVer("space_one", "person"));

    auto sm3 = schemaMan->getTagSchema("space_one", "person");
    EXPECT_EQ(2, sm3->getVersion());
    EXPECT_EQ(4, sm3->getNumFields());
    EXPECT_EQ(3, sm3->getFieldIndex("address"));
    EXPECT_EQ(nebula::cpp2::SupportedType::STRING, sm3->getFieldType("address").type);

    auto sm4 = schemaMan->getTagSchema("space_one", "person", 1);
    EXPECT_EQ(1, sm4->getVersion());
    EXPECT_EQ(3, sm4->getNumFields());
    EXPECT_EQ(2, sm4->getFieldIndex("employer"));
    EXPECT_EQ(nebula::cpp2::SupportedType::STRING, sm4->getFieldType("employer").type);

    auto status1 = schemaMan->checkSpaceExist("space_one");
    EXPECT_TRUE(status1.ok());

    auto status2 = schemaMan->checkSpaceExist("myspace");
    EXPECT_FALSE(status2.ok());
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    nebula::meta::prepareSchemaFile();

    return RUN_ALL_TESTS();
}


