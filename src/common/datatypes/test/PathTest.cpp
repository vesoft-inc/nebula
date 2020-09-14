/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/datatypes/Path.h"

namespace nebula {
TEST(Path, Reverse) {
    {
        Path path;
        path.src = Vertex("1", {});
        path.addStep(Step(Vertex("2", {}), 1, "", 0, {}));
        path.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        path.reverse();

        Path expected;
        expected.src = Vertex("3", {});
        expected.addStep(Step(Vertex("2", {}), -1, "", 0, {}));
        expected.addStep(Step(Vertex("1", {}), -1, "", 0, {}));

        EXPECT_EQ(expected, path);
    }
    {
        Path path;
        path.src = Vertex("1", {});
        path.addStep(Step(Vertex("2", {}), 1, "", 0, {}));
        path.reverse();

        Path expected;
        expected.src = Vertex("2", {});
        expected.addStep(Step(Vertex("1", {}), -1, "", 0, {}));

        EXPECT_EQ(expected, path);
    }
    {
        Path path;
        path.src = Vertex("1", {});
        path.reverse();

        Path expected;
        expected.src = Vertex("1", {});

        EXPECT_EQ(expected, path);
    }
}

TEST(Path, Base) {
    {
        Path path;
        path.src = Vertex("1", {});
        path.addStep(Step(Vertex("2", {}), 1, "", 0, {}));
        path.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        path.reverse();

        Path path2;
        path2.src = Vertex("5", {});
        path2.addStep(Step(Vertex("4", {}), 1, "", 0, {}));
        path2.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        path2.append(std::move(path));

        Path expected;
        expected.src = Vertex("5", {});
        expected.addStep(Step(Vertex("4", {}), 1, "", 0, {}));
        expected.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        expected.addStep(Step(Vertex("2", {}), -1, "", 0, {}));
        expected.addStep(Step(Vertex("1", {}), -1, "", 0, {}));

        EXPECT_EQ(expected, path2);
    }
    {
        Path path;
        path.src = Vertex("1", {});
        path.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        path.reverse();

        Path path2;
        path2.src = Vertex("5", {});
        path2.addStep(Step(Vertex("4", {}), 1, "", 0, {}));
        path2.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        path2.append(std::move(path));

        Path expected;
        expected.src = Vertex("5", {});
        expected.addStep(Step(Vertex("4", {}), 1, "", 0, {}));
        expected.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        expected.addStep(Step(Vertex("1", {}), -1, "", 0, {}));

        EXPECT_EQ(expected, path2);
    }
    {
        Path path;
        path.src = Vertex("3", {});
        path.reverse();

        Path path2;
        path2.src = Vertex("5", {});
        path2.addStep(Step(Vertex("4", {}), 1, "", 0, {}));
        path2.addStep(Step(Vertex("3", {}), 1, "", 0, {}));
        path2.append(std::move(path));

        Path expected;
        expected.src = Vertex("5", {});
        expected.addStep(Step(Vertex("4", {}), 1, "", 0, {}));
        expected.addStep(Step(Vertex("3", {}), 1, "", 0, {}));

        EXPECT_EQ(expected, path2);
    }
}
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
