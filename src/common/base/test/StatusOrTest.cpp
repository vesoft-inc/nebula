/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/base/StatusOr.h"


namespace nebula {

TEST(StatusOr, ConstructFromDefault) {
    StatusOr<std::string> result;
    ASSERT_FALSE(result.ok());
}


TEST(StatusOr, ConstructFromStatus) {
    {
        StatusOr<std::string> result(Status::OK());
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().ok());
        ASSERT_EQ("OK", result.status().toString());
    }
    {
        StatusOr<std::string> result(Status::Error("SomeError"));
        ASSERT_FALSE(result.ok());
        ASSERT_FALSE(result.status().ok());
        ASSERT_EQ("SomeError", result.status().toString());
    }
}


TEST(StatusOr, ConstructFromValue) {
    StatusOr<std::string> result("SomeValue");
    ASSERT_TRUE(result.ok());
    ASSERT_EQ("SomeValue", result.value());
}


TEST(StatusOr, ReturnFromStatus) {
    {
        auto foo = [] () -> StatusOr<std::string> {
            return Status::Error("SomeError");
        };

        auto result = foo();
        ASSERT_FALSE(result.ok());
        ASSERT_FALSE(result.status().ok());
        ASSERT_EQ("SomeError", result.status().toString());
    }
    {
        auto foo = [] () -> StatusOr<std::string> {
            auto status = Status::Error("SomeError");
            return status;
        };

        auto result = foo();
        ASSERT_FALSE(result.ok());
        ASSERT_FALSE(result.status().ok());
        ASSERT_EQ("SomeError", result.status().toString());
    }
}


TEST(StatusOr, ReturnFromValue) {
    auto foo = [] () -> StatusOr<std::string> {
        return "SomeValue";
    };
    auto result = foo();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ("SomeValue", result.value());
}


TEST(StatusOr, ReturnFromMoveOnlyValue) {
    // return move only from anonymous value
    {
        auto foo = [] () -> StatusOr<std::unique_ptr<std::string>> {
            return std::make_unique<std::string>("SomeValue");
        };
        auto result = foo();
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeValue", *result.value());
    }
    // return move only from named value
    {
        auto foo = [] () -> StatusOr<std::unique_ptr<std::string>> {
            // TODO(dutor) It seems that GCC before 8.0 has some issues to compile
            // the following code, in the combination of NVRO and implicit conversion.
            // We use `std::move' explicitly for now
            auto ptr = std::make_unique<std::string>("SomeValue");
            return ptr;
        };
        auto result = foo();
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeValue", *result.value());
    }

    /*
    // return move only from compatible named value
    {
        auto foo = [] () -> StatusOr<std::shared_ptr<std::string>> {
            auto ptr = std::make_unique<std::string>("SomeValue");
            // TODO(dutor) It seems that GCC before 8.0 has some issues to compile
            // the following code, in the combination of NVRO and implicit conversion.
            // We use `std::move' explicitly for now
            return std::move(ptr);
        };
        auto result = foo();
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeValue", *result.value());
    }
    */
}


TEST(StatusOr, CopyConstructFromDefault) {
    StatusOr<std::string> result1;
    auto result2 = result1;
    ASSERT_FALSE(result2.ok());
}


TEST(StatusOr, CopyConstructFromStatus) {
    StatusOr<std::string> result1(Status::Error("SomeError"));
    auto result2 = result1;
    ASSERT_FALSE(result1.ok());
    ASSERT_FALSE(result1.status().ok());
    ASSERT_FALSE(result2.ok());
    ASSERT_FALSE(result2.status().ok());
    ASSERT_EQ("SomeError", result1.status().toString());
    ASSERT_EQ("SomeError", result2.status().toString());
}


TEST(StatusOr, CopyConstructFromValue) {
    StatusOr<std::string> result1("SomeValue");
    auto result2 = result1;
    ASSERT_TRUE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_EQ("SomeValue", result1.value());
    ASSERT_EQ("SomeValue", result2.value());
}


TEST(StatusOr, CopyAssignFromDefault) {
    StatusOr<std::string> result1;
    decltype(result1) result2;
    result2 = result1;
    ASSERT_FALSE(result2.ok());
}


TEST(StatusOr, CopyAssignFromStatus) {
    StatusOr<std::string> result1(Status::Error("SomeError"));
    decltype(result1) result2;
    result2 = result1;
    ASSERT_FALSE(result1.ok());
    ASSERT_FALSE(result1.status().ok());
    ASSERT_FALSE(result2.ok());
    ASSERT_FALSE(result2.status().ok());
    ASSERT_EQ("SomeError", result1.status().toString());
    ASSERT_EQ("SomeError", result2.status().toString());
}


TEST(StatusOr, CopyAssignFromValue) {
    StatusOr<std::string> result1("SomeValue");
    decltype(result1) result2;
    result2 = result1;
    ASSERT_TRUE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_EQ("SomeValue", result1.value());
    ASSERT_EQ("SomeValue", result2.value());
}


TEST(StatusOr, MoveConstructFromDefault) {
    StatusOr<std::string> result1;
    auto result2 = std::move(result1);
    ASSERT_FALSE(result2.ok());
}


TEST(StatusOr, MoveConstructFromStatus) {
    StatusOr<std::string> result1(Status::Error("SomeError"));
    auto result2 = std::move(result1);
    ASSERT_FALSE(result1.ok());
    ASSERT_FALSE(result2.ok());
    ASSERT_FALSE(result2.status().ok());
    ASSERT_EQ("SomeError", result2.status().toString());
}


TEST(StatusOr, MoveConstructFromValue) {
    StatusOr<std::string> result1("SomeValue");
    auto result2 = std::move(result1);
    ASSERT_FALSE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_EQ("SomeValue", result2.value());
}


TEST(StatusOr, MoveAssignFromDefault) {
    StatusOr<std::string> result1;
    decltype(result1) result2;
    result2 = std::move(result1);
    ASSERT_FALSE(result2.ok());
}


TEST(StatusOr, MoveAssignFromStatus) {
    StatusOr<std::string> result1(Status::Error("SomeError"));
    decltype(result1) result2;
    result2 = std::move(result1);
    ASSERT_FALSE(result1.ok());
    ASSERT_FALSE(result2.ok());
    ASSERT_FALSE(result2.status().ok());
    ASSERT_EQ("SomeError", result2.status().toString());
}


TEST(StatusOr, MoveAssignFromValue) {
    StatusOr<std::string> result1("SomeValue");
    decltype(result1) result2;
    result2 = std::move(result1);
    ASSERT_FALSE(result1.ok());
    ASSERT_TRUE(result2.ok());
    ASSERT_EQ("SomeValue", result2.value());
}


TEST(StatusOr, AssignFromStatus) {
    {
        StatusOr<std::string> result;
        result = Status::OK();
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().ok());

        result = Status::Error("SomeError");
        ASSERT_FALSE(result.ok());
        ASSERT_FALSE(result.status().ok());
        ASSERT_EQ("SomeError", result.status().toString());
    }
    {
        StatusOr<std::string> result;
        Status status = Status::OK();
        result = status;
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().ok());

        status = Status::Error("SomeError");
        result = status;
        ASSERT_FALSE(result.ok());
        ASSERT_FALSE(result.status().ok());
        ASSERT_EQ("SomeError", result.status().toString());
    }
    {
        StatusOr<std::string> result;
        Status status = Status::OK();
        result = std::move(status);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().ok());

        status = Status::Error("SomeError");
        result = std::move(status);
        ASSERT_TRUE(status.ok());
        ASSERT_FALSE(result.ok());
        ASSERT_FALSE(result.status().ok());
        ASSERT_EQ("SomeError", result.status().toString());
    }
}


TEST(StatusOr, AssignFromValue) {
    {
        StatusOr<std::string> result;
        result = "SomeValue";
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeValue", result.value());

        result = "SomeOtherValue";
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeOtherValue", result.value());
    }
    {
        StatusOr<std::string> result;
        std::string value = "SomeValue";
        result = value;
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeValue", result.value());

        value = "SomeOtherValue";
        result = value;
        ASSERT_TRUE(result.ok());
        ASSERT_EQ("SomeOtherValue", result.value());
    }
}


TEST(StatusOr, CopyConstructFromCompatibleTypes) {
    {
        StatusOr<const char*> from("SomeValue");
        StatusOr<std::string> to(from);
        ASSERT_TRUE(to.ok());
        ASSERT_EQ("SomeValue", to.value());
    }
    {
        {
            StatusOr<int> from(0);
            ASSERT_EQ(0, from.value());
            StatusOr<bool> to(from);
            ASSERT_TRUE(to.ok());
            ASSERT_FALSE(to.value());
        }
        {
            StatusOr<int> from(123);
            StatusOr<bool> to(from);
            ASSERT_TRUE(to.ok());
            ASSERT_TRUE(to.value());
        }
    }
}


TEST(StatusOr, CopyAssignFromCompatibleTypes) {
    {
        StatusOr<const char*> from("SomeValue");
        StatusOr<std::string> to;
        to = from;
        ASSERT_TRUE(to.ok());
        ASSERT_EQ("SomeValue", to.value());
    }
    {
        StatusOr<int> from(123);
        StatusOr<bool> to(false);
        ASSERT_TRUE(to.ok());
        ASSERT_FALSE(to.value());
        to = from;
        ASSERT_TRUE(to.ok());
        ASSERT_TRUE(to.value());
    }
}


TEST(StatusOr, MoveConstructFromCompatibleTypes) {
    StatusOr<std::unique_ptr<std::string>> from(std::make_unique<std::string>("SomeValue"));
    ASSERT_TRUE(from.ok());
    ASSERT_EQ("SomeValue", *from.value());

    StatusOr<std::shared_ptr<std::string>> to(std::move(from));
    ASSERT_FALSE(from.ok());
    ASSERT_TRUE(to.ok());
    ASSERT_EQ("SomeValue", *to.value());
}


TEST(StatusOr, MoveAssignFromCompatibleTypes) {
    StatusOr<std::unique_ptr<std::string>> from(std::make_unique<std::string>("SomeValue"));
    ASSERT_TRUE(from.ok());
    ASSERT_EQ("SomeValue", *from.value());

    StatusOr<std::shared_ptr<std::string>> to(std::make_shared<std::string>("SomeOtherValue"));
    ASSERT_TRUE(to.ok());
    ASSERT_EQ("SomeOtherValue", *to.value());

    to = std::move(from);
    ASSERT_FALSE(from.ok());
    ASSERT_TRUE(to.ok());
    ASSERT_EQ("SomeValue", *to.value());
}


TEST(StatusOr, MoveOnlyValue) {
    {
        StatusOr<std::unique_ptr<std::string>> result1(std::make_unique<std::string>("SomeValue"));
        auto result2 = std::move(result1);
        ASSERT_FALSE(result1.ok());
        ASSERT_TRUE(result2.ok());
        auto ptr = std::move(result2).value();
        ASSERT_NE(nullptr, ptr.get());
        ASSERT_EQ("SomeValue", *ptr);
        ASSERT_FALSE(result2.ok());
    }
    {
        StatusOr<std::unique_ptr<std::string>> result1(std::make_unique<std::string>("SomeValue"));
        decltype(result1) result2;
        result2 = std::move(result1);
        ASSERT_FALSE(result1.ok());
        ASSERT_TRUE(result2.ok());
        auto ptr = std::move(result2).value();
        ASSERT_NE(nullptr, ptr.get());
        ASSERT_EQ("SomeValue", *ptr);
        ASSERT_FALSE(result2.ok());
    }
}


TEST(StatusOr, MoveOutStatus) {
    StatusOr<std::string> result(Status::Error("SomeError"));
    ASSERT_FALSE(result.ok());
    ASSERT_FALSE(result.status().ok());
    ASSERT_EQ(Status::Error("SomeError"), result.status());
    auto status = std::move(result).status();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(Status::Error("SomeError"), status);
}


TEST(StatusOr, Destruct) {
    auto ptr = std::make_shared<std::string>("SomeValue");
    ASSERT_EQ(1UL, ptr.use_count());
    {
        StatusOr<std::shared_ptr<std::string>> result;
        result = ptr;
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(2UL, ptr.use_count());
        auto ptr2 = result.value();
        ASSERT_EQ(3UL, ptr.use_count());
    }
    ASSERT_EQ(1UL, ptr.use_count());
}


/*
TEST(StatusOr, ConstructibleButNotConvertible) {
    struct ConstructibleButNotConvertible {
        explicit ConstructibleButNotConvertible(double) {}
    };
    StatusOr<ConstructibleButNotConvertible> x(1.);
}


TEST(StatusOr, ConvertibleButNotConstructible) {
    struct ConvertibleButNotConstructible {
        explicit ConvertibleButNotConstructible(double) = delete;
        ConvertibleButNotConstructible(int) {}
    };
    StatusOr<ConvertibleButNotConstructible> x(1.);
}
*/

TEST(StatusOr, ReturnIfError) {
    auto value = []() -> StatusOr<int> { return 1; };
    auto error = []() -> StatusOr<int> { return Status::Error("error"); };
    auto returnOk = [=]() {
        NG_RETURN_IF_ERROR(value());
        return Status::OK();
    };
    auto returnError = [=]() {
        NG_RETURN_IF_ERROR(error());
        return Status::OK();
    };
    auto returnError2 = [=]() {
        auto err = error();
        NG_RETURN_IF_ERROR(err);
        return Status::OK();
    };
    EXPECT_TRUE(returnOk().ok());
    EXPECT_FALSE(returnError().ok());
    EXPECT_FALSE(returnError2().ok());
}

}   // namespace nebula
