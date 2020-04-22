nebula_add_library(
    expression_obj OBJECT
    Expression.cpp
    ConstantExpression.cpp
    ArithmeticExpression.cpp
    UnaryExpression.cpp
    RelationalExpression.cpp
    LogicalExpression.cpp
    TypeCastingExpression.cpp
    FunctionCallExpression.cpp
    AliasPropertyExpression.cpp
    UUIDExpression.cpp
)

#nebula_add_subdirectory(test)


