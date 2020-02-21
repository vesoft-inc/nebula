# Copy Schema Syntax

This documents shows how to copy schema from a graph space without indexes.

```ngql
COPY SCHEMA FROM <space_name> [NO INDEX]
```

`COPY SCHEMA FROM` statement copies schema from one space to another, including any default values and excluding indexes defined in the original space (if you add NO INDEX keywords).

**Note:** You must create a graph space and use it before copying schema from the original one. Please refer to the [CREATE SPACE Syntax](create-space-syntax.md) and [USE Syntax](../3.utility-statements/use-syntax.md) on detail usages.
