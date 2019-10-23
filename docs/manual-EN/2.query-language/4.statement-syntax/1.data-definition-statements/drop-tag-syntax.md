# Drop Tag Syntax
```
DROP TAG tag_name
```

You must have the DROP privilege for the tag.

> Be careful with this statement.

A vertex can have either only one tag (types) or multiple tags (types).

In the former case, such a vertex can NOT be accessible after the statement -- which may result in DANGLING edges those are connecting with such vertex.

In the latter case, such a vertex is still accessible. But all the properties defined by this dropped tag are not accessible.

All the files and directories in the disk are NOT deleted directly. They can only be released by future operations (see TODO).
