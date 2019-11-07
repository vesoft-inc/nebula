# Drop Tag Syntax

```ngql
DROP TAG <tag_name>
```

You must have the DROP privilege for the tag.

> Be careful with this statement.

A vertex can have either only one tag (types) or multiple tags (types).

In the former case, such a vertex can NOT be accessible after the statement, and edges connected with such vertex may result in DANGLING.

In the latter case, the dropped a vertex is still accessible. But all the properties defined by this dropped tag are not accessible.

This operation only deletes the Schema data, all the files and directories in the disk are NOT deleted directly, data is deleted in the next compaction.
