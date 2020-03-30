# Keywords and Reserved Words

Keywords are words that have significance in nGQL. Certain keywords are reserved and require special treatment for use as identifiers.

Non-reserved keywords are permitted as identifiers without quoting. All the non-reserved keywords are automatically converted to lower case. Reserved words are permitted as identifiers if you quote them with single or double quotes such as \`AND\`.

```ngql
nebula> CREATE TAG TAG(name string);
[ERROR (-7)]: SyntaxError: syntax error near `TAG'

nebula> CREATE TAG SPACE(name string); -- SPACE is an unreserved KEY WORD
Execution succeeded

nebula> SHOW TAGS; -- All the non-reserved keywords are automatically converted to lower case.
=============
| ID | Name |
=============
| 25 | space|
-------------
```

`TAG` is a reserved keyword and must be quoted with backtick to be used as an identifier. `SPACE` is keyword but not reserved, so its use as identifiers does not require quoting.

```ngql
nebula> CREATE TAG `TAG` (name string); -- TAG is a reserved word here
Execution succeeded
```

## Reserved Words

The following list shows reserved words in nGQL.

```ngql
ADD
ALTER
AND
AS
ASC
BALANCE
BIGINT
BOOL
BY
CHANGE
COMPACT
CREATE
DELETE
DESC
DESCRIBE
DISTINCT
DOUBLE
DOWNLOAD
DROP
EDGE
EDGES
EXISTS
FETCH
FIND
FLUSH
FROM
GET
GO
GRANT
IF
IN
INDEX
INDEXES
INGEST
INSERT
INT
INTERSECT
IS
LIMIT
LOOKUP
MATCH
MINUS
NO
NOT
NULL
OF
OFFSET
ON
OR
ORDER
OVER
OVERWRITE
PROP
REBUILD
RECOVER
REMOVE
RETURN
REVERSELY
REVOKE
SET
SHOW
STEPS
STOP
STRING
SUBMIT
TAG
TAGS
TIMESTAMP
TO
UNION
UPDATE
UPSERT
UPTO
USE
VERTEX
WHEN
WHERE
WITH
XOR
YIELD
```

## Non-Reserved Keywords

```ngql
ACCOUNT
ADMIN
ALL
AVG
BIDIRECT
BIT_AND
BIT_OR
BIT_XOR
CHARSET
COLLATE
COLLATION
CONFIGS
COUNT
COUNT_DISTINCT
DATA
DBA
DEFAULT
FORCE
GOD
GRAPH
GROUP
GUEST
HDFS
HOSTS
JOB
JOBS
LEADER
MAX
META
MIN
OFFLINE
PART
PARTITION_NUM
PARTS
PASSWORD
PATH
REPLICA_FACTOR
ROLE
ROLES
SHORTEST
SNAPSHOT
SNAPSHOTS
SPACE
SPACES
STATUS
STD
STORAGE
SUM
TTL_COL
TTL_DURATION
USER
USERS
UUID
VALUES
```
