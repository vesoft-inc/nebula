# Keywords and Reserved Words

Keywords are words that have significance in nGQL. Certain keywords are reserved and require special treatment for use as identifiers.

Non-reserved keywords are permitted as identifiers without quoting. Reserved words are permitted as identifiers if you quote them with single or double quotes such as `'AND'`.

```ngql
nebula> CREATE TAG TAG(name string);
[ERROR (-7)]: SyntaxError: syntax error near `TAG'

nebula> CREATE TAG SPACE(name string);
Execution succeeded
```

`TAG` is a reserved keyword and must be quoted to be used as an identifier. `SPACE` is keyword but not reserved, so its use as identifiers does not require quoting.

```ngql
nebula> CREATE TAG 'TAG' (name string);
Execution succeeded
```

> All the non-reserved keywords are automatically converted to lower case.

## Reserved Words

The following list shows reserved words in nGQL.

```ngql
GO
AS
TO
OR
AND
XOR
USE
SET
FROM
WHERE
MATCH
INSERT
YIELD
RETURN
DESCRIBE
DESC
VERTEX
EDGE
EDGES
UPDATE
UPSERT
WHEN
DELETE
FIND
LOOKUP
ALTER
STEPS
OVER
UPTO
REVERSELY
INDEX
INDEXES
REBUILD
INT
BIGINT
DOUBLE
STRING
BOOL
TIMESTAMP
TAG
TAGS
UNION
INTERSECT
MINUS
NO
OVERWRITE
SHOW
ADD
CREATE
DROP
REMOVE
IF
NOT
EXISTS
WITH
CHANGE
GRANT
REVOKE
ON
BY
IN
DOWNLOAD
GET
OF
ORDER
INGEST
COMPACT
FLUSH
SUBMIT
ASC
DISTINCT
FETCH
PROP
BALANCE
STOP
LIMIT
OFFSET
GROUP
IS
NULL
FORCE
RECOVER
```

## Non-Reserved Keywords

```ngql
SPACE
SPACES
VALUES
HOSTS
USER
USERS
PASSWORD
ROLE
ROLES
GOD
ADMIN
DBA
GUEST
GROUP
COUNT
SUM
AVG
MAX
MIN
STD
BIT_AND
BIT_OR
BIT_XOR
COUNT
COUNT_DISTINCT
PATH
DATA
LEADER
UUID
VARIABLES
JOB
JOBS
SUBMIT
RECOVER
FLUSH
COMPACT
BIDIRECT
OFFLINE
FORCE
STATUS
REBUILD
PART
PARTS
DEFAULT
CONFIGS
ACCOUNT
HDFS
PHONE
EMAIL
FIRSTNAME
LASTNAME
OFFLINE
PARTITION_NUM
REPLICA_FACTOR
CHARSET
COLLATE
COLLATION
TTL_DURATION
TTL_COL
SNAPSHOT
SNAPSHOTS
OFFLINE
BIDIRECT
GRAPH
META
STORAGE
ALL
SHORTEST
```
