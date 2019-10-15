# Timestamp Type
- The supported range of timestamp type is '1970-01-01 00:00:01' UTC to '2262-04-11 23:47:16' UTC
- Supported data inserting methods
    - call function now()
    - Time string, for example: "2019-10-01 10:00:00"
    - Input the timestamp directly, namely the number of seconds from 1970-01-01 00:00:00
- Nebula converts TIMESTAMP values from the current time zone to **UTC** for storage, and back from UTC to the **current time** zone for retrieval

- The underlying storage data type is: **int64**

**Examples**

Create a tag named shcool
```
nebula> CREATE TAG school(name string , create_time timestamp);
```

Insert a vertex named "xiwang" with the foundation date "2010-09-01 08:00:00"
```
nebula> INSERT VERTEX school(name, create_time) VALUES hash("xiwang"):("xiwang", "2010-09-01 08:00:00")
```

Insert a vertex named "guangming" with the foundation date now
```
nebula> INSERT VERTEX school(name, create_time) VALUES hash("guangming"):("guangming", now())
```
