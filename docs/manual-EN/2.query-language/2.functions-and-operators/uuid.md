# UUID

`UUID` is used to generate the global unique identifiers.  

When the number of vertices reaches billions, using Hash Function to generate vid has a certain conflict probability. Therefore **Nebula Graph** provides UUID Function to avoid vid conflicts in a large number of vertices. UUID Function is composed of the Murmur hash function and the current timestamp (in units of seconds).

Values generated with the UUID are stored in the `Nebula Graph` Storage service in key-value mode. When called, it checks whether this key exists or conflicts. So the performance may be slower than hash.

Insert with UUID:

```ngql
-- INSERT VERTEX player (name, age) VALUES hash("n0"):("n0", 13.0)
nebula> INSERT VERTEX player (name, age) VALUES uuid("n0"):("n0", 13.0)
-- INSERT EDGE like(likeness) VALUES hash("n0") -> hash("n1"): (90.0)
nebula> INSERT EDGE like(likeness) VALUES uuid("n0") -> uuid("n1"): (90.0)
```

Fetch with UUID:

```ngql
nebula> FETCH PROP ON player uuid("n0")  YIELD player.name, player.age
nebula> FETCH PROP ON like uuid("n0") -> uuid("n1")
```

Go with UUID:

```ngql
nebula> GO FROM uuid("n0") OVER like
```
