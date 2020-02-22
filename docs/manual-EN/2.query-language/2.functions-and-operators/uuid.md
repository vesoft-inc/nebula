# UUID

`UUID` is used to generate the global unique identifiers.  

When the number of vertices reaches billions, using `Hash` Function to generate vids has a certain conflict probability. Therefore, **Nebula Graph** provides `UUID` Function to avoid vid conflicts in a large number of vertices. `UUID` Function is composed of the `Murmur` hash function and the current timestamp (in seconds).

Values generated with the `UUID` are stored in the **Nebula Graph** Storage service in key-value mode. When called, it checks whether this key exists or conflicts. So the performance may be slower than hash.

Insert with `UUID`:

```ngql
-- Insert a vertex with the UUID function.
nebula> INSERT VERTEX player (name, age) VALUES uuid("n0"):("n0", 21);
-- Insert an edge with the UUID function.
nebula> INSERT EDGE follow(degree) VALUES uuid("n0") -> uuid("n1"): (90);
```

Fetch with `UUID`:

```ngql
nebula> FETCH PROP ON player uuid("n0") YIELD player.name, player.age;
-- The following result is returned:
===================================================
| VertexID             | player.name | player.age |
===================================================
| -5057115778034027261 | n0          | 21         |
---------------------------------------------------
nebula> FETCH PROP ON follow uuid("n0") -> uuid("n1");
-- The following result is returned:
=============================================================================
| follow._src          | follow._dst         | follow._rank | follow.degree |
=============================================================================
| -5057115778034027261 | 4039977434270020867 | 0            | 90            |
-----------------------------------------------------------------------------
```

`Go` with `UUID`:

```ngql
nebula> GO FROM uuid("n0") OVER follow;
--The following result is returned:
=======================
| follow._dst         |
=======================
| 4039977434270020867 |
-----------------------
```
