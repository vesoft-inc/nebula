```
USE graph_space_name
```
The USE statement tells Nebula to use the named (graph) space as the current working space. This statement requires some privileges.

The named space remains the default until the end of the session or another USE statement is issued:
```
USE space1
GO FROM 1 OVER edge1   # Traverse in graph space1
USE space2
GO FROM 1 OVER edge1   # Traverse in graph space2
USE space1; USE 
```
Make a particular space as working space DO NOT allow to access to the other spaces.  The only way to traverse in a new graph space is to switch by the USE statement.
>SPACES are FULLY ISOLATED from each other. 

