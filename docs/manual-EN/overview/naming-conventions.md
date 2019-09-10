# Naming Conventions
Certain objects within Nebula graph, including space, tag, edge, alias, customer variables and other object names are referred as identifiers. These identifiers and properties are case sensitive, meaning for example that the property `name` means something different than the property `Name`. This section describes the rules for identifiers in Nebula Graph:
- Permitted characters in identifiers:
```
ASCII: [0-9,a-z,A-Z,_] (basic Latin letters, digits 0-9, underscore), Other punctuation characters are not supported. 
```
- All identifiers must begin with a letter of the alphabet.
- Identifiers are case sensitive.
- You cannot use a keyword (a reserved word) as an identifier.

It is recommended to follow the naming conventions described in the following table:

| Graph entity  | Recommended style                                          | Example     |
|:-:            | :-:                                                        | :-:         |
|Key words      | Upper case                                                 | SHOW SPACES |
|Vertex tags    | Upper camel case, beginning with an upper-case character   | ManageTeam  |
|Edges          | Upper snake case, beginning with an upper-case character   | Play_for    |
|Property names | Lower camel case, beginning with a lower-case character    | inService   |