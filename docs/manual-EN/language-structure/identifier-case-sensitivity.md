In Nebula Graph, Identifiers are case-sensitive. 

The following statement would not work because it refers to a space both as 'my_space' and as 'MY_SPACE':

```
nebula> CREATE SPACE my_space;
nebula> use MY_SPACE;
```

However, keywords and reserved words are case-insensitive. 

The following statements are equivalent:
```
nebula> show spaces;
nebula> SHOW SPACES;
nebula> SHOW spaces;
nebula> show spaces;
```