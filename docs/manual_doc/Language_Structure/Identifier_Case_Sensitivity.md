In Nebula Graph, Identifiers are case-sensitive. 

The following statement would not work because it refers to a space both as 'my_space' and as 'MY_SPACE':

```
(user@127.0.0.1) [(none)]> CREATE SPACE my_space();
(user@127.0.0.1) [(none)]> use MY_SPACE;
```

However, keywords and reserved words are case-insensitive. 

The following statements are equivalent:
```
(user@127.0.0.1) [(none)]> show spaces;
(user@127.0.0.1) [(none)]> SHOW SPACES;
(user@127.0.0.1) [(none)]> SHOW spaces;
(user@127.0.0.1) [(none)]> show spaces;
```