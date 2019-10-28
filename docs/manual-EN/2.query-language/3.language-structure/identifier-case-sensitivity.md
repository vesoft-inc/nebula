# Identifer Case Sensitivity

## Identifiers are case-sensitive

The following statement would not work because it refers to a space both as 'my_space' and as 'MY_SPACE':

```SQL
nebula> CREATE SPACE my_space;
nebula> use MY_SPACE;  -- my_space and MY_SPACE are two different names
```

## Keywords and reserved words are case-insensitive

The following statements are equivalent:

```
nebula> show spaces;  -- show and spaces are keywords.
nebula> SHOW SPACES;
nebula> SHOW spaces;
nebula> show spaces;
```