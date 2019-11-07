# Identifer Case Sensitivity

## Identifiers are case-sensitive

The following statements would not work because they refer to two different spaces, i.e. `my_space` and `MY_SPACE`:

```ngql
nebula> CREATE SPACE my_space;
nebula> use MY_SPACE;  -- my_space and MY_SPACE are two different spaces
```

## Keywords and reserved words are case-insensitive

The following statements are equivalent:

```ngql
nebula> show spaces;  -- show and spaces are keywords.
nebula> SHOW SPACES;
nebula> SHOW spaces;
nebula> show SPACES;
```
