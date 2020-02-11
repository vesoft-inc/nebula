# 属性引用

`WHERE` 和 `YIELD` 可引用节点或边的属性。

## 引用点的属性

### 引用起点的属性

```ngql
$^.tag_name.prop_name
```

其中符号 `$^` 用于获取起点属性，`tag_name` 表示起点的`标签`，`prop_name` 为指定属性的名称。

### 引用终点的属性

```ngql
$$.tag_name.prop_name
```

其中符号 `$$` 用于获取终点属性，`tag_name` 表示终点的 `标签`，`prop_name` 为指定属性的名称。

### 示例

```ngql
nebula> GO FROM 100 OVER follow YIELD $^.player.name AS StartName, $$.player.age AS EndAge;
```

该语句用于获取起点的属性名称和终点的属性年龄。

## 引用边

### 引用边的属性

使用如下方式获取边属性：

```ngql
edge_type.edge_prop
```

此处，`edge_type`为边的类型，`edge_prop`为属性，例如：

```ngql
nebula> GO FROM 100 OVER follow YIELD follow.degree;
```

### 引用边的内置属性

一条边有四个内置属性：

- _src: 边起点 ID
- _dst: 边终点 ID
- _type: 边类型
- _rank: 边的权重

获取起点和终点 ID 可通过 `_src` 和 `_dst` 获取，这在显示图路径时经常会用到。

例如：

```ngql
nebula> GO FROM 100 OVER follow YIELD follow._src AS startVID /* 起点为100 */, follow._dst AS endVID;
```

该语句通过引用 `follow._src` 作为起点 ID 和 `follow._dst` 作为终点 ID，返回起点 100 `follow` 的所有邻居节点。其中 `follow._src` 返回起点 ID，`follow._dst` 返回终点 ID。
