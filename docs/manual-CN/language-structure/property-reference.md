# 属性引用

`WHERE` 和 `YIELD` 可引用节点或边的属性。

## 引用点属性

### 引用起点属性

```
$^.tag_name.prop_name
```

其中符号`$ ^`用于获取起点属性，`tag_name`表示起点的`标签`，`prop_name`为指定属性的名称。
### 引用终点属性

```
$$.tag_name.prop_name
```

其中符号`$$`用于获取终点属性，`tag_name`表示终点的`标签`，`prop_name`为指定属性的名称。

### 示例

```
GO FROM 1 YIELD $^.start.name AS startName, $$.end.Age AS endAge
```

获取起点的属性名称和终点的属性年龄。

## 引用边

### 引用边属性

使用如下方式获取边属性：

```
edge_type.edge_prop
```

此处，`edge_type`为边的类型，`edge_prop`为属性，例如：

```
GO FROM 1 OVER e1 YIELD e1.prop1
```

### 引用内置属性

一条边有四种内置属性：

- _src: 边起点id
- _dst: 边终点id
- _type: 边类型
- _rank: 边权重

获取起点和终点id可通过`_src`和`_dst`获取，这在显示图路径时经常会用到。

例如：

```
GO FROM 1 OVER e1 YIELD e1._src as startVID /* which is, 1 */, e1._dst as endVID
```

该语句通过引用 `e1._src` 作为起始顶点id（当然，这是 `1` ）和` e1._dst` 作为结束顶点，返回 `1` 边类型为 `e1` 的所有邻居节点。其中 `e1._src` 返回起点ID，此处为1，`e1._dst` 返回终点ID。
