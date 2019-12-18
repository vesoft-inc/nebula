# Delete Vertex 语法

**Nebula Graph** 支持给定一个 vertex ID，删掉这个顶点和与它相关联的入边和出边，语法如下：

```ngql
DELETE VERTEX <vid>
```

系统内部会找出与这个顶点相关联的出边和入边，并将其全部删除，然后再删除点相关的信息。整个过程当前还无法保证原子性，因此当遇到失败时请重试。
