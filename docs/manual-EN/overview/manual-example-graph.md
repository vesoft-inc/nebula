# Example Graph

We will use the example graph below to introduce the basic concepts of property graph:


![pict4](https://user-images.githubusercontent.com/42762957/64775622-21740c00-d589-11e9-98ad-04e149a5c371.png)


In the preceding picture, we have a data set about the players and teams information of NBA. We can see the eleven vertices are classified to two kinds, i.e. player and name while the fifteen edges are classified to serve and like.

To better understand the elements of a graph data model, let us walk through each concept of the example graph.

## Vertices

Vertices are typically used to represent entities in the real world. In the preceding example, the graph contains eleven vertices.

![image](https://user-images.githubusercontent.com/42762957/64915700-aa649080-d79f-11e9-983b-7564d03c7eee.png)


## Tags

In **Nebula Graph**, vertex properties are clustered by **tags**. In the example above, the vertices have tags **player** and **team**.

![image](https://user-images.githubusercontent.com/42762957/64915770-9b7edd80-d7a1-11e9-8874-9d5e2deadf4d.png)


## Edge

Edges are used to connect vertices. Each edge usually represents a relationship or a behavior between two vertices. In the preceding example, edges are _**serve**_ and _**like**_.

![image](https://user-images.githubusercontent.com/42762957/64915798-455e6a00-d7a2-11e9-8944-8c04b8484d25.png)

## Edge Type

Each edge is an instance of an edge type. Our example uses _**serve**_ and _**like**_ as edge types. Take edge _**serve**_ for example, **player** is the source vertex and **team** is the target vertex. We see that **player** has an outgoing edge while the **team** has an incoming edge.


## Properties

Properties are name-value pairs that are used to add qualities to vertices and edges. In our example graph, we have used the properties id, name and age on **player**, id and name on **team**, and likeness on _**like**_ edge.

## Schema

In **Nebula Graph**, schema refers to the definition of properties (name, type, etc.). Like Mysql, **Nebula Graph** is a strong typed database. The name and data type of the properties are determined before the data is written.


