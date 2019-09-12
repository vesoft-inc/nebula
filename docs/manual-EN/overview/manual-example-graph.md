# Example Graph

We will use the example graph below to introduce the basic concepts of the property graph:


![pict1](https://user-images.githubusercontent.com/42762957/64762315-84a57480-d570-11e9-8361-07f2c03cd28c.png)



## Vertices

Vertices are typically used to represent entities in the real world. In the preceding example, the graph contains eleven vertices.

## Tags

In **Nebula Graph**, vertex properties are clustered by **tags**. In the example above, the vertices have tags
**player** and **team**.

## Edge

Edges are used to connect vertices. Each edge usually represents a relationship or a behavior between two vertices. In
the preceding example, edges are _**serve**_ and _**like**_.

## Edge Type

Each edge is an instance of an edge type. Our example uses _**serve**_ and _**like**_ as edge types. Take edge _**serve**_ for example, **player** is the source vertex and **team** is the target vertex. We see that **player** has an outgoing edge while the **team** has an incoming edge.


## Properties

Properties are name-value pairs that are used to add qualities to vertices and edges. In our example graph, we have used the properties id, name and age on **player**, id and name on **team**, and likeness on _**like**_ edge.

## Schema

In **Nebula Graph**, schema refers to the definition of properties (name, type, etc.). Like Mysql, **Nebula Graph** is a strong typed database. The name and data type of the properties are determined before the data is written.


