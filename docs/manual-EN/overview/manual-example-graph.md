# Example Graph

We will use the example graph below to introduce the basic concepts of property graph:


![map300](https://user-images.githubusercontent.com/42762957/64932536-51b1f800-d872-11e9-9016-c2634b1eeed6.png)


In the preceding picture, we have a data set about the players and teams information of NBA. We can see the eleven vertices are classified to two kinds, i.e. player and name while the fifteen edges are classified to serve and like.

To better understand the elements of a graph data model, let us walk through each concept of the example graph.



## Vertices

Vertices are typically used to represent entities in the real world. In the preceding example, the graph contains eleven vertices.


<img src="https://user-images.githubusercontent.com/42762957/64932628-00eecf00-d873-11e9-844b-6b2a535ca734.png" width="10%" height="20%">


## Tags

In **Nebula Graph**, vertex properties are clustered by **tags**. In the example above, the vertices have tags **player** and **team**.


<img src="https://user-images.githubusercontent.com/42762957/64932330-bff5bb00-d870-11e9-9940-4ff76ceca353.png" width="35%" height="20%">


## Edge

Edges are used to connect vertices. Each edge usually represents a relationship or a behavior between two vertices. In the preceding example, edges are _**serve**_ and _**like**_.

<img src="https://user-images.githubusercontent.com/42762957/64932285-68efe600-d870-11e9-8dc7-051f7b43c4aa.png" width="35%" height="20%">

## Edge Type

Each edge is an instance of an edge type. Our example uses _**serve**_ and _**like**_ as edge types. Take edge _**serve**_ for example, in the preceding picture, vertex 101 (represents a **player**) is the source vertex and vertex 215 (represents a **team**) is the target vertex. We see that vertex 101 has an outgoing edge while vertex 215 has an incoming edge.



## Properties

Properties are name-value pairs that are used to add qualities to vertices and edges. In our example graph, we have used the properties id, name and age on **player**, id and name on **team**, and likeness on _**like**_ edge.

## Schema

In **Nebula Graph**, schema refers to the definition of properties (name, type, etc.). Like Mysql, **Nebula Graph** is a strong typed database. The name and data type of the properties are determined before the data is written.


