## Get Started

---

This tutorial provides a quick introduction to use `Nebula Graph`.

---

### Step 1 Install Nebula Graph

The easiest way to startup `nebula` is using `docker`.
`Docker` is a tool designed to make it easier to create, deploy, and run applications by using containers.
Containers allow a developer to package up an application with all of the parts it needs,
such as libraries and other dependencies, and ship it all out as one package.
By doing so, the developer can rest assured that the application will run on any other `Linux` machine regardless of any customized settings that machine might have that could differ from the machine used for writing and testing the code.

First of all, you should make sure `docker` have installed on your machine. Open a terminal and run the following command :

```
docker --version
```

If `docker` is not found, please see [here](https://docs.docker.com/install/) for more information to install docker.

After that, using `docker pull vesoft/nebula-graph:latest` to get `nebula` image and `docker images` that can display images status.

---

### Step 2 Startup Nebula Graph

When `nebula` image is ready, run

`docker run -it vesoft/nebula-graph:latest /bin/bash`

to start and login the docker container.
After login in the container, you're in the `root` directory and you should use `cd ~/nebula-graph/` to switch to the nebula home directory.

Run `./start-all.sh` to start meta service, storage service and graph service.

Run

`ps -ef | grep nebula`

to display the servie's running status.

Please make sure the services are working.

`bin/nebula` is a `console` which can be used to insert and query data.

`--port` is used for specifying the graph server port and the default value is `3699`.

`--u` and `--p` are used to specify the user name and password, `user` and `password` are the default authority.
 
Run `bin/nebula --port=3699 --u=user --p=password` to connect to the graph server.

One easier way to start console is to run

`./start-console.sh`.

```
Welcome to Nebula Graph (Version 0.1)

nebula>
```

Before query the dataset, you should switch to an existed graph space.

```
nebula> use space nba
Execution succeeded (Time spent: 154/793 us)
```

---

### Step 3 Simple Query Demo

This query comes from a vertex walk up an edge.

```
nebula> go from 5209979940224249985 over like
========================
|                   id |
========================
| -7187791973189815797 |
------------------------
|  3778194419743477824 |
------------------------
| -6952676908621237908 |
------------------------
........................
```

This query comes from a vertex walk up an edge and the target's age should be greater than or equal to 30, also renames the columns as `Player`, `Friend` and `Age`.

```
nebula> go from 5209979940224249985 over like where $$[player].age >= 30 YIELD $^[player].name as Player, $$[player].name as Friend, $$[player].age As Age
=============================================
|          Player |            Friend | Age |
=============================================
| Dejounte Murray |      Kevin Durant |  30 |
---------------------------------------------
| Dejounte Murray |        Chris Paul |  33 |
---------------------------------------------
| Dejounte Murray |      LeBron James |  34 |
---------------------------------------------
.............................................
```

This query comes from a vertex walk up an edge and the target's age should be greater than or equal to 30.

Setting output result as input, query `Player Name`, `FromYear`, `ToYear` and `Team Name` with input's `id`.

In this query, `$^` is delegated the source vertex and `$$` is the target vertex. 

`$-` is used to indicate the input from pipeline.

(reference to `regular expressions`, using the special ^ (hat) and $ (dollar sign) metacharacters to describe the start and the end of the line.)

```
nebula> go from 5209979940224249985 over like where $$[player].age >= 30 | go from $-.id over serve yield $^[player].name As Player, serve.start_year as FromYear, serve.end_year as ToYear, $$[team].name as Team
=====================================================
|            Player | FromYear | ToYear |      Team |
=====================================================
|      Kevin Durant |     2016 |   2019 |  Warriors |
-----------------------------------------------------
|      Kevin Durant |     2007 |   2016 |  Thunders |
-----------------------------------------------------
|        Chris Paul |     2017 |   2021 |   Rockets |
-----------------------------------------------------
.....................................................
```

For more detail about Query Language, please see [Traverse The Graph](../docs/nGQL.md#traverse-the-graph).

---

### Step 4 Advanced Usage

Currently you can create your own graph space, such as :

```
Create space mySpace(partition_num=1, replica_factor=1)
```

When you start `Nebula Graph`, a set of vertex and edge have already existed.
Currently the default schema is `nba`.

The graph space describes the relationship between players and teams.
In the graph space, there are two tags (player and team) and two edges (serve and like) which is composed of `string` and `integer`.

The Schema looks like :

```
{
	"nba": {
		"tags": {
			"player": [
				[
					"name: string",
					"age: integer"
				]
			],
			"team": [
				[
					"name: string"
				]
			]
		},
		"edges": {
			"serve": [
				[
					"start_year: integer",
					"end_year: integer"
				]
			],
			"like": [
				[
					"likeness: integer"
				]
			]
		}
	}
}
```

The insert sentences look like the following commands.

```
// Insert some vertexs: players and teams:
INSERT VERTEX player(name, age) VALUES(-8379929135833483044: "Amar'e Stoudemire",36)

INSERT VERTEX team(name) VALUES(-9110170398241263635: "Magic")

// Insert some edges: likes and serves:
INSERT EDGE like(likeness) VALUES(-8379929135833483044 -> 6663720087669302163: 90)

INSERT EDGE serve(start_year, end_year) VALUES(-8379929135833483044 -> 868103967282670864: 2002, 2010)

```

