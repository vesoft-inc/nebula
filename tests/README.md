Nebula Graph Test Manual
========================

## Usage

### Build project

First of all, change directory to the root of `nebula-graph`, and build the whole project.

### Init environment

Nebula Test framework depends on python3(>=3.7) and some thirdparty libraries, such as [nebula-python](https://github.com/vesoft-inc/nebula-python), [reformat-gherkin](https://github.com/OneContainer/reformat-gherkin), pytest, [pytest-bdd](https://pytest-bdd.readthedocs.io/en/latest/) and so on.

So you should install all these dependencies before running test cases by:

```shell
$ make init-all
```

### Start nebula servers

Then run the following commands to start all nebula services built in above steps by GNU Make tool:

```shell
$ cd tests
$ make up
```

The target `up` in Makefile will select some random ports used by nebula, install all built necessary files to temporary folder which name format is like `server_2021-03-15T17-52-52` and start `metad/storaged/graphd` servers.

If your build directory is not `nebula-graph/build`, you should specify the `BUILD_DIR` parameter when up the nebula services:

```shell
$ make BUILD_DIR=/path/to/nebula/build/directory up
```

### Run all test cases

There are two classes of nebula graph test cases, one is built on pytest and another is built on TCK. We split them into different execution methods:

```shell
$ make test # run pytest cases
$ make tck  # run TCK cases
```

If you want to debug the `core` files when running tests, you can pass the `RM_DIR` parameter into `make` target to enable it, like:

```shell
$ make RM_DIR=false tck  # default value of RM_DIR is true
```

And if you want to debug only one test case, you should check the usage of `pytest` itself by `pytest --help`. For example, run the test cases related to `MATCH`, you can do it like:

```shell
$ pytest -k 'match' -m 'not skip' .
```

We also provide a parameter named `address` to allow these tests to connect to the nebula services maintained by yourself:

```shell
$ pytest --address="192.168.0.1:9669" -m 'not skip' .
```

You can use following commands to only rerun the test cases if they failed:

```shell
$ pytest --last-failed --gherkin-terminal-reporter --gherkin-terminal-reporter-expanded .
```

or

```shell
$ make fail
```

`gherkin-terminal-reporter` options will print the pytest report prettily.


### Stop nebula servers

Following command will stop the nebula servers started in above steps:

```shell
$ make down
```

If you want to stop some unused nebula processes, you can kill them by:

```shell
$ make kill
```

cleanup temporary files by:

```shell
$ make clean
```

## How to add test case

You can find all nebula test cases in [tck/features](tck/features) and some openCypher cases in [tck/openCypher/features](tck/openCypher/features). Some references about [TCK](https://github.com/opencypher/openCypher/tree/master/tck) may be what you need.

The test cases are organized in feature files and described in gherkin language. The structure of feature file is like following example:

#### Basic Case:

```gherkin
Feature: Basic match

  Background:
    Given a graph with space named "nba"

  Scenario: Single node
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"}) RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v            |
      | ("Yao Ming") |

  Scenario: One step
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2)
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "like"  | "Ray Allen" |
      | "serve" | "Cavaliers" |
      | "serve" | "Heat"      |
      | "serve" | "Lakers"    |
      | "serve" | "Cavaliers" |
```

#### Case With an Execution Plan:
```gherkin
Scenario: push edge props filter down
  When profiling query:
    """
    GO FROM "Tony Parker" OVER like
    WHERE like.likeness IN [v IN [95,99] WHERE v > 0]
    YIELD like._dst, like.likeness
    """
  Then the result should be, in any order:
    | like._dst       | like.likeness |
    | "Manu Ginobili" | 95            |
    | "Tim Duncan"    | 95            |
  And the execution plan should be:
    | id | name         | dependencies | operator info                                               |
    | 0  | Project      | 1            |                                                             |
    | 1  | GetNeighbors | 2            | {"filter": "(like.likeness IN [v IN [95,99] WHERE (v>0)])"} |
    | 2  | Start        |              |                                                             |
```

Each feature file is composed of different scenarios which split test units into different parts. There are many steps in each scenario to define the inputs and outputs of test. These steps are started with following words:

- Given
- When
- Then

The table in `Then` step must have the first header line even if there's no data rows.

`Background` is the common steps of different scenarios. Scenarios will be executed in parallel.

Note that for cases that contain execution plans, it is mandatory to fill the `id` column.

### Format

In order to check your changed files for reviewers conveniently, please format your feature file before creating pull request. Try following command to do that:

```shell
$ make fmt
```
