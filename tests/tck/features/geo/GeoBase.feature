# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
Feature: Geo base

  Background:
    Given an empty graph
    And create a space with following options:
      | partition_num  | 1                |
      | replica_factor | 1                |
      | vid_type       | FIXED_STRING(30) |
      | charset        | utf8             |
      | collate        | utf8_bin         |
    And having executed:
      """
      CREATE TAG any_shape(geo geography);
      CREATE TAG only_point(geo geography(point));
      CREATE TAG only_linestring(geo geography(linestring));
      CREATE TAG only_polygon(geo geography(polygon));
      CREATE EDGE any_shape_edge(geo geography);
      """
    And wait 3 seconds
    Given parameters: {"p4": {"s1":"test","s2":"2020-01-01 10:00:00","s3":[1,2,3,4,5],"longitude":[1.0,2.0,3.0],"latitude":[10.1,11.1,12.1]}}

  Scenario: test geo schema
    # Desc geo schema
    When executing query:
      """
      DESC TAG any_shape;
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default | Comment |
      | "geo" | "geography" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      DESC TAG only_point;
      """
    Then the result should be, in any order:
      | Field | Type               | Null  | Default | Comment |
      | "geo" | "geography(point)" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      DESC TAG only_linestring;
      """
    Then the result should be, in any order:
      | Field | Type                    | Null  | Default | Comment |
      | "geo" | "geography(linestring)" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      DESC TAG only_polygon;
      """
    Then the result should be, in any order:
      | Field | Type                 | Null  | Default | Comment |
      | "geo" | "geography(polygon)" | "YES" | EMPTY   | EMPTY   |
    When executing query:
      """
      DESC EDGE any_shape_edge;
      """
    Then the result should be, in any order:
      | Field | Type        | Null  | Default | Comment |
      | "geo" | "geography" | "YES" | EMPTY   | EMPTY   |
    # Show create geo schema
    When executing query:
      """
      SHOW CREATE TAG only_point;
      """
    Then the result should be, in any order:
      | Tag          | Create Tag                                                                                  |
      | "only_point" | 'CREATE TAG `only_point` (\n `geo` geography(point) NULL\n) ttl_duration = 0, ttl_col = ""' |
    # Test default property value
    When executing query:
      """
      CREATE TAG test_1(geo geography DEFAULT ST_Point(3, 8));
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE test_2(geo geography DEFAULT ST_GeogFromText("LINESTRING(0 1, 2 3)"));
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE test_2(geo geography DEFAULT ST_GeogFromText("LINESTRING(0 1, 2xxxx"));
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG test_3(geo geography(point) DEFAULT ST_GeogFromText("LineString(0 1, 2 3)"));
      """
    Then a ExecutionError should be raised at runtime: Invalid param!
    When executing query:
      """
      CREATE TAG test_3(geo geography(linestring) DEFAULT ST_GeogFromText("LineString(0 1, 2 3)"));
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      INSERT VERTEX test_1() VALUES "test_101":()
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE test_2() VALUES "test_101"->"test_102":()
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX test_3() VALUES "test_103":()
      """
    Then the execution should be successful
    And drop the used space

  Scenario: test geo CURD
    # Any geo shape(point/linestring/polygon) is allowed to insert to the column geography
    When try to execute query:
      """
      INSERT VERTEX any_shape(geo) VALUES "101":(ST_GeogFromText("POINT(3 8)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX any_shape(geo) VALUES "102":(ST_GeogFromText("LINESTRING(3 8, 4.7 73.23)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX any_shape(geo) VALUES "103":(ST_GeogFromText("POLYGON((0 1, 1 2, 2 3, 0 1))"));
      """
    Then the execution should be successful
    # "POINT(3 8)" is a string not a geography literal.
    # We must use some geography costructor function to construct a geography literal.
    # e.g.`ST_GeogFromText("POINT(3 8)")`
    When executing query:
      """
      INSERT VERTEX any_shape(geo) VALUES "104":("POINT(3 8)");
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    # Only point is allowed to insert to the column geograph(point)
    When executing query:
      """
      INSERT VERTEX only_point(geo) VALUES "201":(ST_GeogFromText("POINT(3 8)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX only_point(geo) VALUES "202":(ST_GeogFromText("LINESTRING(3 8, 4.7 73.23)"));
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX only_point(geo) VALUES "203":(ST_GeogFromText("POLYGON((0 1, 1 2, 2 3, 0 1))"));
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    # Only linestring is allowed to insert to the column geograph(linestring)
    When executing query:
      """
      INSERT VERTEX only_linestring(geo) VALUES "301":(ST_GeogFromText("POINT(3 8)"));
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX only_linestring(geo) VALUES "302":(ST_GeogFromText("LINESTRING(3 8, 4.7 73.23)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX only_linestring(geo) VALUES "303":(ST_GeogFromText("POLYGON((0 1, 1 2, 2 3, 0 1))"));
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    # Only polygon is allowed to insert to the column geograph(polygon)
    When executing query:
      """
      INSERT VERTEX only_polygon(geo) VALUES "401":(ST_GeogFromText("POINT(3 8)"));
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX only_polygon(geo) VALUES "402":(ST_GeogFromText("LINESTRING(3 8, 4.7 73.23)"));
      """
    Then a ExecutionError should be raised at runtime: Storage Error: The data type does not meet the requirements. Use the correct type of data.
    When executing query:
      """
      INSERT VERTEX only_polygon(geo) VALUES "403":(ST_GeogFromText("POLYGON((0 1, 1 2, 2 3, 0 1))"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE any_shape_edge(geo) VALUES "201"->"302":(ST_GeogFromText("POLYGON((0 1, 1 2, 2 3, 0 1))"));
      """
    Then the execution should be successful
    # Fetch the geo column
    When executing query:
      """
      FETCH PROP ON any_shape "101","102","103" YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POINT(3 8)"                    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      FETCH PROP ON only_point "201","202","203" YIELD ST_ASText(only_point.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(only_point.geo) |
      | "POINT(3 8)"              |
    When executing query:
      """
      FETCH PROP ON only_linestring "301","302","303" YIELD ST_ASText(only_linestring.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(only_linestring.geo) |
      | "LINESTRING(3 8, 4.7 73.23)"   |
    When executing query:
      """
      FETCH PROP ON only_polygon "401","402","403" YIELD ST_ASText(only_polygon.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(only_polygon.geo)     |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      FETCH PROP ON any_shape_edge "201"->"302" YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo)   |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    # Create index on geo column
    When executing query:
      """
      CREATE TAG INDEX any_shape_geo_index ON any_shape(geo) with (s2_max_level=30, s2_max_cells=8) comment "test";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX only_point_geo_index ON only_point(geo) comment "test2";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX only_linestring_geo_index ON only_linestring(geo) with (s2_max_cells=12) comment "test3";
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE TAG INDEX only_polygon_geo_index ON only_polygon(geo);
      """
    Then the execution should be successful
    When executing query:
      """
      CREATE EDGE INDEX any_shape_edge_geo_index ON any_shape_edge(geo) with (s2_max_level=23);
      """
    Then the execution should be successful
    And wait 3 seconds
    # Show create tag index
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      SHOW CREATE TAG INDEX any_shape_geo_index;
      """
    Then the result should be, in any order:
      | Tag Index Name        | Create Tag Index                                                                                                                 |
      | "any_shape_geo_index" | "CREATE TAG INDEX `any_shape_geo_index` ON `any_shape` (\n `geo`\n) WITH (s2_max_level = 30, s2_max_cells = 8) comment \"test\"" |
    # Rebuild the geo index
    When submit a job:
      """
      REBUILD TAG INDEX any_shape_geo_index;
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD TAG INDEX only_point_geo_index;
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD TAG INDEX only_linestring_geo_index;
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD TAG INDEX only_polygon_geo_index;
      """
    Then wait the job to finish
    When submit a job:
      """
      REBUILD EDGE INDEX any_shape_edge_geo_index;
      """
    Then wait the job to finish
    # Lookup on geo index
    When profiling query:
      """
      LOOKUP ON any_shape YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    And the execution plan should be:
      | id | name             | dependencies | operator info |
      | 2  | Project          | 3            |               |
      | 3  | TagIndexFullScan | 0            |               |
      | 0  | Start            |              |               |
    When executing query:
      """
      LOOKUP ON only_point YIELD ST_ASText(only_point.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(only_point.geo) |
      | "POINT(3 8)"              |
    When executing query:
      """
      LOOKUP ON only_linestring YIELD ST_ASText(only_linestring.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(only_linestring.geo) |
      | "LINESTRING(3 8, 4.7 73.23)"   |
    When executing query:
      """
      LOOKUP ON only_polygon YIELD ST_ASText(only_polygon.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(only_polygon.geo)     |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      LOOKUP ON any_shape_edge YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo)   |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    # Match with geo index
    When executing query:
      """
      MATCH (v:any_shape) RETURN ST_ASText(v.any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(v.any_shape.geo)      |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    # Insert geo data with index
    When try to execute query:
      """
      INSERT VERTEX any_shape(geo) VALUES "108":(ST_GeogFromText("POINT(72.3 84.6)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX only_point(geo) VALUES "208":(ST_GeogFromText("POINT(0.01 0.01)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX only_linestring(geo) VALUES "308":(ST_GeogFromText("LINESTRING(9 9, 8 8, 7 7, 9 9)"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT VERTEX only_polygon(geo) VALUES "408":(ST_GeogFromText("POLYGON((0 1, 1 2, 2 3, 0 1))"));
      """
    Then the execution should be successful
    When executing query:
      """
      INSERT EDGE any_shape_edge(geo) VALUES "108"->"408":(ST_GeogFromText("POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1.0 1.0, 2.0 2.0, 0.0 2.0, 1.0 1.0))"));
      """
    Then the execution should be successful
    # Lookup on geo index again
    When executing query:
      """
      LOOKUP ON any_shape YIELD id(vertex) as id, ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | id    | ST_ASText(any_shape.geo)        |
      | "101" | "POINT(3 8)"                    |
      | "102" | "LINESTRING(3 8, 4.7 73.23)"    |
      | "103" | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "108" | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON only_point YIELD id(vertex)  as id, ST_ASText(only_point.geo);
      """
    Then the result should be, in any order:
      | id    | ST_ASText(only_point.geo) |
      | "201" | "POINT(3 8)"              |
      | "208" | "POINT(0.01 0.01)"        |
    When executing query:
      """
      LOOKUP ON only_linestring YIELD id(vertex) as id, ST_ASText(only_linestring.geo);
      """
    Then the result should be, in any order:
      | id    | ST_ASText(only_linestring.geo)   |
      | "302" | "LINESTRING(3 8, 4.7 73.23)"     |
      | "308" | "LINESTRING(9 9, 8 8, 7 7, 9 9)" |
    When executing query:
      """
      LOOKUP ON only_polygon YIELD id(vertex) as id, ST_ASText(only_polygon.geo);
      """
    Then the result should be, in any order:
      | id    | ST_ASText(only_polygon.geo)     |
      | "403" | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "408" | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      LOOKUP ON any_shape_edge YIELD src(edge) as src, dst(edge) as dst, rank(edge) as rank, ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | src   | dst   | rank | ST_ASText(any_shape_edge.geo)                                              |
      | "108" | "408" | 0    | "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1 1, 2 2, 0 2, 1 1))" |
      | "201" | "302" | 0    | "POLYGON((0 1, 1 2, 2 3, 0 1))"                                            |
    # Lookup and Yield geo functions
    When executing query:
      """
      LOOKUP ON any_shape YIELD id(vertex) as id, S2_CellIdFromPoint(any_shape.geo);
      """
    Then the result should be, in any order:
      | id    | S2_CellIdFromPoint(any_shape.geo) |
      | "101" | 1166542697063163289               |
      | "102" | BAD_DATA                          |
      | "103" | BAD_DATA                          |
      | "108" | 4987215245349669805               |
    When executing query:
      """
      LOOKUP ON any_shape YIELD id(vertex) as id, S2_CoveringCellIds(any_shape.geo);
      """
    Then the result should be, in any order:
      | id    | S2_CoveringCellIds(any_shape.geo)                                                                                                                                        |
      | "101" | [1166542697063163289]                                                                                                                                                    |
      | "102" | [1167558203395801088, 1279022294173220864, 1315051091192184832, 1351079888211148800, 5039527983027585024, 5062045981164437504, 5174635971848699904, 5183643171103440896] |
      | "103" | [1152391494368201343, 1153466862374223872, 1153554823304445952, 1153836298281156608, 1153959443583467520, 1154240918560178176, 1160503736791990272, 1160591697722212352] |
      | "108" | [4987215245349669805]                                                                                                                                                    |
    # Lookup with geo predicates which could be index accelerated
    # ST_Intersects
    When profiling query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(any_shape.geo, ST_GeogFromText('POINT(3 8)')) YIELD id(vertex) as id, ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | id    | ST_ASText(any_shape.geo)     |
      | "101" | "POINT(3 8)"                 |
      | "102" | "LINESTRING(3 8, 4.7 73.23)" |
    And the execution plan should be:
      | id | name      | dependencies | operator info |
      | 3  | Project   | 4            |               |
      | 4  | IndexScan | 0            |               |
      | 0  | Start     |              |               |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(any_shape.geo, ST_GeogFromText('POINT(0 1)')) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(any_shape.geo, ST_GeogFromText('POINT(4.7 73.23)')) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "LINESTRING(3 8, 4.7 73.23)" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(any_shape.geo, ST_Point(72.3, 84.6)) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo) |
      | "POINT(72.3 84.6)"       |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(ST_Point(72.3, 84.6), any_shape.geo) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo) |
      | "POINT(72.3 84.6)"       |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(any_shape.geo, any_shape.geo) YIELD ST_ASText(any_shape.geo);
      """
    Then a SemanticError should be raised at runtime: Expression ST_Intersects(any_shape.geo,any_shape.geo) not supported yet
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8.4)) < true YIELD ST_ASText(any_shape.geo);
      """
    Then a SemanticError should be raised at runtime:
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_DWithin(any_shape.geo, ST_Point(3, 8.4), true) YIELD ST_ASText(any_shape.geo);
      """
    Then a SemanticError should be raised at runtime:
    # Match with geo predicate
    When executing query:
      """
      MATCH (v:any_shape) WHERE ST_Intersects(v.any_shape.geo, ST_GeogFromText('POINT(3 8)')) RETURN ST_ASText(v.any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(v.any_shape.geo)   |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    # ST_Distance
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) < 1.0 YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) <= 1.0 YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) <= 8909524.383934561 YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) < 8909524.383934561 YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) < 8909524.383934563 YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point($p4.longitude[0], $p4.latitude[1])) < $p4.latitude[2] YIELD id(vertex)
      """
    Then the result should be, in any order:
      | id(VERTEX) |
    When executing query:
      """
      LOOKUP ON any_shape WHERE 8909524.383934560 > ST_Distance(any_shape.geo, ST_Point(3, 8)) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE 8909524.3839345630 >= ST_Distance(any_shape.geo, ST_Point(3, 8)) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) > 1.0 YIELD ST_ASText(any_shape.geo);
      """
    Then a SemanticError should be raised at runtime: Expression (ST_Distance(any_shape.geo,ST_Point(3,8))>1) not supported yet
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Distance(any_shape.geo, ST_Point(3, 8)) != 1.0 YIELD ST_ASText(any_shape.geo);
      """
    Then a SemanticError should be raised at runtime: Expression (ST_Distance(any_shape.geo,ST_Point(3,8))!=1) not supported yet
    # ST_DWithin
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_DWithin(any_shape.geo, ST_Point(3, 8), 8909524.383934561) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POINT(3 8)"                    |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_DWithin(any_shape.geo, ST_Point(3, 8), 100.0) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_DWithin(any_shape.geo, ST_Point(3, 8), 100) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    # ST_Covers
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Covers(any_shape.geo, ST_Point(3, 8)) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Covers(any_shape.geo, ST_Point(3, 8)) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "POINT(3 8)"                 |
      | "LINESTRING(3 8, 4.7 73.23)" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Covers(ST_GeogFromText('POLYGON((-0.7 3.8,3.6 3.2,1.8 -0.8,-3.4 2.4,-0.7 3.8))'), any_shape.geo) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_CoveredBy(any_shape.geo, ST_GeogFromText('POLYGON((-0.7 3.8,3.6 3.2,1.8 -0.8,-3.4 2.4,-0.7 3.8))')) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
    # Update vertex with index
    When executing query:
      """
      UPDATE VERTEX ON any_shape "101" SET any_shape.geo = ST_GeogFromText('LINESTRING(3 8, 6 16)');
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON any_shape "101" YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo) |
      | "LINESTRING(3 8, 6 16)"  |
    When executing query:
      """
      LOOKUP ON any_shape YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "LINESTRING(3 8, 6 16)"         |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_DWithin(any_shape.geo, ST_Point(3, 8), 100.0) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "LINESTRING(3 8, 6 16)"      |
      | "LINESTRING(3 8, 4.7 73.23)" |
    # Update edge with index
    When executing query:
      """
      UPDATE EDGE ON any_shape_edge "201"->"302" SET any_shape_edge.geo = ST_GeogFromText('POINT(-1 -1)');
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON any_shape_edge "201"->"302" YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo) |
      | "POINT(-1 -1)"                |
    When executing query:
      """
      LOOKUP ON any_shape_edge YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo)                                              |
      | "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1 1, 2 2, 0 2, 1 1))" |
      | "POINT(-1 -1)"                                                             |
    When executing query:
      """
      LOOKUP ON any_shape_edge WHERE ST_Intersects(any_shape_edge.geo, ST_Point(-1, -1)) YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo)                                              |
      | "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1 1, 2 2, 0 2, 1 1))" |
      | "POINT(-1 -1)"                                                             |
    # Delete vertex with index
    When executing query:
      """
      DELETE VERTEX "101" WITH EDGE;
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON any_shape "101" YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo) |
    When executing query:
      """
      LOOKUP ON any_shape YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)        |
      | "LINESTRING(3 8, 4.7 73.23)"    |
      | "POLYGON((0 1, 1 2, 2 3, 0 1))" |
      | "POINT(72.3 84.6)"              |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Covers(any_shape.geo, ST_Point(3, 8)) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo)     |
      | "LINESTRING(3 8, 4.7 73.23)" |
    # Delete edge with index
    When executing query:
      """
      DELETE EDGE any_shape_edge "201"->"302";
      """
    Then the execution should be successful
    When executing query:
      """
      FETCH PROP ON any_shape_edge "201"->"302" YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo) |
    When executing query:
      """
      LOOKUP ON any_shape_edge YIELD ST_ASText(any_shape_edge.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape_edge.geo)                                              |
      | "POLYGON((-20 -20, -20 20, 20 20, 20 -20, -20 -20), (1 1, 2 2, 0 2, 1 1))" |
    When executing query:
      """
      LOOKUP ON any_shape WHERE ST_Intersects(ST_Point(-1, -1), any_shape.geo) YIELD ST_ASText(any_shape.geo);
      """
    Then the result should be, in any order:
      | ST_ASText(any_shape.geo) |
    # Drop tag index
    When executing query:
      """
      DROP TAG INDEX any_shape_geo_index;
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      LOOKUP ON any_shape YIELD id(vertex) as id;
      """
    Then a ExecutionError should be raised at runtime: There is no index to use at runtime
    # Drop edge index
    When executing query:
      """
      DROP EDGE INDEX any_shape_edge_geo_index;
      """
    Then the execution should be successful
    And wait 3 seconds
    When executing query and retrying it on failure every 6 seconds for 3 times:
      """
      LOOKUP ON any_shape_edge YIELD edge as e;
      """
    Then a ExecutionError should be raised at runtime: There is no index to use at runtime
    # Drop tag
    When executing query:
      """
      DROP TAG any_shape;
      """
    Then the execution should be successful
    # Drop edge
    When executing query:
      """
      DROP EDGE any_shape_edge;
      """
    Then the execution should be successful
    And drop the used space
