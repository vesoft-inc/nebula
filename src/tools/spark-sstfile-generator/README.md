# mapping file format

conceptually, each hive table maps to a nebula vertex/edge, whose primary key should be identified


# TODO
1. add database_name property to graphspace level and tag/edge level, which the latter will override the former when provided in both levels
2. schema column definitions' order is important, keep it when parsing mapping file and when encoding
3. integrated build with maven or cmake, where this spark assembly should be build after nebula native client
4. to handle following situation: different tables share a common Tag, like a tag with properties of (start_time, end_time)

