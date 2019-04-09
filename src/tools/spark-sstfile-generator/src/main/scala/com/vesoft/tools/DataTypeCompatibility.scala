package com.vesoft.tools

object DataTypeCompatibility {

  /**
    * nebula data type --> Hive data type mapping
    */
  val compatibilityMatrix: Map[String, Set[String]] = Map(
    "INTEGER" -> Set("TINYINT", "SMALLINT", "INT", "BIGINT"),
    "DOUBLE" -> Set("DOUBLE", "DECIMAL"),
    "FLOAT" -> Set("FLOAT", "DECIMAL"),
    //TODO: varchar/char?
    "STRING" -> Set("VARCHAR", "CHAR", "STRING"),
    "BOOL" -> Set("BOOLEAN"),
    "DATE" -> Set("DATE", "TIMESTAMP"),
    "DATETIME" -> Set("DATE", "TIMESTAMP"),
    "YEARMONTH" -> Set("DATE")

    //TODO(qianyong) "binary" -> ?
  )

  /**
    * check whether nebula data type is compatible with hive data type
    */
  def isCompatible(nebulaType: String, hiveType: String): Boolean = {
    // all type can be converted to nebula's string
    if (nebulaType.equalsIgnoreCase("string")) {
      true
    } else {
      compatibilityMatrix.get(nebulaType.toUpperCase).map(_.contains(hiveType.toUpperCase)).getOrElse(false)
    }
  }
}
