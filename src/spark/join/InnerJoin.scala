package spark.join

import org.apache.spark.sql.SparkSession

object InnerJoin {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .config("spark.master", "local[*]")
      .appName("Inner Join")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val employees = sc.parallelize(Array[(String, Option[Int])](
      ("Rafferty", Some(31)), ("Jones", Some(33)), ("Heisenberg", Some(33)), ("Robinson", Some(34)), ("Smith", Some(34)), ("Williams", null))).toDF("LastName", "DepartmentID")

    employees.show()
    
     val departments = sc.parallelize(Array(
      (31, "Sales"), (33, "Engineering"), (34, "Clerical"),
      (35, "Marketing"))).toDF("DepartmentID", "DepartmentName")

    departments.show()
    
    employees
      .join(departments, "DepartmentID")
      .show()

    /*
 *
 *
+----------+------------+
|  LastName|DepartmentID|
+----------+------------+
|  Rafferty|          31|
|     Jones|          33|
|Heisenberg|          33|
|  Robinson|          34|
|     Smith|          34|
|  Williams|        null|
+----------+------------+

+------------+--------------+
|DepartmentID|DepartmentName|
+------------+--------------+
|          31|         Sales|
|          33|   Engineering|
|          34|      Clerical|
|          35|     Marketing|
+------------+--------------+


Inner Join

SELECT *
FROM employee
INNER JOIN department
ON employee.DepartmentID = department.DepartmentID;

   *
   * Spark automatically removes duplicated “DepartmentID” column, so column names are unique
   * 
+------------+----------+--------------+
|DepartmentID|  LastName|DepartmentName|
+------------+----------+--------------+
|          31|  Rafferty|         Sales|
|          34|  Robinson|      Clerical|
|          34|     Smith|      Clerical|
|          33|     Jones|   Engineering|
|          33|Heisenberg|   Engineering|
+------------+----------+--------------+

   */

    
  }
}