package spark.join

import org.apache.spark.sql.SparkSession

object JoinExpression {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .config("spark.master", "local[*]")
      .appName("Inner Join")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val products = sc.parallelize(Array(
  ("steak", "1990-01-01", "2000-01-01", 150),
  ("steak", "2000-01-02", "2020-01-01", 180),
  ("fish", "1990-01-01", "2020-01-01", 100)
)).toDF("name", "startDate", "endDate", "price")

products.show()

val orders = sc.parallelize(Array(
  ("1995-01-01", "steak"),
  ("2000-01-01", "fish"),
  ("2005-01-01", "steak")
)).toDF("date", "product")

orders.show()

orders
  .join(products, $"product" === $"name" && $"date" >= $"startDate" && $"date" <= $"endDate")
  .show()
  }
  
  /*
   * 
   * One application of it is slowly changing dimensions. Assume there is a table with product prices over time:
   * 
+-----+----------+----------+-----+
| name| startDate|   endDate|price|
+-----+----------+----------+-----+
|steak|1990-01-01|2000-01-01|  150|
|steak|2000-01-02|2020-01-01|  180|
| fish|1990-01-01|2020-01-01|  100|
+-----+----------+----------+-----+

There are two products only: steak and fish, price of steak has been changed once. 
Another table consists of product orders by day:

+----------+-------+
|      date|product|
+----------+-------+
|1995-01-01|  steak|
|2000-01-01|   fish|
|2005-01-01|  steak|
+----------+-------+

Our goal is to assign an actual price for every record in the orders table. 
It is not obvious to do using only equality operators, 
however, spark join expression allows us to achieve the result in an elegant way:

+----------+-------+-----+----------+----------+-----+
|      date|product| name| startDate|   endDate|price|
+----------+-------+-----+----------+----------+-----+
|2000-01-01|   fish| fish|1990-01-01|2020-01-01|  100|
|1995-01-01|  steak|steak|1990-01-01|2000-01-01|  150|
|2005-01-01|  steak|steak|2000-01-02|2020-01-01|  180|
+----------+-------+-----+----------+----------+-----+
   */
}