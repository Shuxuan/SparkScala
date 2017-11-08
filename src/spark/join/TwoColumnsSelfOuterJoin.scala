package spark.join

import org.apache.spark.sql.SparkSession

object TwoColumnsOuterJoin {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .config("spark.master", "local[*]")
      .appName("Inner Join")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.parallelize(Array(
      ("0", "2"),
      ("1", "5"),
      ("1", "6"))).toDF("c1", "c2")

    val df2 = df.drop("c1").withColumn("c1", df("c2")).drop("c2")
    df.show()
    df2.show()

    df.select("c1").join(df2, Seq("c1"), "outer").show()

    /*
 *
 *
// Original DataFrame
+---+---+
| c1| c2|
+---+---+
|  0|  2|
|  1|  5|
|  1|  6|
+---+---+

d2_new
+---+
| c1|
+---+
|  2|
|  5|
|  6|
+---+

// Self-joined DataFrame
+---+
| c1|
+---+
|  0|
|  5|
|  6|
|  1|
|  1|
|  2|
+---+

   */

  }
}