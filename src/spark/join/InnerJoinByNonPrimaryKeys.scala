package spark.join

import org.apache.spark.sql.SparkSession

object InnerJoinByNonPrimaryKeys {
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
      (0), (1), (1))).toDF("c1")

    df.show()
    df.join(df, "c1").show()

    val df2 = sc.parallelize(Array(
      (2), (5), (6))).toDF("c2")
    df2.show()
    
    val df2_new = df2.withColumn("c1", df2("c2"))
    df2_new.show()
    
    df.join(df2_new, "c1").show()
    /*
 *
 *
// Original DataFrame
+---+
| c1|
+---+
|  0|
|  1|
|  1|
+---+

// Self-joined DataFrame
+---+
| c1|
+---+
|  0|
|  1|
|  1|
|  1|
|  1|
+---+

   */

  }
}