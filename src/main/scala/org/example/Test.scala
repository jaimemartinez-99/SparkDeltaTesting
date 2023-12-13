package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Trying Delta")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val csvPath= "D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/data1.csv"
    //val schema = StructType(List(StructField("student_name", StringType, true),StructField("graduation_year", StringType, true),StructField("major", StringType, true)))

    val csvDF = spark.read.format("csv").option("header", true).load("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/*.csv")
    csvDF.write.format("delta").save("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table")
    csvDF.show()
    /*
    val data = spark.range(0, 5)
    data.write.format("delta").save("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table")
    val df = spark.read.format("delta").load("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table")
    df.show()

     */
    spark.stop()


  }
}
