package org.example

import org.apache.spark.sql.SparkSession

object SparkDemo {
  def main(args: Array[String]): Unit = {
    try{
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("SparkDemo")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()


    println("Printing Spark Session Variables:")
    println("App Name: " + spark.sparkContext.appName)
    println("Deployment Mode: " + spark.sparkContext.deployMode)
    println("Master: " + spark.sparkContext.master)


      //Testing: Append new table to previous students table.
      /*
      val test_df = spark.range(0,3)

      test_df.show()
    */
    val file_path = "D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/data1.csv"
      //test_df.repartition(1).write.mode("append").format("csv").option(
        //"header", true).save("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv")



    spark.stop()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}