package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, from_json, split}
import org.apache.spark.sql.streaming.Trigger

object KafkaSparkDelta {
  def main(args: Array[String]): Unit = {
    // Se crea la Sesión de Spark con configuración para poder incluir Delta
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Trying Kafka integration in Delta")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    /*
    //val csvPath= "D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/data1.csv"
    // data = spark.range(0, 5)
    data.write.format("delta").save("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table")
    */

    // Se crea el DataFrame a partir del topic de kafka al que se envían los datos
/*
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testTopic")
      .load()

    //val schema = StructType(List(StructField("student_name", StringType, true), StructField("graduation_year", StringType, true), StructField("major", StringType, true)))
    //df.writeStream.format("delta").option("overwriteSchema", "true").mode("overwrite").save("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table")

    // Se transforman los datos para poder mostrar la información correctamente y no los metadatos
    val transformedDF = df
      .selectExpr("CAST(value AS STRING) as value")
      .selectExpr("from_json(value, 'student_name string, graduation_year string, major string') as data")
      .select("data.*")


    // Se guarda la información recibida en una tabla delta
    val deltaQuery = transformedDF
      .writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", "D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table-stream-checkpoint")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table-stream")
    // Se muestra por consola la información


    val query = transformedDF
      .writeStream
      .format("console") // Use "console" format to display in the console
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()


  */
    //query.awaitTermination()


    //query.awaitTermination()


    // Se lee el archivo delta guardado
    val dfTable = spark.read.format("delta").load("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table-stream")
    dfTable.show()
    spark.stop()


  }
}