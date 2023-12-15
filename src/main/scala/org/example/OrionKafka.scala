package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, explode, from_json, split}
import org.apache.spark.sql.streaming.Trigger

object OrionKafka {
  def main(args: Array[String]): Unit = {
    // Se crea la Sesión de Spark con configuración para poder incluir Delta
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("Trying retrieving Orion-NGSI messages from Kafka")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Se crea el DataFrame a partir del topic de kafka al que se envían los datos

              val df = spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "testing")
                .load()
  /*
          val schema = StructType(
            List(
              StructField("id", StringType),
              StructField("type", StringType),
              StructField("pressure", ArrayType(StructType)
              StructField("Wet_Bulb_Temperature", FloatType),
              StructField("Humidity", FloatType),
              StructField("Rain_Intensity", FloatType),
              StructField("Interval_Rain", FloatType),
              StructField("Total_Rain", FloatType),
              StructField("Precipitation_Type", StringType),
              StructField("Wind_Direction", FloatType),
              StructField("Wind_Speed", FloatType),
              StructField("Maximum_Wind_Speed", FloatType),
              StructField("Barometric_Pressure", FloatType),
              StructField("Solar_Radiation", FloatType),
              StructField("Heading", FloatType),
              StructField("Battery_Life", FloatType),
              StructField("Measurement_Timestamp_Label", StringType),
              StructField("Measurement_ID", StringType)
            )
          )

   */
  val schema1 = StructType(Seq(
    StructField("id", StringType),
    StructField("type", StringType),
    StructField("pressure", ArrayType(
      StructType(Seq(
        StructField("type", StringType),
        StructField("value", DoubleType),
        StructField("metadata", MapType(StringType, StringType))
      ))
    )),
    StructField("temperature", ArrayType(
      StructType(Seq(
        StructField("type", StringType),
        StructField("value", DoubleType),
        StructField("metadata", MapType(StringType, StringType))
      ))
    ))
  ))



          //val schema = StructType(List(StructField("student_name", StringType, true), StructField("graduation_year", StringType, true), StructField("major", StringType, true)))
          //df.writeStream.format("delta").option("overwriteSchema", "true").mode("overwrite").save("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table")
          // Se transforman los datos para poder mostrar la información correctamente y no los metadatos
        /*
          val transformedDF = df
            .selectExpr("CAST(value AS STRING) as value")
            .selectExpr("from_json(
            .select("data.*")
          transformedDF.printSchema()
          */


    val json_df = df.selectExpr("cast(value as string) as value")
    val json_expanded_df = json_df
      .withColumn("value", from_json(col("value"), schema1))
      .select("value.*")
    //json_expanded_df.printSchema()


        val exploded_df = json_expanded_df
          .select("id", "type", "pressure", "temperature") // Keep the columns that won't be exploded
          .withColumn("pressure", explode(col("pressure")))
          .withColumn("temperature", explode(col("temperature")))
        //exploded_df.printSchema()

    val flattened_df = exploded_df
    .select("id", "type", "pressure.type","pressure.value","pressure.metadata", "temperature.type", "temperature.value","temperature.metadata")
    flattened_df.printSchema()


            //Station_Name,Measurement_Timestamp,Air_Temperature,Wet_Bulb_Temperature,Humidity,Rain_Intensity,Interval_Rain,Total_Rain,Precipitation_Type,Wind_Direction,
                // Wind_Speed,Maximum_Wind_Speed,Barometric_Pressure,Solar_Radiation,Heading,Battery_Life,Measurement_Timestamp_Label,Measurement_ID
              // Se guarda la información recibida en una tabla delta

    json_expanded_df
                    .writeStream
                    .outputMode("append")
                    .format("delta")
                    .option("checkpointLocation", "D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table-stream-iot-orion-checkpoint")
                    .trigger(Trigger.ProcessingTime("10 seconds"))
                    .start("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table-stream-iot-orion")
                  // Se muestra por consola la información



                  val query = json_expanded_df
                    .writeStream
                    .format("console") // Use "console" format to display in the console
                    .outputMode("append")
                    .trigger(Trigger.ProcessingTime("10 seconds"))
                    .start()



                  query.awaitTermination()



    // Se lee el archivo delta guardado
    //val dfTable = spark.read.format("delta").load("D:/BECA/SparkTest/SparkTest/src/main/scala/org/example/data/csv/results/delta-table-stream-iot")
    //dfTable.show()

    spark.stop()

  }
}

