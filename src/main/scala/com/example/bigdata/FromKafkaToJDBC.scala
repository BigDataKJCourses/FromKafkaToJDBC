package com.example.bigdata

import com.example.bigdata.tools.GetContext.getSparkSession
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FromKafkaToJDBC {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("Usage: ETLWProdukty <bootstrapServers> <kafkaTopic> <mysqlServer> <mysqlUser> <mysqlPass>")
      System.exit(1)
    }

    val bootstrapServers = args(0)
    val kafkaTopic = args(1)
    val mysqlServer = args(2)
    val mysqlUser = args(3)
    val mysqlPass = args(4)

    val spark = getSparkSession("FromKafkaToJDBC")

    val ds1 = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", bootstrapServers).
      option("subscribe", kafkaTopic).
      load()

    import spark.implicits._

    val valuesDF = ds1.select(expr("CAST(value AS STRING)").as("value"))

    val dataSchema = StructType(
      List(
        StructField("house", StringType, nullable = true),
        StructField("character", StringType, nullable = true),
        StructField("score", StringType, nullable = true),
        StructField("ts", TimestampType, nullable = true)
      )
    )

    val dataDF = valuesDF.select(
        from_json($"value".cast(StringType), dataSchema).as("val")).
      select($"val.house",$"val.character",
        $"val.score".cast("int").as("score"),$"val.ts")

    val resultDF = dataDF.groupBy($"house").
      agg(count($"score").as("how_many"),
        sum($"score").as("sum_score"),
        approx_count_distinct("character",0.1).as("no_characters"))

    val streamWriter = resultDF.writeStream.
      outputMode("complete").
      foreachBatch {
      (batchDF: DataFrame, _: Long) =>
        batchDF.write.
          format("jdbc").
          mode(SaveMode.Overwrite).
          option("url", s"jdbc:mysql://$mysqlServer/streamdb").
          option("dbtable", "housestats").
          option("user", mysqlUser).
          option("password", mysqlPass).
          option("trucate", "true").
          save()
    }

    val query = streamWriter.start()
    query.awaitTermination()

  }

}
