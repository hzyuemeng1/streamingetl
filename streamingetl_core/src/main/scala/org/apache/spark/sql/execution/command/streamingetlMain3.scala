package org.apache.spark.sql.execution.command

import com.netease.streamingetl.schema.{JsonSchemaGenerator, inferInformation}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession, persiteDB}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by hzyuemeng1 on 2016/10/26.
  *
  * start streaming etl programm to etl json data which store in kafka to hive in real time
  *bin/spark-submit --master local[40] --driver-memory 2g --class
  * com.netease.streamingetl.streamingetlMain /home/ds/jars/streamingetl_core-1.0-SNAPSHOT.jar
  * db-180.photo.163.org:9092
  * json_test
  * file:///home/ds/ym/sample.json
  * json_tmp4
  *
  * db-180.photo.163.org:9092  brokerlist for which broker will be connected
  * json_test  topic name for which topic in kafka will be poll data
  * file:///home/ds/ym/sample.json  a sample json file for generator a schema and sample data ,this arg also can be json string
  * json_tmp4 table name,persist the schema with this name in db,we can access later.
  *
  * Notes:for this streamingetl ,user just give the etl logical with sql in two tables,one table in generate by streming source,one is the actual table we want
  * in hive.
  */
object streamingetlMain3 {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: streamingetlMain <brokerList> <topics> <path> <origin_table_name>")
      System.exit(1)
    }

    val Array(brokerlist,topics,path,origin_table_name) = args


    //config this streamingetl to get the spark session and streamingcontext

    val conf = new SparkConf()
    val spark = SparkSession.builder().appName("streamingetl-demo").config(conf).enableHiveSupport().getOrCreate()
    val  ssc = StreamingContext.getActiveOrCreate(() => new StreamingContext(spark.sparkContext,Seconds(20)))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerlist,
      "zookeeper.connect" -> "db-180.photo.163.org:2182",
      "group.id" -> "consumergroup",
      "serializer.class" -> "kafka.serializer.StringEncoder")

    // generator schema and samle data by path or json string
    println("-------- step 1 >> start to generator schema and samle data by path or json string ----")
    val Jsonschemahandler = new JsonSchemaGenerator(spark)
    val infer = inferInformation(true,true,path) // here ,we can also use json string for genedate schema and sample data ,jus constructor inferInformation(false,true,jsonstring)

    val generator = Jsonschemahandler.generateSchema(infer)
    val schema =  generator.getSchema
    println("******************* start to print schema *********************")
    println(schema.treeString)
    println("******************* end to print schema *********************")
    val sampleData = generator.getShowData
    println("******************* start to print sampleData *********************")
    println(sampleData)
    println("******************* end to print sampleData *********************")

    println("-------- end to generator schema and samle data by path or json string ----")

   // persist the schema to db with sepcificed the given name

    println("--------- step 2 >>  start  persist the schema to db with sepcificed the given name -------------")
   val dbHandler =  new persiteDB(spark,Option(schema))
    dbHandler.saveSchmaToDB(origin_table_name)

    println("--------- end  persist the schema to db with sepcificed the given name -------------")
    // we will be create a partition table use spark sql which hive also can see it

    spark.sql("DROP TABLE IF EXISTS user_partitioned")
    spark.sql("CREATE TABLE IF NOT EXISTS user_partitioned (user String,money Double) PARTITIONED BY (city STRING,phone STRING)")

    // then we config dynamic partition to insert data which we receive from kafka and etl it

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("SET hive.exec.max.dynamic.partitions.pernode = 1000")
    spark.sql("SET hive.exec.max.dynamic.partitions=1000")

    // get the origin data streaming source use spark to receive data from kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicsSet).repartition(200)
    val ds: DStream[String]= messages.map(_._2)

    println("-----------  step 3 >> etl the data to get the vauleable data we want,then sync to hive table we first create       -------------------")
    // then etl the data to get the vauleable data we want,then sync to hive table we first create

    ds.foreachRDD{

      rdd => {
        //turn rdd to dataframe,then we can turn dataframe to table,use sql to access it
        val df = spark.read.json(rdd)
       // etl data
        df.write.format("json").mode(SaveMode.Append).insertInto(origin_table_name) //receive data to tmp file
        // auto etl the data to hive parition table,use sql logical for etl one table to another table
        val tmp = spark.sql(s"insert into table user_partitioned PARTITION (city,phone) select body.user,body.money,body.city AS city,body.phone AS phone from $origin_table_name distribute by city,phone")
      }
    }

   println("---------  step 4 >>   start the streaming programm ---------------")
    //start the streaming programm

    ssc.start()
    ssc.awaitTermination()
  }

}




