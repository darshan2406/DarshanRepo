package com.demo.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.slf4j.Logger;
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory;
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
object PercentageHarvest {
  val logger = LoggerFactory.getLogger(this.getClass.getName);
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark ETL Job")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    sparkContext.setLogLevel("ERROR")
    
    val usHarvestDf = sqlContext.read
                  .format("com.databricks.spark.csv")
                  .option("header", "true")
                  .option("delimiter", ",")
                  .option("inferSchema", "true")
                  .load("C:\\Users\\63580.AD\\Downloads\\US_Harvest_Data.csv")
                
  val woldHarvestDf = sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .load("C:\\Users\\63580.AD\\Downloads\\World_Harvest_Data.csv")
                
//  val avgUSHarvestDf = woldHarvestDf.join(usHarvestDf).agg((sum(usHarvestDf(" harvest")).divide(sum(woldHarvestDf(" harvest"))).multiply(100)))
  val windowYeardf = Window.partitionBy(woldHarvestDf("year"))
  val avgUSharvestDf  = woldHarvestDf.join(usHarvestDf,woldHarvestDf("year")===usHarvestDf("year"),"inner").select(woldHarvestDf("year"), woldHarvestDf(" harvest"), usHarvestDf(" harvest")).groupBy(woldHarvestDf("year"),woldHarvestDf(" harvest"),usHarvestDf(" harvest")).agg((usHarvestDf(" harvest").divide(woldHarvestDf(" harvest"))).multiply(100).as("usa_barley_contribution%")).drop(usHarvestDf(" harvest")).dropDuplicates().orderBy("year") 
 avgUSharvestDf.show()
  
  }
  
}