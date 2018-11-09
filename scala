package com.vanguard.global.icsdna.exacttarget

import org.apache.spark.SparkContext

import scala.reflect.runtime.universe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ProcessExactTargetFiles extends App {

  val spark = SparkSession
    .builder()
    .appName("Loading Exact target data from raw file")
    .getOrCreate()


  // to get the input file name
  val inputFileName = args(1)

  // to get the partition date
  val filedate = args(2)

  // The folder that hive will use as this partition
  val datePartition = "file_date=" + args(2)

  // Get application.properties file path from command line
  val propertiesFile = args(0)

  val stream = args(3)

  // Broadcast application.properties file so that all worker nodes can read properties at runtime
  val props = getApplicationProperties(propertiesFile, spark.sparkContext)

  //Assign all properties to the respective variables
  val s3AccessKey = props.value.get("s3AccessKey").get
  val s3SecretKey = props.value.get("s3SecretKey").get
  val stagedb = props.value.get("stagedb").get
  val discovery_bucket_name = props.value.get("discovery_bucket_name").get
  val filter_criteria = props.value.get("filter_criteria").get
  val target_temp_tables=props.value.get("target_temp_tables").get
  val exact_target_db = props.value.get("exact_target_db").get
  val tableName=getTemptableName(inputFileName,target_temp_tables)

  // Set hadoop configuration to AWS access Key to read and write S3 Buckets
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", s3AccessKey.toString())
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", s3SecretKey.toString())

  // Add partitions to temporary table
  addTablePartition(tableName,stagedb,filedate,stream)

  // Write Processed output into Target table S3 location
  writeFinalOutputS3(filter_criteria,stagedb,exact_target_db,tableName,filedate,stream,discovery_bucket_name,datePartition)

  /* The below function reads properties file from S3 bucket and load into RDD and create Map of key-value pair. It
   broadcast the Map to workers
 */
  def getApplicationProperties(filepath: String, sc: SparkContext) = {
    val propRdd = sc.textFile(filepath)
    val mapRdd = propRdd.map(line => line.split("=")).map { tokens => (tokens(0), tokens(1)) }
    val bc_properties = sc.broadcast(mapRdd.collectAsMap())
    bc_properties

  }

  // This function return the target temporary table based on the input file name
  def getTemptableName(infile: String, temp_table_list: String) = {
    val tablename = temp_table_list.split(",").filter(x => infile.toLowerCase.contains(x))(0)
    tablename
  }

  // Format Filter criteria as SQL compatible format
  def formatFilterString(filter_criteria: String) = {
    val filter_array = filter_criteria.split(",").map(x => s"'$x'")
    var filter_string = ""
    for (str <- filter_array) {
      if (!filter_string.isEmpty) {
        filter_string += ","
      }
      filter_string += str

    }

    filter_string

  }

  // Alter table to add partition to tables
  def addTablePartition(tableName:String,dbname:String,filedate:String,stream:String) = {
    val APP_NAME = "EXACTTARGET:" + stream + ":ProcessExactTargetFiles"
    try {

      spark.sql(s"ALTER TABLE $dbname.$tableName  ADD IF NOT EXISTS PARTITION(file_date=$filedate)")
    } catch {
      case t: Throwable => {
        val logger = LoggerFactory.getLogger(ProcessExactTargetFiles.getClass)
        logger.warn("WARN:" + APP_NAME + ":failed Alter table [" + tableName + "]for the partition file_date="+ filedate + "due to Exceptions")
      }
    }

  }

  def writeFinalOutputS3(filter_criteria:String,dbname:String,exact_target_db:String,tableName:String,filedate:String,stream:String,discovery_bucket_name:String,datePartition:String) ={
    // Read stage table and filter the data based on the filter criteria from the property file

    val filter_string=formatFilterString(filter_criteria.toString())

    // Read hive stage table with filter
    val outDF = spark.sql(s"SELECT * FROM $dbname.$tableName WHERE file_date = '$filedate' " +
      s"AND SITE_NM IN ($filter_string)")
    outDF.write.mode("OverWrite").format("parquet").save(s"$discovery_bucket_name/$exact_target_db/$tableName/$datePartition")

    addTablePartition(tableName,dbname,filedate,stream)

  }



}
