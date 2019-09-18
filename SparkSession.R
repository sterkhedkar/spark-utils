
# Function to initialize Spark Session from R interface.
#
# Usage:
#   Download and save this file as SparkSession.R in your current working directory.
#   Source 'SparkSession.R' to your environment.
#   call initializeSparkSession() to initialize the Spark Session with Dynamic Memory Allocation.
# 
# Arguments:
#      ARG           |      TYPE      |   DEFAULT
#   isDynamic        -     logical    -    TRUE
#   cores            -    character   -      '4'  
#   instances        -    character   -     '10'  
#   exec_memory      -    character   -     24g'  
#   driver_memory    -    character   -     '5g'  
#   memory_overhead  -    character   -     '4g'  
#
# Returns:
#   Instance of Spark Session
#
# Example:
#   source('SparkSession.R')
#   initializeSparkSession(isDynamic = FALSE, cores = '5', instances = '10', exec_memory = '20g', driver_memory = '5g')

Sys.getenv("SPARK_HOME")

old_cluster_path = 'wasb://v18devhdirserlinds-c1@v18devhdirserlindsstor.blob.core.windows.net'
app_data_path_16_17 = '/voot/mixpanel_app_data_parquet_apr2016_onwards/date_stamp='
app_data_path_18 = '/voot/mixpanel_app_data_parquet_2018/date_stamp='

temp_container_path = "wasb://vooteventbasedata@v18devhdirserlindsstor.blob.core.windows.net/"
app_data_sep_dec_18 = paste(temp_container_path, "date_stamp=",sep="")


initializeSparkSession <- function(isDynamic=TRUE, appName="sparkr_session", 
                                   instances='10', cores='10', exec_memory='24g', 
                                   driver_memory='4g', memory_overhead='2g', 
                                   parquet_compression_codec="snappy", 
                                   max_file_read_size_bytes="134217728",dynamic_allocation_enabled=T, shuffle_service_enabled=T){
  
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  library(SparkR)
  spark_pkgs='com.databricks:spark-csv_2.10:1.3.0'
  spark_shuffle_size=1000L
  spark_env=list(spark.executor.instances=instances,
                 spark.executor.cores=cores,
                 spark.executor.memory=exec_memory,
                 spark.driver.memory=driver_memory, 
                 spark.sql.parquet.compression.codec=parquet_compression_codec,
                 spark.files.maxPartitionBytes=max_file_read_size_bytes,
                 spark.dynamicAllocation.enabled=dynamic_allocation_enabled,
                 spark.shuffle.service.enabled=shuffle_service_enabled, 
                 spark.dynamicAllocation.minExecutors='1',
                 spark.dynamicAllocation.maxExecutors=instances)
  if(isDynamic){ 
    spark_session=sparkR.session(sparkPackages=spark_pkgs,appName = appName,
                                 spark.sql.shuffle.partitions=spark_shuffle_size)} 
  else { 
    spark_session=sparkR.session(sparkConfig=spark_env,
                                 appName=appName,
                                 sparkPackages=spark_pkgs,
                                 spark.sql.shuffle.partitions=spark_shuffle_size)}
  return(spark_session)
}
