
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
initializeSparkSession <- function(isDynamic=TRUE, appName="sparkr_session", instances='10', cores='10', exec_memory='24g', driver_memory='4g', memory_overhead='2g'){
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  library(SparkR)
  spark_pkgs='com.databricks:spark-csv_2.10:1.3.0'
  spark_shuffle_size=1000L
  spark_env=list(spark.executor.instances=instances,
                 spark.executor.cores=cores,
                 spark.executor.memory=exec_memory,
                 spark.driver.memory=driver_memory)
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





