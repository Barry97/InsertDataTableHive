package main
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.kafka.clients


object InsertData  {
  
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application")
				.setMaster("local[*]")
				val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)	
		// creation d'un DataFrame 
		val rdd = sqlContext.read.format("com.databricks.spark.csv").option("header", "false")      
      .option("delimiter", ",")
     /* val rdd = sqlContext.read.format("com.databricks.spark.csv").option("header", "false")      
      .option("delimiter", ",")*/
      .load("hdfs://sldifrdwbhd01.fr.intranet:8020/tmp/BARRY/train.csv") 
      // insertion des données dans la table mydata en mode ajout
      rdd.saveAsTable("eag_fxticks.myData", SaveMode.Append)
	}
}

