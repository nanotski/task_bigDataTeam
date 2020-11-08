import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main {
  def main(args: Array[String]): Unit = {

//    //initialization
//    val pathToDB = "jdbc:sqlite:/C:/Users/www/Desktop/SQLite/testDatabase.db"

    val startDate = 20200921 // format = yyyymmdd
    val endDate = 20200930 // format = yyyymmdd
    val csv_path = "/C:/Users/www/Desktop/data.csv"
    val exportTableName = "testTable"

    var etlObj = ETL
    val dataFrames = etlObj.extract(startDate, endDate) // list(0) -> clients, list(1) -> transactions+currency

    val aggResult1=etlObj.transform1(dataFrames(0), dataFrames(1))
    val aggResult2=etlObj.transform2(dataFrames(0),dataFrames(1))

    etlObj.loadData(aggResult1,exportTableName,1) // mode = 1 -> Hive Metastore
    etlObj.loadData(aggResult2,csv_path,0) // mode !=1 -> .csv

//    aggResult1.show()
//    aggResult2.show()



  }


}
