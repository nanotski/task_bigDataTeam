import org.apache.spark
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object ETL  {
  //initialization
  val pathToDB = "jdbc:sqlite:/C:/Users/www/Desktop/SQLite/testDatabase.db"
  val conf: SparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("TestApp")
    .set("spark.driver.host", "localhost")
  conf.set("spark.testing.memory", "2147480000")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("TestApp")
    .getOrCreate()
  val sqlContext = spark.sqlContext


  def extract(startDate: Int, endDate: Int)= {

    //==========================extract Data================================
    // ============ some tables must be extracted separately ===============
    val clientData = sqlContext.read.format("jdbc")
      .options(Map("url" -> pathToDB,
        "dbtable" -> s"(SELECT * FROM Client  WHERE'${endDate}' >= EffectiveStartDate AND EffectiveEndDate>='${startDate}') AS t".format(startDate, endDate))).load()

    val transactionData = sqlContext.read.format("jdbc")
      .options(Map("url" -> pathToDB,
        "dbtable" -> s"(SELECT IBAN as AccountIBAN, Amount, CurrencyId, Date, c.* FROM Transactions t INNER JOIN Currency c ON c.Id = t.CurrencyId  WHERE Date BETWEEN '$startDate' AND '${endDate}' ) AS t")).load()


    List(clientData, transactionData)



  }


  def transform1(clientDf: DataFrame, transactionsDf: DataFrame): DataFrame = {
    val df = transactionsDf.join(clientDf
                            ,(col("IBAN") === col("AccountIban"))
                                and (col("Date") between(col("EffectiveStartDate"), col("EffectiveEndDate")))
                          ,"left")
    val aggResult = df.join(
      df.select(col("AccountIban").alias("IBAN1"))
        .groupBy("IBAN1").count().withColumnRenamed("count", "TransactionCount")
      ,col("IBAN1")===col("AccountIban")
      ,"inner")
      .select("Date","AccountIban","Amount", "CCYFrom","TransactionCount","FirstName", "LastName", "Age" )
      aggResult
  }

  def transform2(clientDf: DataFrame, transactionsDf: DataFrame): DataFrame = {
    val df = clientDf.join(transactionsDf
      ,(col("IBAN") === col("AccountIban"))
        and (col("Date") between(col("EffectiveStartDate"), col("EffectiveEndDate")))
      ,"left")

    val aggResult = df.join(
      df.select(col("IBAN").alias("IBAN1"), col("Amount"), col("Rate"))
        .withColumn("AmountInGel", col("Amount")*col("Rate"))
        .groupBy("IBAN1").avg("AmountInGel").withColumn("AvgAmountInGel", when(col("avg(AmountInGel)").isNull,0.0).otherwise(col("avg(AmountInGel)")))
      ,col("IBAN1")===col("IBAN")
      ,"inner")
      .select("IBAN","AvgAmountInGel", "FirstName", "LastName" )
    aggResult
  }


  def loadData (df: DataFrame, tablePath:String, mode:Int ): Unit ={
    if( mode ==1 ){
      df.write.mode("overwrite").saveAsTable(tablePath)
    } else {
      df.coalesce(1).write.csv(tablePath)
    }

  }

      //=========== some exercise example for building dynamic code =================
  //    val metaData = sqlContext.read.format("jdbc")
  //      .options(Map("url" -> "jdbc:sqlite:/C:/Users/www/Desktop/SQLite/testDatabase.db",
  //        "dbtable" -> "(SELECT * FROM sqlite_master) AS t")).load()

  //    val myTableNames = metaData.select("tbl_name").distinct()
  //      .map(_.getString(0)).collect()
  //    val bla = myTableNames.collect()
  //    for (t <- bla) {
  //      println(t.getString(0).toString)
  //
  //      val tableData = sqlContext.read.format("jdbc")
  //        .options(
  //          Map(
  //            "url" -> "jdbc:sqlite:/C:/Users/www/Desktop/SQLite/testDatabase.db",
  //            "dbtable" -> t.getString(0))).load()
  //
  //      tableData.show()
  //    }


}
