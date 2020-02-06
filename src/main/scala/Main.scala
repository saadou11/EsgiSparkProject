import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  private val CSV = "csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkProject")
      .getOrCreate()

    def loadDataFrame(path: String, format: String) = {
      spark.read.format(format)
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(path)
    }

    val transactionData = loadDataFrame(spark.conf.get(ApplicationProperties.TRANSACTION_FILE), CSV)
    val clientsData = loadDataFrame(spark.conf.get(ApplicationProperties.CLIENTS_FILE), CSV)
    val itemsData = loadDataFrame(spark.conf.get(ApplicationProperties.ITEMS_FILE), CSV)

    val bestClient = spark.conf.get(ApplicationProperties.BEST_CLIENT)
    val bestSoldItem = spark.conf.get(ApplicationProperties.BEST_SOLD_ITEM)
    val bestProfitableItem = spark.conf.get(ApplicationProperties.BEST_PROFITABLE_ITEM)

    transactionData.groupBy("client_id")
      .count()
      .sort(desc("count"))
      .join(clientsData, transactionData("client_id") === clientsData("id"))
      .select(col("client_id").as("ID client"),
        col("count").as("Count transaction"),
        col("Name").as("Name"),
        col("First_Name").as("First Name"))
      .repartition(1)
      .write.option("header", "true")
      .csv(bestClient)

    transactionData.groupBy("Item_id")
      .count()
      .sort(desc("count"))
      .join(itemsData, transactionData("Item_id") === itemsData("id"))
      .select(col("Item_id").as("ID item"),
        col("count").as("Sold count"),
        col( "Item_Name").as("Name item"))
      .repartition(1)
      .write.option("header", "true")
      .csv(bestSoldItem)

    transactionData
      .join(itemsData, transactionData("Item_id") === itemsData("id"))
      .withColumn("profit", itemsData("Item_Selling_Price") - itemsData("Item_Buyed_Price"))
      .groupBy("Item_Name")
      .sum("profit")
      .sort(desc("sum(profit)"))
      .select(col("Item_Name").as("Name Item")
        , col("sum(profit)").as("Profit sum"))
      .repartition(1)
      .write.option("header", "true")
      .csv(bestProfitableItem)

  }
}
