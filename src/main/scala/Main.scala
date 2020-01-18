import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {

  private val CSV = "csv"
  private val PATH = "src/main/resources/"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SparkProject")
      .getOrCreate()

    def loadDataFrame(path: String, format: String) = {
      spark.read.format(format)
        .option("sep", ";")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(path)
    }

    val transactionData = loadDataFrame(PATH + "transaction.csv", CSV)
    val clientsData = loadDataFrame(PATH + "clients.csv", CSV)
    val itemsData = loadDataFrame(PATH + "items.csv", CSV)


    transactionData.groupBy("client_id")
      .count()
      .sort(desc("count"))
      .join(clientsData,transactionData("client_id") === clientsData("id"))
      .select("client_id","count","Name","First_Name")
      .repartition(1)
      .write
      .csv(PATH + "plusGrosClient")

    transactionData.groupBy("Item_id")
      .count()
      .sort(desc("count"))
      .join(itemsData,transactionData("Item_id") === itemsData("id"))
      .select("Item_id","count","Item_Name")
      .repartition(1)
      .write
      .csv(PATH + "produitsPlusVendu")

    transactionData
      .join(itemsData,transactionData("Item_id") === itemsData("id"))
      .withColumn("profit",itemsData("Item_Selling_Price") - itemsData("Item_Buyed_Price"))
      .groupBy("Item_Name")
      .sum("profit")
      .sort(desc("sum(profit)"))
      .select("Item_Name","sum(profit)")
      .repartition(1)
      .write
      .csv(PATH + "produitsQuiRapportentLePlus")

  }

}
