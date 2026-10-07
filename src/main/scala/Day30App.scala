import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day30App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Final Data Engineering Capstone")
      .master("local[*]")
      .config("spark.ui.enabled","false")
      .getOrCreate()

    import spark.implicits._

    println("==============================================")
    println("DAY 30 - FINAL DATA ENGINEERING CAPSTONE")
    println("==============================================")

    val customers = Seq(
      (1,"Amit","Hyderabad"),
      (2,"Sara","Delhi"),
      (3,"Rahul","Mumbai"),
      (4,"Neha","Bangalore")
    ).toDF("customer_id","customer","city")

    val products = Seq(
      (101,"Laptop","Electronics",60000),
      (102,"Phone","Electronics",30000),
      (103,"Mouse","Accessories",800),
      (104,"Chair","Furniture",7000),
      (105,"Keyboard","Accessories",1500)
    ).toDF("product_id","product","category","price")

    val orders = Seq(
      (1001,1,101,1),
      (1002,1,103,2),
      (1003,2,102,1),
      (1004,3,104,2),
      (1005,4,105,3),
      (1006,2,101,1),
      (1007,3,103,5)
    ).toDF(
      "order_id",
      "customer_id",
      "product_id",
      "quantity"
    )

    val cleanOrders = orders
      .filter(col("quantity") > 0)
      .dropDuplicates("order_id")

    val enriched = cleanOrders
      .join(customers,Seq("customer_id"))
      .join(products,Seq("product_id"))
      .withColumn(
        "revenue",
        col("quantity") * col("price")
      )

    enriched.cache()

    println("TOTAL ORDERS:")
    println(enriched.count())

    println("CATEGORY ANALYTICS:")

    enriched
      .groupBy("category")
      .agg(
        sum("revenue").as("total_revenue"),
        sum("quantity").as("units_sold"),
        countDistinct("order_id").as("orders")
      )
      .orderBy(desc("total_revenue"))
      .show(false)

    println("CUSTOMER ANALYTICS:")

    val customerRevenue = enriched
      .groupBy("customer_id","customer","city")
      .agg(
        sum("revenue").as("total_revenue")
      )

    customerRevenue
      .orderBy(desc("total_revenue"))
      .show(false)

    println("CUSTOMER RANK:")

    val rankingWindow =
      Window.orderBy(desc("total_revenue"))

    customerRevenue
      .withColumn(
        "customer_rank",
        dense_rank().over(rankingWindow)
      )
      .show(false)

    println("TOP PRODUCTS:")

    enriched
      .groupBy("product_id","product","category")
      .agg(
        sum("revenue").as("revenue"),
        sum("quantity").as("units")
      )
      .orderBy(desc("revenue"))
      .show(false)

    println("WRITING PARQUET...")

    enriched.write
      .mode("overwrite")
      .parquet("output/enriched_orders")

    println("Parquet written successfully.")
    println("CAPSTONE COMPLETED SUCCESSFULLY")

    enriched.unpersist()
    spark.stop()
  }
}
