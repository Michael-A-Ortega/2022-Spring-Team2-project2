import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types.{StructType, StructField, StringType,TimestampType}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{col, when}

object Querey{
    def main(args: Array[String]): Unit = {
        val spark = 
            SparkSession
            .builder
            .appName("Querey")
            .config("spark.master", "local")
            .config("spark.eventLog.enable", "false")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        
           
        val schema = StructType(Array(
            StructField("order_id", IntegerType),
            StructField("customer_id", IntegerType),
            StructField("customer_name", StringType),
            StructField("prod_id", IntegerType),
            StructField("prod_name", StringType),
            StructField("prod_category", StringType),
            StructField("payment_type", StringType),
            StructField("quantity", StringType),
            StructField("price", StringType),
            StructField("datetime", StringType),
            StructField("country", StringType),
            StructField("city", StringType),
            StructField("website", StringType),
            StructField("pay_txn_id", IntegerType),
            StructField("payment_status", StringType),
            StructField("failed_note", StringType)
        ))

        val ecomerce_data = spark.read
            .format("csv")
            .option("inferSchema", "true")
            .option("charset", "UTF8")
            .schema(schema)
            .load("inputs/ecommerce-team4.csv")

        ecomerce_data.limit(10).show()
        
        
        ecomerce_data.createOrReplaceTempView("ecomerce_data")
        val countryDF = spark.sql("SELECT country, count(*) as sales FROM ecomerce_data GROUP by country")


        countryDF.coalesce(1).write.mode("overwrite").csv("outputs/country/")
        countryDF.show()

        //single country
        ///spark.sql("SELECT prod_name, quantity FROM ecomerce_data WHERE country = 'Russia' and prod_name = 'ti84' ")

        spark.sql("SELECT DISTINCT country  FROM ecomerce_data").show()
        spark.sql("SELECT prod_name, SUM(quantity) as units FROM ecomerce_data WHERE country = 'Russia' GROUP BY prod_name ORDER BY units desc").show


       
        spark.stop()
    }
}