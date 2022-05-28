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
            StructField("quantity", IntegerType),
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

        
        
        
        ecomerce_data.createOrReplaceTempView("ecomerce_data")
        //Groups all of the products per country 
        val grouped_items_per_country = spark.sql("SELECT prod_name, SUM(quantity) AS units, cntry.country " +
          "FROM (SELECT DISTINCT country FROM ecomerce_data) as cntry JOIN ecomerce_data ON cntry.country = ecomerce_data.country " +
          "GROUP BY prod_name, cntry.country " +
          "ORDER BY cntry.country , SUM(quantity) DESC")

        //create table so we can querey from grouped items
        grouped_items_per_country.createOrReplaceTempView("grouped_items_per_country")

        //get product with the most units sold and turn it to a table so we can querey
        val max_units_country = spark.sql("SELECT  country, max(units) as units FROM  grouped_items_per_country GROUP BY country ORDER BY max(units) DESC")
        max_units_country.createOrReplaceTempView("max_units_country")

        //finally get the product name, number of units and country, 
        val max_products_per_country = spark.sql("SELECT muc.country, prod_name, muc.units FROM grouped_items_per_country AS gipc JOIN max_units_country AS muc " +
          "ON muc.country = gipc.country and muc.units = gipc.units " +
          "ORDER BY muc.units")

        max_products_per_country.coalesce(1).write.option("header",true).mode("overwrite").csv("outputs/max_units_country/")
        //max_units_country.coalesce(1).write.mode("overwrite").csv("outputs/max_units_country/")


       
        spark.stop()
    }
}
        //ecomerce_data.limit(10).show()
        //val countryDF = spark.sql("SELECT country, count(*) as sales FROM ecomerce_data GROUP by country")


        //countryDF.coalesce(1).write.mode("overwrite").csv("outputs/country/")
        //countryDF.show()

        //single country
        ///spark.sql("SELECT prod_name, quantity FROM ecomerce_data WHERE country = 'Russia' and prod_name = 'ti84' ")

        //val countryDF = spark.sql("SELECT DISTINCT country FROM ecomerce_data")
        //countryDF.show
        //spark.sql("SELECT prod_name, SUM(quantity) as units FROM ecomerce_data WHERE country = 'China' GROUP BY prod_name ORDER BY units desc").show
        //grouped_items_per_country.coalesce(1).write.mode("overwrite").csv("outputs/items_per_country/")
        //spark.sql("SELECT country, prod_name, MAX(units) FROM grouped_items_per_country GROUP BY country, prod_name").show

        //spark.sql("SELECT country, prod_name, units FROM grouped_items_per_country").show

        //WORKING QUEREY PER COUNTRY ITEMS MAX