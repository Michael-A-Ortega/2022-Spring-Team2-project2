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

        spark.sparkContext.setLogLevel("OFF")
        
           
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
            StructField("datetime", TimestampType),
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
        ecomerce_data.cache()

        val grouped_items_per_country = spark.sql("SELECT country, first(prod_category) as prod_category, prod_name,SUM(quantity) as n_units " +
          "FROM ecomerce_data " +
          "GROUP BY prod_name, country " +
          "ORDER BY country, n_units DESC")
        grouped_items_per_country.createOrReplaceTempView("grouped_items_per_country")

        //grouped_items_per_country.coalesce(1).write.mode("overwrite").csv("outputs/items_per_country/")

        val gb_categories_countries = spark.sql("SELECT country, prod_category, sum(n_units) as n_units " +
          " FROM grouped_items_per_country " +
          "Group By prod_category, country " +
          "ORDER BY country, n_units DESC")

        gb_categories_countries.createOrReplaceTempView("gb_categories_countries")

        //gets max per country
        /*spark.sql("SELECT country, MAX(n_units) " +
          "FROM gb_categories_countries " +
          "GROUP BY country " +
          "ORDER BY country")*/
      gb_categories_countries.cache()
        //first marketing q
      val question_1_querey = spark.sql("SELECT gb.country, gb.prod_category, gb.n_units " +
        "FROM gb_categories_countries AS gb JOIN (" +
        "SELECT country, MAX(n_units) as units " +
        "FROM gb_categories_countries " +
        "GROUP BY country " +
        "ORDER BY country) AS mic " +
        "ON gb.country = mic.country and gb.n_units = mic.units")
      

        question_1_querey.createOrReplaceTempView("question_1_querey")
        question_1_querey.coalesce(1).write.option("header",true).mode("overwrite").csv("outputs/top_category_per_country/")

        

        //
        //quetion  2 How does the popularity of products change throughout the year? Per Country?
        
        val question_2_querey = spark.sql("SELECT country, MONTH(datetime) as Month ,prod_name, SUM(quantity) AS `Units Sold` " +
          "FROM ecomerce_data " +
          "GROUP BY country, prod_name, Month " +
          "ORDER BY country, Month")

 

        question_2_querey.createOrReplaceTempView("question_2_querey")
        question_2_querey.coalesce(1).write.option("header",true).mode("overwrite").csv("outputs/products_change_monthly/")
      

        /*
            Question 3
            Which locations see the highest traffic of sales
        */
        val question_3 = spark.sql("SELECT country, count(*) as Total_Transactions " +
        "FROM ecomerce_data " +
        "GROUP by country " +
        "ORDER BY Total_Transactions DESC")

        question_3.createOrReplaceTempView("question_3")
        question_3.coalesce(1).write.option("header",true).mode("overwrite").csv("outputs/highest_traffic_of_sales/")


        //Question 4
        //What times have the highest traffic of sales? Per Country?
        
        //highest traffic of sales global
        /*println("Global Top 10 highest traffic times")
        spark.sql("SELECT  date_format(datetime, 'MMM y') as `Month Year`, date_format(datetime, 'h a') as Hour,  COUNT(*)  as Sales " +
          "FROM ecomerce_data " +
          "WHERE datetime IS NOT NULL " +
          "GROUP BY Hour, `Month Year` " +
          "ORDER BY Sales DESC")*/

      //highest traffic of sales per country 
      val countries_grouped_ht = spark.sql(" SELECT country, date_format(datetime, 'MMM') as Month, COUNT(*) as Sales " +
          "FROM ecomerce_data " +
          "WHERE datetime IS NOT NULL " +
          "GROUP BY Month, country " +
          "ORDER BY Sales DESC, Country")
      countries_grouped_ht.createOrReplaceTempView("countries_grouped_ht")

      val question_4 = spark.sql("SELECT max_ht.country, Month, max_ht.Sales " +
        "FROM countries_grouped_ht AS cms " +
        "JOIN (SELECT country, MAX(sales) AS Sales " +
        "FROM countries_grouped_ht " +
        "GROUP BY country " +
        "ORDER BY Sales DESC) as max_ht " +
        "ON max_ht.Sales = cms.Sales and max_ht.country = cms.country")
      
      question_4.coalesce(1).write.option("header",true).mode("overwrite").csv("outputs/traffic_of_sales_per_country/")
      spark.stop()
    }
}
