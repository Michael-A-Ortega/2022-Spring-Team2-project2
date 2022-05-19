import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar
import java.io.File
import java.io.FileWriter
import au.com.bytecode.opencsv.CSVWriter
import java.io.{BufferedWriter, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object project2 {
        val country_codes = Map("US" -> "United States", "CN"->"China", 
        "CAN" -> "Canada", "AUS" -> "Australia", "DEU" -> "Germany",
        "JPN"  -> "Japan")

        val products = List(
	(1, 35, "Crate of Bananas", "Consumables"),
	(2, 108, "Crate of Strawberries, (18x)", "Consumables"),
	(3, 18, "Watermelon (4x)", "Consumables"),
	(4, 30, "Crate of Plastic Forks", "Utensils"),
	(5, 30, "Crate of Plastic Knives", "Utensils"),
	(6, 30, "Crate of Plastic Spoons", "Utensils"),
	(7, 50, "Set of Metal Forks", "Utensils"),
	(8, 50, "Set of Metal Knives", "Utensils"),
	(9, 50, "Set of Metal Spoons", "Utensils"),
	(10, 70, "Box of Ensure(18x), Chocolate),", "Consumables"),
	(11, 70, "Box of Ensure(18x), Vanilla),", "Consumables"),
	(12, 70, "Box of Ensure(18x), Strawberry),", "Consumables"),
	(13, 80, "Crate of Granulated Potatoes (6x)", "Consumables"),
	(14, 30, "Disposable Napkins (200x)", "Utensils"),
	(15, 70, "Crate of Tomato Sauce(6x)", "Consumables"),
	(16, 105,"Crate of Clams in Gravy (6x)", "Consumables"),
	(17, 26, "Crate of Helm's Mayonaise (4x)", "Consumables"),
	(18, 40, "Disposable Plastic Cups (150x)", "Utensils"),
	(19, 35, "Disposable Styrofoam Cups (100x)", "Utensils"),
	(20, 45, "Disposable Styrofoam Bowls (75x)", "Utensils"),
	(21, 75, "Set of 10 Metal Tongs", "Utensils"),
	(22, 65, "Set of 10 Food Serving Scoops, Small),", "Utensils"),
	(23, 70, "Set of 10 Food Serving Scoops, Medium),", "Utensils"),
	(22, 80, "Set of 10 Food Serving Scoops, Large),", "Utensils"),
	(23, 200, "Large Capacity Blender", "Device"),
	(24, 75, "Frozen Chicken, Drumsticks),", "Consumables"),
	(25, 60, "Frozen Ham, (10 pounds),", "Consumables"),
	(26, 156, "Frozen Fish, (15 pounds),", "Consumables"),  
	(27, 450, "Crate of Beef, (138 servings),", "Consumables"),
	(28, 90,"Frozen Chicken, Whole Breast),","Consumable"),
	(39, 35,"Stainless Cooking Pot Set (8x)","Utensil"),
	(40, 6,"Whole Kernel Corn (1x)","Consumable"),
	(41, 161,"Sliced Jalapenos in brine (6x)","Consumable"),
	(42, 30,"Red and Green Pepper Strips (6x)","Consumable"),
	(43, 60,"Set of Whisks (25x)","Utensil"),
	(44, 40,"Styrofoam Cup Lids, 1000x)","Utensil"),
	(45, 23,"Minced Garlic (6x)","Consumable"),
	(46, 90,"Fancy chopped Spinach (6x)","Consumable"),
	(47, 60,"Sweet Peas (6x)","Consumable"),
	(48, 71,"Diced Carrots (6x)","Consumable"),
	(49, 92,"Black Olives (6x)","Consumable"),
	(50, 65,"Chili Beans (6x)","Consumable")
)

    def payment(): (String, String) = {
        //print success or not
        val pt_inv_reasons = List("Insufficient funds", "Invalid expiration date", "Incorrect credit card number",
        "Over limit", "Expired card", "Invalid security code", "Lost card", "Unsupported card type")
        var payment_success = 0
        var successTup = ("", "")
        payment_success = Random.nextInt(2)
        if(payment_success == 0){
            successTup = ("Success", null)
        }
        else{
            successTup = ("Failed",pt_inv_reasons(Random.nextInt(pt_inv_reasons.size)))
            //+ pt_inv_reasons(Random.nextInt(pt_inv_reasons.size))
        }
        return successTup  
    }

    def generate_txn_id(): String = {
        //val payment_txn_id = ArrayBuffer[String]()
        var payment_txn_id = ""
        val now = Calendar.getInstance()
        val id = now.getTimeInMillis() + Random.nextInt(100)
        payment_txn_id += id.toString() + Random.alphanumeric.take(4).mkString

        return payment_txn_id

    }

    def generate_orderId(date:String,cust_id:String): String = {
        //United States, China, Canada, Australia, Germany, Japan, United Kingdom
        //order id: countrycode - date - 1-10000
 
        //val dates = List("20221705","20221805","20221905","2022205")
        val kys_country = country_codes.keys.toList

        return kys_country(Random.nextInt(kys_country.size)) + "-" + date.split("-").mkString + "-" + Random.alphanumeric.take(4).mkString+"-"+cust_id
    }

    def generate_datetime():String = {

        val currentTime = LocalDateTime.now()
        val format = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm")
        var timeStamp = currentTime.minusMonths(Random.nextInt(2)).minusDays(Random.nextInt(26)).minusHours(Random.nextInt(7)).minusMinutes(Random.nextInt(40))
        
        return timeStamp.format(format)
    }


    def main(args:Array[String]): Unit = {
        val customer_Last_NameList = List("Smith", "Doe", "Jackson", "Lock", "Kringler", "Davidson", "Robinson", "Jordan", "O'niel")
        val customer_First_NameList = List("Joe", "Alice", "Dave", "Bob", "Michael", "Sam", "John", "Shaun", "Omar")

        val country_cities = Map("United States" -> List("San Franciso", "San Diego", "Houston", "New York"),
        "China" -> List("Beijing", "Shanghai", "Chengdu"),
        "Canada" -> List("Quebec", "Montreal", "Vancouver", "Toronto"),
        "Australia" -> List("Sydney", "Melbourne", "Brisbane"),
        "Germany" -> List("Berlin", "Munich", "Hamburg"),
        "Japan" -> List("Tokyo", "Kyoto", "Osaka"))
        
        //Creates an array buffer 
        val names_arry = ArrayBuffer[String]()
        var sz = 81
        while(sz > 0){
            val name = customer_First_NameList(Random.nextInt(customer_First_NameList.size)) + " " + customer_Last_NameList(Random.nextInt(customer_Last_NameList.size))
            if(!names_arry.exists(_ == (name))){
                names_arry += name
                sz -=1
            }   
        }
        val persons = ArrayBuffer[Person]()
        //get country codes
        val kys_country = country_codes.keys.toList
        //creates instances of persons
        for (i <- 0 to names_arry.size-1){
            val country = country_codes(kys_country(Random.nextInt(kys_country.size)))
            val cites = country_cities(country)

            persons+= new Person(names_arry(i), i+1, country, cites(Random.nextInt(cites.size)))
        }

        make_csv(persons)

    }

    def make_csv(persons:ArrayBuffer[Person]): Unit ={
        val payment_type = List("card", "Internet Banking", "UPI", "Wallet")
        var payment_choice = payment_type(Random.nextInt(4))
        val outputfile = new BufferedWriter(new FileWriter("eCommerce.csv"))
        val csvWriter = new CSVWriter(outputfile)
        val csvFields = Array("Order_id", "Customer_id", "Customer_name", "Product_ID",
         "Product_Name", "Product_Category", "Payment Type","QTY","Price","Datetime" ,"Country", "City", "Payment Transaciton ID",
         "Payment Status", "Notes")

        var listOfRecords = new ListBuffer[Array[String]]()
        listOfRecords += csvFields
        (4, 30, "Crate of Plastic Forks", "Utensils")
        var quantity = 0
         for (i <- 0 to 10000){
            
            quantity = Random.nextInt(200)
            payment_choice = payment_type(Random.nextInt(4))
            val product = products(Random.nextInt(products.size))
            val rnd = Random.nextInt(persons.size)
            val success = payment()
            val date_time = generate_datetime()
            val order_Id = generate_orderId(date = date_time.split(" ")(0),persons(rnd).customerId.toString())
            listOfRecords += Array(order_Id, persons(rnd).customerId.toString(),
            persons(rnd).name, product._1.toString(), product._3, product._4, payment_choice.toString(),
            quantity.toString(),"$"+(quantity * product._2), date_time ,persons(rnd).country, persons(rnd).city,
            generate_txn_id(), success._1, success._2)
        
        }
        val imlist = listOfRecords.toList
        csvWriter.writeAll(imlist)
        outputfile.close()
    }
}