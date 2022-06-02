# 2022-Spring-Team2-project2
## Description

There are two programs, one is under DataAnalyzer and the other in DataGeneration. DataGenerations creates 10,000 random global purchase records. Records are stored in a CSV file. A CSV file can be analyzed with DataAnalyzer. We wanted to visualize 10,000 records from a CSV file. We processed the data and queried from the CSV using the Spark API.
## Technologies

 - Scala
 - Spark
 - VsCode
 - GitHub
 
 ## Features
 
 - Generates 10,000 unique records 
 - Writes records to CSV file
 - Read in CSV file using Spark 
 - Process and perform queries using Spark 
 
TODO
 - Improve DataAnalyzer performance
 - Change created filenames to something meaningful  
 - Refactor code
 
 ## SETUP
 
 - Install JDK [HOW TO](https://docs.oracle.com/en/java/javase/11/install/installation-jdk-microsoft-windows-platforms.html#GUID-61460339-5500-40CC-9006-D4FC3FBCFC0D)
 -   git clone https://github.com/Michael-A-Ortega/2022-Spring-Team2-project2.git
 - Setup for DataGeneration
	 - In the terminal: cd DataGeneration
	 - In the terminal: sbt
	 - finally type run
	 - eCommerce.csv should be under DataGeneration
 - Setup for DataAnalyzer
	 - In the terminal: cd DataAnalyzer
	 - In the terminal: sbt
	 - finally type run
	 - You should have a new folder called outputs which will include query results.
## Contributors
 - [Joseph Chang](https://github.com/CodeMonkey97)
 - [Ivan Ildefonso](https://github.com/ivanildefonso)
 - [Shaun Jackson](https://github.com/shaunLjackson)
 - [Carolyn Tindi](https://github.com/Caroltin)
 - [Michael Ortega](https://github.com/Michael-A-Ortega)