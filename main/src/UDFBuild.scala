import org.apache.spark.sql.functions.udf
import sqlContext.implicits._
import java.util.regex.Pattern
import org.apache.spark.sql.{Row, SparkSession}

object UDFBuild extends App
{
def main(args : Array[String]): Unit = {

//Creating Spark Session Here
val spark = SparkSession
			.builder()
			.appName("Spark UDF Example")
			.config()
			.enableHiveSupport()
			.getOrCreate()
					
import spark.implicits._
import spark.sql

//creating dataframe with the results of below  SQL query .
val sqlDF = spark.sql("SELECT entity_list FROM SCHEMA_NAME.iScreen")

Calling UDF
val ExtractString = udf(toExtractString _)
sqlDF.select(ExtractString(col("entity_list"))).show()

// Using JAVA RegEx to extract pattern
def toExtractString(str: String) = {      
  val pattern = Pattern.compile("Banque",Pattern.CASE_INSENSITIVE)
  val templist = scala.collection.mutable.ListBuffer.empty[String]
  val matcher = pattern.matcher(str)
  while (matcher.find()) {
    templist += matcher.group()
  }
  templist.mkString(",").split(",").toDF
	
}

}

}


