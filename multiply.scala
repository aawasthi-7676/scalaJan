import org.apache.spark.sql.SparkSession
object scalaPractise {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkPractiseApp")
      .config("spark.master", "local")
      .getOrCreate()

    var empFile = spark.read.format("csv")
      .option("header", "true")
      .load("C:/data/emp.csv")


    var empDF = empFile.toDF("ename","grade","sal","sex","edeptno")
	
	empDF.show(false)

  }
}