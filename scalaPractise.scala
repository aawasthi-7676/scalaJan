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
    var deptFile = spark.read.format("csv")
      .option("header","true")
      .load("C:/data/dept.csv")


    var empDF = empFile.toDF("ename","grade","sal","sex","edeptno")
    val deptDF = deptFile.toDF("ddeptno","depname")
	
	empDF.show(false)

    empDF.createOrReplaceTempView("empview")
    deptDF.createOrReplaceTempView("deptview")

    val FinalDF = spark.sql("select ename,grade,sal,sex,edeptno,depname from empview e, deptview d where d.ddeptno = e.edeptno")

    FinalDF.coalesce(1).write.format("csv")
      .option("header","true") //it will give header info in o/p file
      .mode("overwrite") //can be append, overwrite
      .save("C:/data/scalaPractise.csv")

    FinalDF.show(false)
  }
}