package com.luxoft.training.eas017.day2

object ManualDataFrameCreationExample extends App {

  import org.apache.spark.sql._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.functions._

  val spark = SparkSession.builder()
      .master("local[*]")
      .appName("DF creation")
      .getOrCreate()
  val sc = spark.sparkContext
  import spark.implicits._

  // Creating a DataFrame
  // 1. Using toDF()
  // 1.1 from a local collection of tuples
  val seqOfPairs = Seq(
    (1, "A"),
    (2, "B"),
    (3, "C")
  )
  //colNames are optional
  val dfFromSeqOfPairs = seqOfPairs.toDF("number", "letter")

  // 1.2 from an RDD of tuples
  val rddOfPairs =  sc.parallelize(seqOfPairs)

  val dfFromRdd = rddOfPairs.toDF()

  //1.3 from a list of case classes
  case class ExampleDF(number : Int, letter : String)

  val caseClasses = Seq(
    ExampleDF(1, "A"),
    ExampleDF(2, "B"),
    ExampleDF(3, "C")
  )
  val dfFromCaseClasses = caseClasses.toDF() //ColumnNames can be redefined

  // toDF() is limited because
  //  - the column type and nullable flag cannot be customized
  //  - requires import spark.implicits._ statement can only be run inside of class definitions when the Spark Session is available
  // 2. createDataFrame() method allows schema customization

  val data = Seq(
    (1, "A", 1.23),
    (2, "B", 1.25),
    (3, "C", 2.12)
  )

  val rdd = sc.parallelize(data)
  
  val schema = StructType(
    Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("value", DoubleType, nullable = true)
    )
  )

  val rows = rdd.map(rowTuple => Row(rowTuple._1, rowTuple._2, rowTuple._3))

  val df = spark.createDataFrame(rows, schema)

  df.createOrReplaceTempView("myTable")

  spark.sql("SELECT name FROM table1 WHERE")



}
