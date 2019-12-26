package com.luxoft.training.eas017.day2

import org.apache.spark.sql.SparkSession

object ManualDataFrameCreationExample extends App {

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

  import org.apache.spark.sql.types._

  val schema = StructType(
    Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("value", DoubleType, nullable = false)
    )
  )

 import org.apache.spark.sql.Row

  val rows = rdd.map(rowTuple => Row(rowTuple._1, rowTuple._2, rowTuple._3))

  val df = spark.createDataFrame(rows, schema)
  
  // Manipulating the DataFrame

  df.createOrReplaceTempView("myTable")

  spark.sql("SELECT name FROM myTable WHERE id > 1")

  // df("id") === col("id") === $"id"
  import org.apache.spark.sql.functions._

  df.filter(col("id") > 1).select("name")

  // where and filter are the same
  df.where(col("id") > 1).select("name")
  
  spark.sql("SELECT * FROM myTable WHERE ID > 1 AND INSTR(Name, 'B') > 0")

  df.where(($"ID" > 1) and ($"Name" contains "B"))
  df.where($"ID" > 1 && $"Name" contains "B")

  spark.sql("SELECT * FROM myTable WHERE ID > 1 OR INSTR(Name,'B')>0")
  df.where($"ID" > 1 || $"Name" contains "B")


  spark.sql("SELECT * FROM myTable WHERE ID > 1 OR INSTR(Name,'B')>0")


  //Another df to demonstrate join, union etc.
  val additionalData = Seq(
    (4, "A", 1.23),
    (1, "D", 3.25),
    (2, "E", 6.12)
  )
  val additionalDf = spark.createDataFrame(additionalData)
    .withColumnRenamed("_1", "id")
    .withColumnRenamed("_2", "name")
    .withColumnRenamed("_3", "value")
  additionalDf.createOrReplaceTempView("myNewTable")

  df.join(additionalDf, "id")

  df.join(additionalDf, "id")

  val bigDf = df.union(additionalDf)


  bigDf.groupBy($"id").avg("value")

  bigDf.groupBy($"id").avg("value")


  //UDF example

  val goodValue = udf((rate: Double) => if (rate > 0.5) true else false)

  df.select(goodValue($"value"))

}
