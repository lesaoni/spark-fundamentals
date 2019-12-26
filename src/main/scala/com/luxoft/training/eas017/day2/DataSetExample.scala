package com.luxoft.training.eas017.day2

import org.apache.spark.sql.SparkSession


object DataSetExample extends App {

  val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("SQL example scala")
      .getOrCreate()
  import spark.implicits._

  val sc = spark.sparkContext

  //Creating a dataset
  val dataset = Seq(1, 2, 3).toDS()

  case class Person(name: String, age: Int)

  val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
  personDS.show()

  //Create from dataframe
  case class Company(name: String, foundingYear: Int, numEmployees: Int)
  val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
  val df = sc.parallelize(inputSeq).toDF()

  val companyDS = df.as[Company]
  companyDS.show()


  // Word count example
  val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
  val groupedDataset = wordsDataset.flatMap(_.toLowerCase.split(" "))
                                 .filter(_ != "")
                                 .groupBy("value")
  val countsDataset = groupedDataset.count()
  countsDataset.show()

  //Join Datasets
  case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
  case class Department(id: Int, name: String)

  case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
  case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

  val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()

  val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()

  val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

  val employeeDataset = employeeDataSet1.union(employeeDataSet2)

  def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
    val (total, count) = iterator.foldLeft(0.0, 0.0) {
        case ((total, count), x) => (total + x.salary, count + 1)
    }
    ResultSet(key._1, key._2, total/count)
  }

  val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
                                            .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
                                            .filter(record => record.age > 25)
                                            .groupBy($"departmentId", $"departmentName")
                                            .avg()

  averageSalaryDataset.show()

}
