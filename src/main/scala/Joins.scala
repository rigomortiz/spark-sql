package org.example

import org.apache.spark.sql.SparkSession

case class Employee(i: Int, str: String)
case class Department(i: Int, str: String)

object Joins extends App {
  //IdDepartment, name
  val employees = Seq(
    Employee(20, "Andrea"),
    Employee(30, "Jorge"),
    Employee(40, "Maria"),
    Employee(50, "Pedro"))

  //IdDepartment, name
  val departments = Seq(
    Department(10, "IT"),
    Department(20, "Sales"),
    Department(30, "Marketing"))

  val spark = SparkSession.builder().master("local[*]").getOrCreate();
  val sc = spark.sparkContext
  val employeesRDD = sc.parallelize(employees)
  val departmentsRDD = sc.parallelize(departments)

  val employeesDF = spark.createDataFrame(employeesRDD)
  val departmentsDF = spark.createDataFrame(departmentsRDD)
  println("Employees:")
  employeesDF.printSchema()
  employeesDF.show()

  println("Departments:")
  departmentsDF.printSchema()
  departmentsDF.show()

  println("Inner Join:")
  employeesDF.join(departmentsDF, employeesDF.col("i") === departmentsDF.col("i"), "inner")
    .show(false)

  println("Full Join:")
  employeesDF.join(departmentsDF, employeesDF.col("i") === departmentsDF.col("i"), "full")
    .show(false)

  println("Left Join:")
  employeesDF.join(departmentsDF, employeesDF.col("i") === departmentsDF.col("i"), "left")
    .show(false)

  println("Right Join:")
  employeesDF.join(departmentsDF, employeesDF.col("i") === departmentsDF.col("i"), "right")
    .show(false)

  sc.stop()
}
