package org.example

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.{max, min, count, avg, sum}

case class Product(pId: Int, name: String, price: Double, cost: Double)
object Agregacion extends App {

  val productos = Seq(
    Product(1, "Iphone", 600, 400),
    Product(2, "Galaxy", 500, 400),
    Product(3, "Ipad", 400, 300),
    Product(4, "Kindle", 200, 100),
    Product(5, "Macbook", 1200, 900),
    Product(6, "Dell", 500, 400)
  )
  val spark = SparkSession.builder().master("local[*]").getOrCreate();
  val sc = spark.sparkContext
  val rdd = sc.parallelize(productos)
  val df = spark.createDataFrame(rdd)
  println("Máximo precio por producto")
  val r1 = df.agg(max("price"))
  r1.show()
  println("Minimo precio por producto")
  val r2 = df.agg(min("price"))
  r2.show()
  println("Conteo de productos")
  val r3 = df.agg(count("pId"))
  r3.show()
  println("Promedio precio por producto")
  val r4 = df.agg(avg("price"))
  r4.show()
  println("Suma de precio por producto")
  val r5 = df.agg(sum("price"))
  r5.show()
}
