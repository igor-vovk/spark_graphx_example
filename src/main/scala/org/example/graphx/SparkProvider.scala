package org.example.graphx

object SparkProvider {

  val local: SparkContext = {
    val conf = new SparkConf().setAppName("recomendr").setMaster("local[2]")

    new SparkContext(conf)
  }

}
