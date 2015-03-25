package org.example.graphx

import org.apache.spark.{SparkConf, SparkContext}

object SparkProvider {

  val local: SparkContext = {
    val conf = new SparkConf().setAppName("graphx_example").setMaster("local[2]")

    new SparkContext(conf)
  }

}
