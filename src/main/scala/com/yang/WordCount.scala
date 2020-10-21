package com.yang

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

        val context: SparkContext = new SparkContext(sparkConf)

        //val fileRDD: RDD[(String)] = context.textFile("hdfs://hadoop01:9000/spark/in")
        val fileRDD: RDD[(String)] = context.textFile("in")

        val resultRDD: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        val array = resultRDD.collect()

        array.foreach(println)

        context.stop()

    }

}
