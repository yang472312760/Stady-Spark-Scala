package com.yang

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>@ProjectName:sparkscalastady</p>
  * <p>@Package:com.yang</p>
  * <p>@ClassName:Spark02_Oper1</p>
  * <p>@Description:${description}</p>
  * <p>@Author:yang</p>
  * <p>@Date:2020/10/13 9:11</p>
  * <p>@Version:1.0</p>
  */
object Spark02_Oper2 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark02_Oper1")

        val sc = new SparkContext(conf)

        val listRDD = sc.makeRDD(1 to 10)

        val mapPartitionsRDD = listRDD.mapPartitions(a => {
            a.map(_ * 2)
        })

        mapPartitionsRDD.collect().foreach(println)

    }

}
