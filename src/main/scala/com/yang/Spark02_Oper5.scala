package com.yang

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>@ProjectName:sparkscalastady</p>
  * <p>@Package:com.yang</p>
  * <p>@ClassName:Spark02_Oper3</p>
  * <p>@Description:${description}</p>
  * <p>@Author:yang</p>
  * <p>@Date:2020/10/13 10:42</p>
  * <p>@Version:1.0</p>
  */
object Spark02_Oper5 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Spark02_Oper3").setMaster("local[*]")

        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

        val glomRDD: RDD[Array[Int]] = listRDD.glom()

        glomRDD.collect().foreach(array => {
            println(array.mkString(","))
        })


    }

}
