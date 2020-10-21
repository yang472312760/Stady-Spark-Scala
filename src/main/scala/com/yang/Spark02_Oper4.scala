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
object Spark02_Oper4 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Spark02_Oper3").setMaster("local[*]")

        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

        val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

        flatMapRDD.collect().foreach(println)

    }

}
