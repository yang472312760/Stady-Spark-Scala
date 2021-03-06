package com.yang

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * <p>@ProjectName:sparkscalastady</p>
  * <p>@Package:com.yang</p>
  * <p>@ClassName:Spark02_Oper8</p>
  * <p>@Description:${description}</p>
  * <p>@Author:yang</p>
  * <p>@Date:2020/10/14 10:57</p>
  * <p>@Version:1.0</p>
  */
object Spark02_Oper8 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Spark02_Oper3").setMaster("local[*]")

        //创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

        //val sampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)
        val sampleRDD: RDD[Int] = listRDD.sample(true, 4, 1)

        sampleRDD.collect().foreach(println)
    }

}
