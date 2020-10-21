package com.yang

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * <p>@ProjectName:sparkscalastady</p>
  * <p>@Package:com.yang</p>
  * <p>@ClassName:Spark02_Oper9</p>
  * <p>@Description:${description}</p>
  * <p>@Author:yang</p>
  * <p>@Date:2020/10/14 13:50</p>
  * <p>@Version:1.0</p>
  */
object Spark02_Oper9 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Spark02_Oper3").setMaster("local[*]")

        //创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(List(2, 1, 3, 5, 6, 6, 5, 8, 9))

        val distinceRDD: RDD[Int] = listRDD.distinct()

        distinceRDD.saveAsTextFile("output")

        distinceRDD.collect().foreach(println)


    }

}
