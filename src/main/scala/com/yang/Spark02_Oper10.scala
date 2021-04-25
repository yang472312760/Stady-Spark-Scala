package com.yang

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * <p>@ProjectName:sparkscalastady</p>
  * <p>@Package:com.yang</p>
  * <p>@ClassName:Spark02_Oper10</p>
  * <p>@Description:${description}</p>
  * <p>@Author:yang</p>
  * <p>@Date:2020/10/15 8:46</p>
  * <p>@Version:1.0</p>
  */
object Spark02_Oper10 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Spark02_Oper3").setMaster("local[*]")

        //创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)


        println("分区前：" + listRDD.partitions.size)

        val coalesceRDD: RDD[Int] = listRDD.coalesce(3)

        println("分区后：" + coalesceRDD.partitions.size)


    }

}
