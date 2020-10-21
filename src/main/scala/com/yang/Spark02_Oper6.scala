package com.yang

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * <p>@ProjectName:sparkscalastady</p>
  * <p>@Package:com.yang</p>
  * <p>@ClassName:Spark02_Oper6</p>
  * <p>@Description:${description}</p>
  * <p>@Author:yang</p>
  * <p>@Date:2020/10/14 10:25</p>
  * <p>@Version:1.0</p>
  */
object Spark02_Oper6 {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("Spark02_Oper3").setMaster("local[*]")

        //创建Spark上下文对象
        val sc: SparkContext = new SparkContext(conf)

        val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8))

        val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(x => x % 2)

        groupByRDD.collect().foreach(println)

    }

}
