package wc

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\nwc.PageRank <output dir> <k> <iterations> " +
        "<damping-factor>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Page Rank")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val k = args(1).toInt
    val countNodes = k*k
    val dampFactor = args(3).toDouble
    val iterations = args(2).toInt
    val nodes = 0 to countNodes
    val nodesRDD = sc.parallelize(nodes)
    val pageValue = 1.toDouble/(countNodes)



    var index = 0;
    val eachLineNodes = countNodes.toInt/Math.sqrt((countNodes.toInt))
    var graphList: List[(Int, Int)] = List()
    while(index <= countNodes) {
      if(index == 0) {
        var j = 1;
        while(j <= countNodes) {
          graphList = graphList :+ (index, j)
          j = j+1;
        }
      } else if(index % eachLineNodes == 0 ){
        graphList = graphList :+ (index, 0)
      } else {
        graphList = graphList :+ (index, index + 1)
      }
      index = index + 1;
    }

    val graphRDD = sc.parallelize(graphList)
    val adjRDD = graphRDD.groupByKey().map(each => (each._1, each._2.size))


    // Setting the initial ranks for the nodes
    var rankRDD = nodesRDD.map(eachNode => (if (eachNode == 0) {
      (eachNode, 0.toDouble)
    } else {
      (eachNode, pageValue.toDouble)
    }))

    var ind: Int =  0;
    while(ind < iterations) {
      println("each iteration", ind)
      val proportionRDD = rankRDD.join(adjRDD)
        .map(each =>
          (each._1, (each._2._1 / each._2._2))
        )

      val joinedRDD = graphRDD.join(proportionRDD)
      val finalRDD = joinedRDD.map(each => (each._2._1, each._2._2)).reduceByKey(_ + _)
      rankRDD = finalRDD.mapValues(eachValue => (1 - dampFactor).toDouble / countNodes + dampFactor.toDouble * eachValue)
        .cache();
//      val valueItemRDD = rankRDD.lookup(0)
//      println(valueItemRDD, "show")
      ind = ind + 1;
    }
    println("tata")
    println(rankRDD.toDebugString)
    val finalItemsRDD = rankRDD.sortByKey()
//    finalItemsRDD.saveAsTextFile(args(0))
  }
}