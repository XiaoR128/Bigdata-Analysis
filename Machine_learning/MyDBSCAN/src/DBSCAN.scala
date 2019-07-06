import java.io.{File, PrintWriter}

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DBSCAN {
  def squared(p: (Long, DenseVector[Double]), q: (Long, DenseVector[Double])): Double = {
    squaredDistance(p._2,q._2)
  }

  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("result.txt"))

    //定义半径和半径内的最少点数
    val eps = 120.0
    val minPnts = 20

    val conf = new SparkConf().setAppName("Dbscan").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("C:\\Users\\11051\\Desktop\\大数据分析\\Lab\\Lab2\\test.txt")
    val pardata = file.map{s=>
      DenseVector(s.split(",").drop(1).map(_.toDouble))
    }
    //给每一个点赋予一个编号
    val points: RDD[(VertexId, DenseVector[Double])] = pardata.zipWithUniqueId().map(x=>(x._2,x._1)).cache()

//    val pointsbehind = points.map(x=>())
    //将所有点做笛卡儿积，并且过滤掉自己与自己的结果
    val distancePoints = points.cartesian(points).filter{case(x,y) => x!=y}
    //计算出所有的两个点之间的距离
    val distanceFilter = distancePoints.map{case (x,y)=>(x,y,squared(x,y))}

    //找到所有在符合eps半径内的点对
    val pointsWithinEps = distanceFilter
      .filter { case (x, y, distance) => distance <= eps }

    //将上面的点对转化为点的ID之间的关系
    val pointsID = pointsWithinEps
      .map { case (x, y, distance) => (x._1, y._1) }

    val pointCount = pointsID.map{case (x,y)=>(x,1)}
      .reduceByKey((a,b)=>(a+b))

    //找到大于等于最小点数的点，也就是核心点
    val filterPointsCount = pointCount.filter{case (x,y)=> (y>=minPnts)}

    //所有点与核心点做join连接
    val filterPoints: RDD[(VertexId, VertexId)] = pointsID.join(filterPointsCount)
      .map{case(k,(v,w)) => (k,v)}.map { tuple => if (tuple._1 < tuple._2) tuple else tuple.swap }
      .distinct()

    //找到所有的点
    val vertices = points

    val edges: RDD[Edge[Double]] = filterPoints
      .map{case(x,y) => Edge(x,y)}

    //构建一张图
    val graph = Graph(vertices,edges)
    //求最大连通图得到的所有点
    val cluster: VertexRDD[VertexId] = graph.connectedComponents().vertices

    val before = cluster.map{case (a,b)=>(a.toInt,b.toInt)}
    val p = before.map{case (x,y)=>(y,x)}.map{case (x,y)=>(x,1)}
      .reduceByKey(_+_).filter{case (a,b)=>(b==1)}.map{case (a,b)=>(a.toInt,1000)}
    val q = before.map{case (x,y)=>(y,x)}.map{case (x,y)=>(x,1)}
      .reduceByKey(_+_).filter{case (a,b)=>(b==1)}.map{case (a,b)=>(a.toInt,a.toInt)}
    val total = before.subtract(q).union(p)

    val sort = total.sortBy(_._1)

    for(k<-sort.toLocalIterator){
      writer.println(k._2.toString)
    }
    writer.close()
  }

}