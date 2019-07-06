import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors

object mainapp {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("result.txt"))
    val writercenters = new PrintWriter(new File("centers.txt"))
    val conf = new SparkConf().setAppName("Km").setMaster("local")

    val sc = new SparkContext(conf)
    val file = sc.textFile("C:\\Users\\11051\\Desktop\\大数据分析\\Lab\\Lab2\\USCensus1990.data.txt")
    val pardata = file.map{s=>Vectors.dense(s.split(",").drop(1).map(_.toDouble))}.cache()

    val m = MyKMeans.traindata(pardata, 8, 30)
    val centers: Array[linalg.Vector] = m.clusterCenters
    val pointr = m.predict(pardata)
    val p = pointr.toLocalIterator

    for(cen<-centers){
      writercenters.println(cen.toString)
    }
    writercenters.close()

    for(c<-p){
      writer.println(c.toString)
    }
    writer.close()
  }
}
