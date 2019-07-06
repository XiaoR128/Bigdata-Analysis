import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Kmeans聚类的模型
  */

class MyKMeansModel ( val clusterCenters: Array[Vector],
                    val distanceMeasure: String,
                    val trainingCost: Double,
                    val numIter: Int)
  extends Serializable{

  val distanceMeasureInstance: DistanceMeasure =
    DistanceMeasure.decodeFromString(distanceMeasure)

  val clusterCentersWithNorm =
    if (clusterCenters == null) null else clusterCenters.map(new VectorWithNorm(_))


  def this(clusterCenters: Array[Vector], distanceMeasure: String) =
    this(clusterCenters: Array[Vector], distanceMeasure, 0.0, -1)


  def this(clusterCenters: Array[Vector]) =
    this(clusterCenters: Array[Vector], DistanceMeasure.EUCLIDEAN, 0.0, -1)


  /**
    * 可以返回聚类的中心数k
    */

  def k: Int = clusterCentersWithNorm.length

  /**
    * 预测新的样本属于哪一个类
    */

  def predict(points: RDD[Vector]): RDD[Int] = {
    val bcCentersWithNorm = points.context.broadcast(clusterCentersWithNorm)
    points.map(p =>
      distanceMeasureInstance.findClosest(bcCentersWithNorm.value, new VectorWithNorm(p))._1)
  }

}


object MyKMeansModel{
}
