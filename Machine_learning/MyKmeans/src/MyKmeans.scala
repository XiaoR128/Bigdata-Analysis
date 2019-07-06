
import scala.util.Random

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


class MyKMeans (
                       private var k: Int,
                       private var maxIterations: Int,
                       private var epsilon: Double,
                       private var seed: Long,
                       private var distanceMeasure: String) extends Serializable{

  private def this(k: Int, maxIterations: Int, epsilon: Double, seed: Long) =
    this(k, maxIterations,
      epsilon, seed, DistanceMeasure.EUCLIDEAN)

  val pp = new BLA()


  def this() = this(2, 20, 1e-4, Random.nextLong(), DistanceMeasure.EUCLIDEAN)

  /**
  设置K的数量
   */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /**
  设置最大迭代次数
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }


  /**
    * Set the distance threshold within which we've consider centers to have converged.
    * If all centers move less than this Euclidean distance, we stop iterating one run.
    */
  def setEpsilon(epsilon: Double): this.type = {
    require(epsilon >= 0,
      s"Distance threshold must be nonnegative but got ${epsilon}")
    this.epsilon = epsilon
    this
  }


  /**
  初始化MyKmeans模型
   */
  var initialModel: Option[MyKMeansModel] = None


  /**
    * Train a K-means model on the given set of points; `data` should be cached for high
    * performance, because this is an iterative algorithm.
    */

  def run(
           data: RDD[Vector]): MyKMeansModel = {

    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist() //将二范数缓存到内存中

    //内部类的格式为向量和二范数的格式
    val zippedData = data.zip(norms).map {
      case(v,norm)=> new VectorWithNorm(v, norm)
    }
    //调用runalgorithm的方法计算模型
    val model = runAlgorithm(zippedData)

    norms.unpersist() //去掉在内存中的缓存。

    model
  }

  /**
    * K-means算法的实现
    */
  def runAlgorithm(
                    data: RDD[VectorWithNorm]): MyKMeansModel = {

    val sc = data.sparkContext

//    val initStartTime = System.nanoTime()

    val distanceMeasureInstance = DistanceMeasure.decodeFromString(this.distanceMeasure)

    val centers = initialModel match {
      case None =>
        initRandom(data)
    }


    var converged = false
    var cost = 0.0
    var iteration = 0

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < maxIterations && !converged) {
      //存储的是每一个并行度下的cost的累加值的初始值，其初始值为0.0. 每个并行计算的kmeans任务会对这个值进行累加。进而计算cost
      val costAccum = sc.doubleAccumulator
      val bcCenters= sc.broadcast(centers)

      // Find the new centers
      val collected = data.mapPartitions { points =>
        val thisCenters = bcCenters.value
        val dims = thisCenters.head.vector.size
        val sums = Array.fill(thisCenters.length)(Vectors.zeros(dims)) //其他点距离第j个中心点的距离
        val counts = Array.fill(thisCenters.length)(0L) //聚类中心数量
        points.foreach { point =>
          val (bestCenter, cost) = distanceMeasureInstance.findClosest(thisCenters, point)
          costAccum.add(cost)
          distanceMeasureInstance.updateClusterSum(point, sums(bestCenter))
          counts(bestCenter) += 1
        }

        counts.indices.filter(counts(_) > 0).map(j => (j, (sums(j), counts(j)))).iterator
      }.reduceByKey { case ((sum1, count1), (sum2, count2)) =>
        pp.axpy(1.0, sum2, sum1)
        (sum1, count1 + count2)
      }.collectAsMap()

      val newCenters = collected.mapValues { case (sum, count) =>
        distanceMeasureInstance.centroid(sum, count)
      }

      bcCenters.destroy()

      // Update the cluster centers and costs
      converged = true
      newCenters.foreach { case (j, newCenter) =>
        if (converged &&
          !distanceMeasureInstance.isCenterConverged(centers(j), newCenter, epsilon)) {
          converged = false
        }
        centers(j) = newCenter
      }

      cost = costAccum.value
      iteration += 1
    }


    new MyKMeansModel(centers.map(_.vector), distanceMeasure, cost, iteration)
  }

  /**
    * 初始化，随机生成k个中心点
    */
  def initRandom(data: RDD[VectorWithNorm]): Array[VectorWithNorm] = {
    data.takeSample(false, k, new Random(this.seed).nextInt())
      .map(_.vector).distinct.map(new VectorWithNorm(_))
  }
}


/**
  * Mykmeans对象
  */
object MyKMeans {

  /**
    * 训练模型
    */
  def traindata(
             data: RDD[Vector],
             k: Int,
             maxIterations: Int): MyKMeansModel = {
    new MyKMeans().setK(k)
      .setMaxIterations(maxIterations)
      .run(data)
  }
}

/**
  * 将向量和范数封装为一个类
  * 格式: (向量，向量的二范数)
  */
class VectorWithNorm(val vector: Vector, val norm: Double)
  extends Serializable {

  def this(vector: Vector) = this(vector, Vectors.norm(vector, 2.0))

  def this(array: Array[Double]) = this(Vectors.dense(array))

  /** 变为密集向量 */
  def toDense: VectorWithNorm = new VectorWithNorm(Vectors.dense(vector.toArray), norm)
}
