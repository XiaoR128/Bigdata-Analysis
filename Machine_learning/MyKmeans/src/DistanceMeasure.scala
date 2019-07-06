
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors,SparseVector}
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}

class BLA extends Serializable{
  @transient private var _f2jBLAS: NetlibBLAS = _
  @transient private var _nativeBLAS: NetlibBLAS = _

  def f2jBLAS: NetlibBLAS = {
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }
  /**
    * y += a * x
    */
  def axpy(a: Double, x: Vector, y: Vector): Unit = {
    require(x.size == y.size)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            axpy(a, sx, dy)
          case dx: DenseVector =>
            axpy(a, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(
              s"axpy doesn't support x type ${x.getClass}.")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"axpy only supports adding to a dense vector but got type ${y.getClass}.")
    }
  }

  /**
    * y += a * x
    */
  private def axpy(a: Double, x: DenseVector, y: DenseVector): Unit = {
    val n = x.size
    f2jBLAS.daxpy(n, a, x.values, 1, y.values, 1)
  }

  /**
    * y += a * x
    */
  private def axpy(a: Double, x: SparseVector, y: DenseVector): Unit = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += xValues(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }

  /**
    * dot(x, y)
    */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
        " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
    * dot(x, y)
    */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
    * dot(x, y)
    */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.length

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
    * dot(x, y)
    */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.length
    val nnzy = yIndices.length

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  /**
    * x = a * x
    */
  def scal(a: Double, x: Vector): Unit = {
    x match {
      case sx: SparseVector =>
        f2jBLAS.dscal(sx.values.length, a, sx.values, 1)
      case dx: DenseVector =>
        f2jBLAS.dscal(dx.values.length, a, dx.values, 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
  }

}


abstract class DistanceMeasure extends Serializable {
  val pp = new BLA()
  /**
    * @return the index of the closest center to the given point, as well as the cost.
    */
  def findClosest(
                   centers: TraversableOnce[VectorWithNorm],
                   point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      val currentDistance = distance(center, point)
      if (currentDistance < bestDistance) {
        bestDistance = currentDistance
        bestIndex = i
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
    * @return the K-means cost of a given point against the given cluster centers.
    */
  def pointCost(
                 centers: TraversableOnce[VectorWithNorm],
                 point: VectorWithNorm): Double = {
    findClosest(centers, point)._2
  }

  /**
    * @return whether a center converged or not, given the epsilon parameter.
    */
  def isCenterConverged(
                         oldCenter: VectorWithNorm,
                         newCenter: VectorWithNorm,
                         epsilon: Double): Boolean = {
    distance(oldCenter, newCenter) <= epsilon
  }

  /**
    * @return the distance between two points.
    */
  def distance(
                v1: VectorWithNorm,
                v2: VectorWithNorm): Double

  /**
    * @return the total cost of the cluster from its aggregated properties
    */
  def clusterCost(
                   centroid: VectorWithNorm,
                   pointsSum: VectorWithNorm,
                   numberOfPoints: Long,
                   pointsSquaredNorm: Double): Double

  /**
    * Updates the value of `sum` adding the `point` vector.
    * @param point a `VectorWithNorm` to be added to `sum` of a cluster
    * @param sum the `sum` for a cluster to be updated
    */
  def updateClusterSum(point: VectorWithNorm, sum: Vector): Unit = {
    pp.axpy(1.0, point.vector, sum)
  }

  /**
    * Returns a centroid for a cluster given its `sum` vector and its `count` of points.
    *
    * @param sum   the `sum` for a cluster
    * @param count the number of points in the cluster
    * @return the centroid of the cluster
    */
  def centroid(sum: Vector, count: Long): VectorWithNorm = {
    pp.scal(1.0 / count, sum)
    new VectorWithNorm(sum)
  }

  /**
    * Returns two new centroids symmetric to the specified centroid applying `noise` with the
    * with the specified `level`.
    *
    * @param level the level of `noise` to apply to the given centroid.
    * @param noise a noise vector
    * @param centroid the parent centroid
    * @return a left and right centroid symmetric to `centroid`
    */
  def symmetricCentroids(
                          level: Double,
                          noise: Vector,
                          centroid: Vector): (VectorWithNorm, VectorWithNorm) = {
    val left = centroid.copy
    pp.axpy(-level, noise, left)
    val right = centroid.copy
    pp.axpy(level, noise, right)
    (new VectorWithNorm(left), new VectorWithNorm(right))
  }

  /**
    * @return the cost of a point to be assigned to the cluster centroid
    */
  def cost(
            point: VectorWithNorm,
            centroid: VectorWithNorm): Double = distance(point, centroid)
}


object DistanceMeasure {


  val EUCLIDEAN = "euclidean"

  val COSINE = "cosine"

  def decodeFromString(distanceMeasure: String): DistanceMeasure =
    distanceMeasure match {
      case EUCLIDEAN => new EuclideanDistanceMeasure
      case _ => throw new IllegalArgumentException(s"distanceMeasure must be one of: " +
        s"$EUCLIDEAN, $COSINE. $distanceMeasure provided.")
    }

  def validateDistanceMeasure(distanceMeasure: String): Boolean = {
    distanceMeasure match {
      case DistanceMeasure.EUCLIDEAN => true
      case DistanceMeasure.COSINE => true
      case _ => false
    }
  }
}

class EuclideanDistanceMeasure extends DistanceMeasure {
  /**
    * @return the index of the closest center to the given point, as well as the squared distance.
    */
  override def findClosest(
                            centers: TraversableOnce[VectorWithNorm],
                            point: VectorWithNorm): (Int, Double) = {
    var bestDistance = Double.PositiveInfinity
    var bestIndex = 0
    var i = 0
    centers.foreach { center =>
      var lowerBoundOfSqDist = center.norm - point.norm
      lowerBoundOfSqDist = lowerBoundOfSqDist * lowerBoundOfSqDist
      if (lowerBoundOfSqDist < bestDistance) {
        val distance: Double = EuclideanDistanceMeasure.fastSquaredDistance(center, point)
        if (distance < bestDistance) {
          bestDistance = distance
          bestIndex = i
        }
      }
      i += 1
    }
    (bestIndex, bestDistance)
  }

  /**
    * @return whether a center converged or not, given the epsilon parameter.
    */
  override def isCenterConverged(
                                  oldCenter: VectorWithNorm,
                                  newCenter: VectorWithNorm,
                                  epsilon: Double): Boolean = {
    EuclideanDistanceMeasure.fastSquaredDistance(newCenter, oldCenter) <= epsilon * epsilon
  }

  /**
    * @param v1: first vector
    * @param v2: second vector
    * @return the Euclidean distance between the two input vectors
    */
  override def distance(v1: VectorWithNorm, v2: VectorWithNorm): Double = {
    Math.sqrt(EuclideanDistanceMeasure.fastSquaredDistance(v1, v2))
  }

  /**
    * @return the total cost of the cluster from its aggregated properties
    */
  override def clusterCost(
                            centroid: VectorWithNorm,
                            pointsSum: VectorWithNorm,
                            numberOfPoints: Long,
                            pointsSquaredNorm: Double): Double = {
    math.max(pointsSquaredNorm - numberOfPoints * centroid.norm * centroid.norm, 0.0)
  }

  /**
    * @return the cost of a point to be assigned to the cluster centroid
    */
  override def cost(
                     point: VectorWithNorm,
                     centroid: VectorWithNorm): Double = {
    EuclideanDistanceMeasure.fastSquaredDistance(point, centroid)
  }
}


object EuclideanDistanceMeasure {
  val pp = new BLA()
  /**
    * @return the squared Euclidean distance between two vectors computed by
    * [[org.apache.spark.mllib.util.MLUtils#fastSquaredDistance]].
    */
  def fastSquaredDistance(
                           v1: VectorWithNorm,
                           v2: VectorWithNorm): Double = {
    fastSquaredDistance(v1.vector, v1.norm, v2.vector, v2.norm)
  }
  def fastSquaredDistance(                v1: Vector,
                                          norm1: Double,
                                          v2: Vector,
                                          norm2: Double,
                                          precision: Double = 1e-6): Double = {
    val n = v1.size
    require(v2.size == n)
    require(norm1 >= 0.0 && norm2 >= 0.0)
    var sqDist = 0.0
    if (v1.isInstanceOf[DenseVector] && v2.isInstanceOf[DenseVector]) {
      sqDist = Vectors.sqdist(v1, v2)
    } else {
      val sumSquaredNorm = norm1 * norm1 + norm2 * norm2
      val normDiff = norm1 - norm2
      val precisionBound1 = 2.0 * EPSILON * sumSquaredNorm / (normDiff * normDiff + EPSILON)
      if (precisionBound1 < precision) {
        sqDist = sumSquaredNorm - 2.0 * pp.dot(v1.toDense, v2.toDense)
      } else {
        val dotValue = pp.dot(v1.toDense, v2.toDense)
        sqDist = math.max(sumSquaredNorm - 2.0 * dotValue, 0.0)
        val precisionBound2 = EPSILON * (sumSquaredNorm + 2.0 * math.abs(dotValue)) /
          (sqDist + EPSILON)
        if (precisionBound2 > precision) {
          sqDist = Vectors.sqdist(v1, v2)
        }
      }
    }
    sqDist
  }

  val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
}
