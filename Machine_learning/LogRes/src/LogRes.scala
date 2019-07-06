import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import org.jblas.MatrixFunctions._
import scala.util.control.Breaks._
import scala.util.control._

object LogRes {
  var truecount = 0
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogRes").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\Users\\11051\\Desktop\\大数据分析\\Lab\\Lab2\\1162100430_肖锐_lab3\\code\\SUSY.txt")

//    val splits = data.randomSplit(Array(0.8,0.2),20L)
//    val train = splits(0)
//    val test = splits(1)
    val splits = data.randomSplit(Array(0.00001,0.99999),20L)
    val train = splits(0)
    val test = splits(1).randomSplit(Array(0.00001,0.99999),20L)(0)

    /**
      * 训练集训练样本，求出逻辑回归的参数w的向量值
      */
    val traindata: RDD[(Double, DoubleMatrix)] = train.map{ s=>
      val words = s.split(",")
      val label = words(0).toDouble
      val matrix = new DoubleMatrix(words.slice(1,words.length).map(_.toDouble))
      (label,matrix)
    }
//    val w = train(traindata,0.02,0.01)

    /**
      * 初始化步长，w值和精确度大小
      */
    val lambda = 0.05
    val alpha = 0.01
    val maxIterations = 300
    val num = 18
//    var w = DoubleMatrix.randn(num).muli(2).subi(1)
    var w = new DoubleMatrix(Array(2.0,-0.001,-0.005893685908310873,0.5182721002281679,-0.008924916835443449,-0.02140265242513656,5.1827494464907895,-0.007688816566230512,-0.3539297744217373,0.11390797141504329,-2.7986565794998848,-1.5964527928836791,-2.847389220265987,0.23916954401637752,0.36625768154860294,0.27398509333062343,-1.0471015815489588,1.3004450298954284))
    var error = DoubleMatrix.zeros(num)
//    var w = DoubleMatrix.zeros(num)
    val epsil = 0.01

    /**
      * 迭代过程，使用梯度下降算法
      */
    breakable{
      for(i<-1 to maxIterations){
        val gradient = traindata.map{ p=>
          p._2.mul(p._1-(1.0/(1.0 + exp(-w.dot(p._2)))))
        }.reduce(_ add _)
//        w = w.add((w.mul(-1 * lambda).add(gradient)).mul(alpha))
        w = w.add(gradient.mul(alpha))
        if(w.sub(error).norm2()<=epsil){
          break
        }else{
          error = w
        }
//        println(w)
      }
    }

    /**
      * 将上面求出的w对测试集求每一条记录的标签，并计算准确性
      */
    val testdata = test.map{s=>
      val words = s.split(",")
      val label = words(0).toDouble
      val matrix = new DoubleMatrix(words.slice(1,words.length).map(_.toDouble))
      (label,matrix)
    }
    val result = testdata.map{s=>
      //      (s._1,math.round(1 / (1 + exp(-w.dot(s._2)))))
      (s._1,math.round(1.0/(1.0+exp(-w.dot(s._2)))))  //计算sigmoid函数
    }
    for(s<-result){
      if(s._1==s._2){
        truecount+=1
      }
    }
    result.take(30).foreach(println)  //抽取30个样本进行展示
    println("accuracy:"+truecount.toDouble/testdata.count().toDouble)
  }

  def predict(testdata:RDD[(Double, DoubleMatrix)],w:DoubleMatrix): RDD[(Double,Double)] ={
    testdata.map{s=>
      //      (s._1,math.round(1 / (1 + exp(-w.dot(s._2)))))
      (s._1,1.0/(1.0+exp(w.dot(s._2))))
    }
  }

//  def train(traindata: RDD[(Double, DoubleMatrix)],alpha: Double,lambda:Double): DoubleMatrix ={
//    val maxIterations = 100
//    val num = 18
//    var w = DoubleMatrix.randn(num).muli(2).subi(1)
//    var error = DoubleMatrix.zeros(num)
//    val epsil = 1e-4
//
//    for(i<-1 to maxIterations){
//      val gradient = traindata.map{ p=>
//        p._2.mul(p._1-(1.0 / (1.0 + exp(-w.dot(p._2)))))
//      }.reduce(_ addi _)
//      w = w.add((w.mul(-1 * lambda).add(gradient)).mul(alpha))
//      if(w.sub(error).norm2()<epsil){
//        break()
//      }
//    }
//    w
//  }
}
