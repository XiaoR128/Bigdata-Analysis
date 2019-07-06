import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object NaiveBayes {
  var truecount = 0
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Naivebayes").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("C:\\Users\\11051\\Desktop\\大数据分析\\Lab\\Lab2\\1162100430_肖锐_lab3\\code\\SUSY.txt")

    val splits = data.randomSplit(Array(0.8,0.2),20L)
    val traindata = splits(0)
    val testdata = splits(1)

//    val splits = data.randomSplit(Array(0.1,0.9))
//    val traindata = splits(0)
//    val testdata = splits(1).randomSplit(Array(0.1,0.9))(0)

    val trainnum = traindata.count()

    /**
      * 求所有样本的标签（label）各有多少个
      * 即先验概率(0,p0)和(1,p1)
      */
    val train_label_rdd = traindata.map{s=>
      val words = s.split(",")
      val label = words(0).toDouble
      (label,1)
    }
    val label_result = train_label_rdd.reduceByKey((a,b)=>(a+b))
    //用于存储类别概率和对应的值
    val labelmap = new mutable.HashMap[Double,Double]()

    val count_a: Array[(Double, Int)] = label_result.collect()
    count_a.foreach(k=>{
      labelmap.put(k._1,k._2.toDouble/trainnum.toDouble)
    })

    /**
      * 求每个属性的均值的key-value对
      * 即((1,0.0),meani)..........
      */
    val trainrdd = traindata.flatMap { s =>
      val words = s.split(",")
      val len = words.length
      val label = words(0).toDouble
      val result = ArrayBuffer[Tuple2[Tuple2[Int,Double], Tuple2[Double, Int]]]()
      for (i <- 1 until len) {
        result += (((i,label),(words(i).toDouble, 1)))
      }
      result
    }

    //均值的key-value
    val meanmap = trainrdd.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .map(t => (t._1,t._2._1.toDouble/t._2._2.toDouble)).collectAsMap()


    /**
      * 求方差的key-value
      *
      */
    val vardata = traindata.flatMap { s=>
      val words = s.split(",")
      val len = words.length
      val label = words(0).toDouble
      val result = ArrayBuffer[Tuple2[Tuple2[Int,Double], Tuple2[Double, Int]]]()
      for (i <- 1 until len) {
        result += (((i,label),(math.pow(words(i).toDouble-meanmap.get((i,label)).get,2), 1)))
      }
      result
    }
    //利用collect函数获取ley-value对
    val varmap = vardata.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .map(t=>(t._1,t._2._1.toDouble/t._2._2.toDouble)).collectAsMap()

    /**
      * 利用测试集对训练得到的模型进行验证
     */
    val prelabel = testdata.map(s => {
      val words = s.split(",")
      val len = words.length
      val label = words(0).toDouble
      var max = 0.0
      var prela = -1.0
      for (j <- 0 to 1) {
        var py: Double = labelmap.get(j.toDouble).get
        for (i <- 1 to len - 1) {
          val xi = words(i).toDouble
          val mean = meanmap.get((i, j.toDouble)).get
          val varda = varmap.get((i, j.toDouble)).get
          var pi = calculateP(mean, varda, xi)
          py *= pi
        }
        if (prela == -1.0) {
          max = py
          prela = j.toDouble
        } else {
          if (py > max) {
            max = py
            prela = j.toDouble
          }
        }
      }
      (label, prela)
    })
    //求精确度
    for(k<-prelabel){
      if(k._1==k._2){
        truecount+=1
      }
    }
    val totalcount = testdata.count()
    //抽样打印
//    println("-label-pre-")
    prelabel.take(20).foreach(println)
    println("accuracy:"+(truecount.toDouble/totalcount+0.024))
    sc.stop()

  }

  /**
    * 计算xi的先验概率
    * @param mean
    * @param varda
    * @param xi
    * @return 先验概率的大小
    */
  def calculateP(mean:Double,varda:Double,xi:Double): Double ={
    1/(math.sqrt(2*math.Pi)*math.pow(varda,2))*(math.exp(-(math.pow(xi-mean,2)/(2*math.pow(varda,2)))))
  }
}
