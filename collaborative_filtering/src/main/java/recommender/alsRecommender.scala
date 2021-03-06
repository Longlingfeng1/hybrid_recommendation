package recommender

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import java.io.{FileInputStream, FileWriter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.collection.Map
import org.jblas.DoubleMatrix
import recLib.evaluate
import recLib.printRec
import recLib.trainTestDataGen


object alsRecommender {

  def loadProperties():Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream("ALS.properties"))
    recConfig.trainDataPath = properties.getProperty("trainDataPath")
    recConfig.videoTable = properties.getProperty("videoTable")
    recConfig.rank = properties.getProperty("rank").toInt
    recConfig.numIterations = properties.getProperty("numIterations").toInt
    recConfig.lambda = properties.getProperty("lambda").toDouble
    recConfig.alpha = properties.getProperty("alpha").toDouble
    recConfig.logFormula = properties.getProperty("logFormula")
    recConfig.minSimilarity = properties.getProperty("minSimilarity").toDouble
  }

  def main(args: Array[String]): Unit = {
    loadProperties()
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("ALS").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // 加载数据
    val data = sc.textFile(recConfig.trainDataPath)
    val videoIdMapVideo:Map[String,String] = sc.textFile(recConfig.videoTable)
      .map(x=>(x.split(",")(0),x.split(",")(1)))
      .collectAsMap()
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val Array(trainData, testDataBinary, testDataRui) = trainTestDataGen(ratings, 0.0, false)
    trainData.persist()
    testDataBinary.persist()
    testDataRui.persist()

    val model = ALS.trainImplicit(trainData, recConfig.rank, recConfig.numIterations, recConfig.lambda, recConfig.alpha)
    //评测
    evaluate(testDataBinary,testDataRui,model)
    //打印具体推荐结果
    printRec(trainData, model, videoIdMapVideo,20,12000)
  }
}
