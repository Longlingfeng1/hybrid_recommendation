package recommender

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import recommender.recLib.{calculateAllCosineSimilarity, trainTestDataGen}

import scala.collection.Map

object videoSim {
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

    val model = ALS.trainImplicit(trainData, recConfig.rank, recConfig.numIterations, recConfig.lambda, recConfig.alpha)
    calculateAllCosineSimilarity(model,videoIdMapVideo,10)
  }

}
