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
import recLib.print_feature
import recLib.parameterSearch


object alsRecommender {

  def loadProperties():Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream("ALS.properties"))
    recConfig.trainDataPath = properties.getProperty("trainDataPath")
    recConfig.testDataPath = properties.getProperty("testDataPath")
    recConfig.videoTable = properties.getProperty("videoTable")
    recConfig.rank = properties.getProperty("rank").toInt
    recConfig.numIterations = properties.getProperty("numIterations").toInt
    recConfig.lambda = properties.getProperty("lambda").toDouble
    recConfig.alpha = properties.getProperty("alpha").toDouble
    recConfig.logFormula = properties.getProperty("logFormula")
    recConfig.minSimilarity = properties.getProperty("minSimilarity").toDouble
    recConfig.minrating = properties.getProperty("minrating").toDouble
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
    }).filter(x=>x.rating>recConfig.minrating)


    val data_2 = sc.textFile(recConfig.testDataPath)

    val ratings_2 = data_2.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    }).filter(x=>x.rating>recConfig.minrating)


    val Array(trainData, testDataBinary, testDataRui) = trainTestDataGen(ratings, 0, false)
    val Array(trainData_1,testDataBinary_1,testDataRui_1) = trainTestDataGen(ratings_2,0,false)
    trainData.persist()
    testDataBinary_1.persist()
    testDataRui_1.persist()

    val model = ALS.trainImplicit(trainData, recConfig.rank, recConfig.numIterations, recConfig.lambda, recConfig.alpha)

    //print_feature(model)
    //评测
    evaluate(testDataBinary_1,testDataRui_1,model)
    //evaluate(testDataBinary,testDataRui,model)
    //parameterSearch(ratings,ratings_2)
    //打印具体推荐结果
    printRec(trainData, model, videoIdMapVideo,20,12000)
  }
}
