package recommender

import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.math

object alsRecommender {

  val trainDatapath = "data/noisyDelete/score_child_nolimit.txt"
  val rank = 20 // feature数量
  val numIterations = 20 //模型迭代次数
  val lambda = 0.01 //正则项参数
  val alpha = 10 //置信度

  def NowTime(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("MM-dd HH:mm")
    val date = dateFormat.format(now)
    return date
  }

  //TODO:KFold
  def KFold(data :RDD[Rating], model: MatrixFactorizationModel, numFold: Int): Unit = {

  }

  def printRec(data: RDD[Rating], model: MatrixFactorizationModel, videoIdMapName:Map[String, String], recNum:Int, watchUserNum:Int): Unit = {

    val logFilePath = "watched-recommend_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath,true)

    fileWriter
      .write(s"输入数据:$trainDatapath\nrank=$rank\nnumIterations=$numIterations\nlambda=$lambda\nalpha=$alpha\n\n")

    val watchHistoryDict = data
      //每条记录转为元组
      .map{ x=>
        val videoPair = (videoIdMapName.getOrElse(x.product.toString,"keyError"),x.rating.formatted("%.3f"))
        (x.user,List(videoPair))
      }
      //按照用户id聚合list
      .reduceByKey((x,y)=>x++y)
      .sortByKey()
      .map{ case(userID , watchList)=>
        (userID, watchList.sortBy{ case (videoID, score) => score.toDouble }.reverse)
      }
      .collectAsMap()

    //选取前N个用户
    val userIDList = data.map(x=>x.user).distinct().sortBy(x=>x).take(watchUserNum)

    for (user <- userIDList){

      val watchHistory = (watchHistoryDict.get(user)).get
        .map{ case(videoName,score) => videoName+","+score }.mkString("|")

      val recommendList = model.recommendProducts(user,20)
        .map(x => (videoIdMapName.getOrElse(x.product.toString, "keyError"), x.rating))
          .map{case (videoName, score)=>videoName+","+score.formatted("%.3f")}.mkString("|")

      val s = s"用户ID:$user\n历史观看:\n$watchHistory\n推荐结果:\n$recommendList\n"
      println(s)
      fileWriter.write(s+"\n")
    }
    fileWriter.close()
//      //在分布式环境下得到对所有用户推荐结果，一次性生成全部的结果
//    val recommendRDDTemp: RDD[(Int, Array[Rating])] = model.recommendProductsForUsers(recNum)
//    val recommendRDD = recommendRDDTemp.map{case (userID, ratingArray) =>
//        val recommendList = ratingArray.map(x => (videoIdMapName.getOrElse(x.product.toString, "keyError"), x.rating))
//        (userID, recommendList)
//    }
//
//    //对两个RDD进行join操作
//    watchHistoryRDD.join(recommendRDD).map{case(userID,(watchHistory, recommendList)) =>
//      (userID, watchHistory, recommendList)
//    }
//    .map{ case (userID, watchHistory, recomendList) =>
//      userID
//      ":\n历史观看记录:\n" + watchHistory.map(x=>x._1+","+x._2).mkString("|") +
//      "\n推荐结果:\n" + recomendList.map(x=>x._1+","+x._2).mkString("|") + "\n"
//    }.take(12000)
//    .foreach(println)
  }

  def parameterSearch(data:RDD[Rating]):Unit = {
    val logFilePath = "parameterSearch_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath, true)
    //rating处理方式
    val logRating = data.map(r=>Rating(r.user, r.product, math.log1p(r.rating))).persist()
    fileWriter
      .write(s"输入数据:$trainDatapath\nrating处理方式:log1p\n") // <- 记得这里一起修改rating处理方式

    val Array(trainData, testData) = data.randomSplit(Array(0.8, 0.2))
    trainData.persist()
    testData.persist()

    val evaluations =
      for (rank   <- Array(10, 50);
           iterations <- Array(10, 20);
           lambda <- Array(10, 0.01);
           alpha  <- Array(1.0, 40.0);
          )
        yield {
          val model = ALS.trainImplicit(trainData, rank, iterations, lambda, alpha)
          val metricsRes = rankingMetrics(testData, model,20)
          unpersist(model)
          ((rank, iterations, lambda, alpha), metricsRes)
        }

    evaluations.foreach{ x=>
      println(x)
      fileWriter.write(x._1.toString()+"=>")
      fileWriter.write(x._2+"\n")
    }
    fileWriter.close()
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }


  def rankingMetrics(testData:RDD[Rating], model: MatrixFactorizationModel, recNum:Int):String = {

//    val logFilePath = "rankingMetrics_" + NowTime() + ".log"
//    val fileWriter = new FileWriter(logFilePath,true)
//
//    fileWriter
//      .write(s"输入数据:$trainDatapath\nrank=$rank\nnumIterations=$numIterations\nlambda=$lambda\nalpha=$alpha\n\n")
    // 用户的观看记录映射到0，1，如果阈值大于0.5就认为是感兴趣

    val binarizedRatings = testData.map(r => Rating(r.user, r.product,
      if (r.rating > 0.5) 1.0 else 0.0)).persist()
    // 保留原始观看记录，如果阈值大于0.5就认为是感兴趣
    val ruiRatings = testData.filter(_.rating > 0.5).persist()


    //得到对每个用户的N个推荐结果
    val userRecommended = model.recommendProductsForUsers(recNum)

    //将用户的观看记录与推荐结果join，并且删去不感兴趣的节目
    val binarizedWatchHistory = binarizedRatings.groupBy(_.user)
    val ruiWatchHistory = ruiRatings.groupBy(_.user).persist()
    val coutUserNum = ruiWatchHistory.count()

    //这里有可能testSet的userID是少于recommendList的，但是join操作按照少的来join，所以没问题
    val binarizedRelevantDocuments = binarizedWatchHistory.join(userRecommended).map { case (user, (actual,
    predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray) //actual 有可能为空
    }.persist()

    //《Collaborative Filtering for Implicit Feedback Datasets》评测方式
    val rankScore = ruiWatchHistory.join(userRecommended).map{ case(user, (actual, predictions)) =>
      val preSum = predictions.length
      var rateList=for(i <- 0 to preSum) yield i/preSum.toDouble
      val productScoreMap = predictions.map(x=>x.product).zip(rateList).toMap
      actual.map{x=>
        val score:Double = productScoreMap.getOrElse(x.product,1)
        score * x.rating
      }.sum / actual.map(x=>x.rating).sum
    }.reduce((x,y)=>x+y) / coutUserNum.toDouble


    var resStr:String = ""

    val rankScoreStr = s"rankScore = ${(rankScore * 100).formatted("%.3f")}\n"
    println(rankScoreStr)
    resStr += rankScoreStr
//    fileWriter.write(rankScoreStr + "\n")
//    fileWriter.write("\n")

    val metrics = new RankingMetrics(binarizedRelevantDocuments)
    // Precision at K
    Array(1, 3, 5, 10).foreach { k =>
      val pak = s"Precision at $k = ${metrics.precisionAt(k)}\n"
      println(pak)
      resStr += pak
//      fileWriter.write(pak+"\n")
    }
//    fileWriter.write("\n")
    // MAP
    val MAP = s"Mean average precision = ${metrics.meanAveragePrecision}\n"
    println(MAP)
    resStr += MAP
//    fileWriter.write(MAP+"\n")
//    fileWriter.write("\n")
    // NDCG at K
    Array(1, 3, 5, 10).foreach { k =>
      val NDCGK = s"NDCG at $k = ${metrics.ndcgAt(k)}\n"
      println(NDCGK)
      resStr += NDCGK
//      fileWriter.write(NDCGK+"\n")
    }
    return resStr
//    fileWriter.close()
  }

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("ALS").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 加载数据
    val data = sc.textFile(trainDatapath)
    val videoIdMapVideo:Map[String,String] = sc.textFile("data/t_video.txt")
      .map(x=>(x.split("#")(0),x.split("#")(1)))
      .collectAsMap()
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })

    parameterSearch(ratings)

//    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
//
//    rankingMetrics(ratings, model,20)

//    printRec(ratings,model, videoIdMapVideo,20,12000)

//    model.save(sc, "target/tmp/myCollaborativeFilter")
//    val model = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
//    model.userFeatures.persist()
//    model.productFeatures.persist()
//    rankingMetrics(ratings, model,20)
  }

}
