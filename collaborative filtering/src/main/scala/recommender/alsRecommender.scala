package recommender

import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

import scala.collection.Map


object alsRecommender {

  val trainDatapath = "data/rawLog/score_adult_nolimit_log.txt"
  val rank = 20 // feature数量
  val numIterations = 20 //模型迭代次数
  val lambda = 0.01 //正则项参数
  val alpha = 40 //置信度

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

  def rankingMetrics(data:RDD[Rating], model: MatrixFactorizationModel, recNum:Int):Unit = {

    val logFilePath = "rankingMetrics_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath,true)

    fileWriter
      .write(s"输入数据:$trainDatapath\nrank=$rank\nnumIterations=$numIterations\nlambda=$lambda\nalpha=$alpha\n\n")

    // 用户的观看记录，如果阈值大于0.5就认为是感兴趣
    val binarizedRatings = data.map(r => Rating(r.user, r.product,
      if (r.rating > 0.5) 1.0 else 0.0)).cache()

    //得到对每个用户的N个推荐结果
    val userRecommended = model.recommendProductsForUsers(recNum)

    //将用户的观看记录与推荐结果join，并且删去不感兴趣的节目
    val userMovies = binarizedRatings.groupBy(_.user)
    val relevantDocuments = userMovies.join(userRecommended).map { case (user, (actual,
    predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray)
    }

    val metrics = new RankingMetrics(relevantDocuments)

    // Precision at K
    Array(1, 3, 5, 10).foreach { k =>
      val pak = s"Precision at $k = ${metrics.precisionAt(k)}"
      println(pak)
      fileWriter.write(pak+"\n")
    }

    // MAP
    val MAP = s"Mean average precision = ${metrics.meanAveragePrecision}"
    println(MAP)
    fileWriter.write(MAP)

    // NDCG at K
    Array(1, 3, 5, 10).foreach { k =>
      val NDCGK = s"NDCG at $k = ${metrics.ndcgAt(k)}"
      println(NDCGK)
      fileWriter.write(NDCGK)
    }

    //TODO:原论文评价函数

  }


  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("ALS").setMaster("local")
    val sc = new SparkContext(conf)

    // 加载数据
    val data = sc.textFile(trainDatapath)
    val videoIdMapVideo:Map[String,String] = sc.textFile("data/t_video.txt")
      .map(x=>(x.split("#")(0),x.split("#")(1)))
      .collectAsMap()
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    }).persist()


    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

    rankingMetrics(ratings, model,20)

    //    printRec(ratings,model, videoIdMapVideo,20,12000)

//    model.save(sc, "target/tmp/myCollaborativeFilter")
//    val model = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")
//    model.userFeatures.persist()
//    model.productFeatures.persist()
  }

}
