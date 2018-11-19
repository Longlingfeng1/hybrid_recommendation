package recommender

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

import scala.collection.Map

object recLib {

  def modifyRui(rating:Double):Double = {
    if (recConfig.logFormula == "log10(1+x)"){
      math.log10(1 + rating)
    }
    else{
      math.log(1 + rating / math.pow(10, -8))
    }
  }

  def NowTime(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("MM-dd HH:mm")
    val date = dateFormat.format(now)
    return date
  }

  //训练集，测试集生成
  def trainTestDataGen(allData:RDD[Rating], testSize:Double, isSplit:Boolean):Array[RDD[Rating]]= {
    var trainSet:RDD[Rating] = null
    var testSet:RDD[Rating] = null

    if(isSplit){
      val Array(train, test) = allData.randomSplit(Array(1.0 - testSize, testSize))
      trainSet = train
      testSet = test
    }else{
      trainSet = allData
      testSet = allData
    }

    val trainData = trainSet.map{ case Rating(user, product, rating) => Rating(user, product, modifyRui(rating)) }
    val testData = testSet

    // 用户的观看记录映射到0，1，如果阈值大于0.5就认为是感兴趣
    val testBinarizedRatings = testData.map(r => Rating(r.user, r.product,
      if (r.rating > 0.5) 1.0 else 0.0))
    // 保留原始观看记录，如果阈值大于0.5就认为是感兴趣
    val testRuiRatings = testData.filter(_.rating > 0.5)
      //数据处理方式为log1p
      .map{ case Rating(user, product, rating) => Rating(user, product, modifyRui(rating)) }

    Array(trainData, testBinarizedRatings, testRuiRatings)
  }

  //释放缓存到内存的模型
  def unpersist(model: MatrixFactorizationModel): Unit = {
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  def printRec(trainData: RDD[Rating], model: MatrixFactorizationModel, videoIdMapName:Map[String, String], recNum:Int, watchUserNum:Int): Unit = {

    val logFilePath = "log/watched-recommend_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath,true)

    fileWriter
      .write(s"输入数据:${recConfig.trainDataPath}\n"+
        s"rui处理公式:${recConfig.logFormula}\n"+
        s"rank=${recConfig.rank}\n"+
        s"numIterations=${recConfig.numIterations}\n"+
        s"lambda=${recConfig.lambda}\n"+
        s"alpha=${recConfig.alpha}\n\n")

    val watchHistoryDict = trainData
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
    val userIDList = trainData.map(x=>x.user).distinct().sortBy(x=>x).take(watchUserNum)

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

//  def parameterSearch(allData:RDD[Rating]):Unit = {
//    val logFilePath = "log/parameterSearch_" + NowTime() + ".log"
//    val fileWriter = new FileWriter(logFilePath, true)
//    fileWriter
//      .write(s"输入数据:$recConfig.trainDataPath\n"+
//        s"rui处理公式:$recConfig.logFormula\n\n") // <- 记得这里一起修改rating处理方式
//
//    val Array(trainData, testDataBinary, testDataRui) = trainTestDataGen(allData,0.0,false)
//    trainData.persist()
//    testDataBinary.persist()
//    testDataRui.persist()
//
//    val evaluations =
//      for (
//        //           lambda <- Array(0.01, 1, 100);
//        alpha  <- Array(200, 500, 1000)
//      ) yield {
//        val model = ALS.trainImplicit(trainData, rank, numIterations, lambda, alpha)
//        val metricsRes = rankingMetrics(testDataBinary,testDataRui, model,20)
//        unpersist(model)
//        ((rank, numIterations, lambda, alpha), metricsRes)
//      }
//
//    evaluations.foreach{ x=>
//      fileWriter.write(x._1.toString()+"=>")
//      fileWriter.write(x._2+"\n")
//    }
//    //从内存中释放
//    trainData.unpersist()
//    testDataBinary.unpersist()
//    testDataRui.unpersist()
//
//    fileWriter.close()
//  }


  def rankingMetrics(binarizedRatings:RDD[Rating],ruiRatings:RDD[Rating], model: MatrixFactorizationModel, recNum:Int):String = {

    //得到对每个用户的N个推荐结果
    val userRecommended = model.recommendProductsForUsers(recNum)

    //将用户的观看记录与推荐结果join，并且删去不感兴趣的节目
    val binarizedWatchHistory = binarizedRatings.groupBy(_.user)
    val ruiWatchHistory = ruiRatings.groupBy(_.user).persist()
    val countUserNum = ruiWatchHistory.count()

    //这里有可能testSet的userID是少于recommendList的，但是join操作按照少的来join，所以没问题
    val binarizedRelevantDocuments = binarizedWatchHistory.join(userRecommended).map { case (user, (actual,
    predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray) //actual 有可能为空
    }.persist()

    var resStr:String = ""

    //《Collaborative Filtering for Implicit Feedback Datasets》评测方式
    val rankScore = ruiWatchHistory.join(userRecommended).map{ case(user, (actual, predictions)) =>
      val preSum = predictions.length
      var rateList=for(i <- 0 to preSum) yield i/preSum.toDouble
      val productScoreMap = predictions.map(x=>x.product).zip(rateList).toMap
      actual.map{x=>
        val score:Double = productScoreMap.getOrElse(x.product,1)
        score * x.rating
      }.sum / actual.map(x=>x.rating).sum
    }.reduce((x,y)=>x+y) / countUserNum.toDouble
    val rankScoreStr = s"rankScore = ${(rankScore * 100).formatted("%.3f")}\n"
    resStr += rankScoreStr

    val metrics = new RankingMetrics(binarizedRelevantDocuments)
    // Precision at K
    Array(1, 3, 5, 10).foreach { k =>
      val pak = s"Precision at $k = ${metrics.precisionAt(k)}\n"
      resStr += pak
    }
    // MAP
    val MAP = s"Mean average precision = ${metrics.meanAveragePrecision}\n"
    resStr += MAP
    // NDCG at K
    Array(1, 3, 5, 10).foreach { k =>
      val NDCGK = s"NDCG at $k = ${metrics.ndcgAt(k)}\n"
      resStr += NDCGK
    }
    //从内存中释放
    binarizedRelevantDocuments.unpersist()
    ruiWatchHistory.unpersist()

    resStr
  }

  def evaluate(testDataBinary:RDD[Rating],testDataRui:RDD[Rating], model: MatrixFactorizationModel):Unit = {

    val logFilePath = "log/rankingMetrics_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath,true)

    fileWriter
      .write(s"输入数据:${recConfig.trainDataPath}\n"+
        s"rui处理公式:${recConfig.logFormula}\n"+
        s"rank=${recConfig.rank}\n"+
        s"numIterations=${recConfig.numIterations}\n"+
        s"lambda=${recConfig.lambda}\n"+
        s"alpha=${recConfig.alpha}\n\n")

    val resStr = rankingMetrics(testDataBinary, testDataRui, model,20)

    fileWriter.write(resStr)
    fileWriter.close()

  }

  def calculateAllCosineSimilarity(model: MatrixFactorizationModel, videoIdMapName:Map[String, String], numRelevent:Int): Unit = {

    //计算cosin距离
    def cosineSimilarity(vector1:DoubleMatrix,vector2:DoubleMatrix):Double = {
      return vector1.dot(vector2) / (vector1.norm2() * vector2.norm2())
    }

    val logFilePath = "log/videoSimilarity_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath,true)

    fileWriter
      .write(s"输入数据:${recConfig.trainDataPath}\n"+
        s"rui处理公式:${recConfig.logFormula}\n"+
        s"rank=${recConfig.rank}\n"+
        s"numIterations=${recConfig.numIterations}\n"+
        s"lambda=${recConfig.lambda}\n"+
        s"alpha=${recConfig.alpha}\n\n")

    //转换movie embedding的格式
    val productsVectorRdd = model.productFeatures
      .map{case (videoID, factor) =>
        val factorVector = new DoubleMatrix(factor)
        (videoID, factorVector)
      }

    //对自身做笛卡尔积，生成一个item-item矩阵
    val productsSimilarity = productsVectorRdd.cartesian(productsVectorRdd)
      //不用与自身做相似度计算
      .filter{ case ((videoID1, vector1), (videoID2, vector2)) => videoID1 != videoID2 }
      //计算相似度
      .map{case ((videoID1, vector1), (videoID2, vector2)) =>
      val sim = cosineSimilarity(vector1, vector2)
      (videoID1, videoID2, sim)
    }
      .filter(_._3 >= recConfig.minSimilarity)

    val videoNum = productsSimilarity.map{case (videoID1, videoID2, sim) => videoID1}.distinct().count()
    val videoList = productsSimilarity.map{case (videoID1, videoID2, sim) => videoID1}.distinct().take(videoNum.toInt)

    val productSimDict = productsSimilarity.map{ case (videoID1, videoID2, sim) =>
      val keyVideoID = videoID1
      val simVideoPair = (videoID2, sim)
      (keyVideoID, List(simVideoPair))
    }
      .reduceByKey((x,y) => x++y)
      .map{ case(keyVideoID, simList) =>
        val allSimList:List[(Int,Double)] = simList.sortBy{case (simVideoID, score) => score.toDouble}.reverse
        var releventVideo:List[(Int,Double)] = List()
        for(i <- allSimList.indices if i < numRelevent){
          releventVideo = releventVideo :+ allSimList(i)
        }
        (keyVideoID, releventVideo)
      }.collectAsMap()

    for (videoID <- videoList){
      val keyVideoName = videoIdMapName.getOrElse(videoID.toString, "keyError")

      val simStr = (productSimDict.get(videoID)).get
        .map{ case(simVideoID, simScore) =>
          videoIdMapName.getOrElse(simVideoID.toString, "keyError") + "," + simScore.formatted("%.3f")}
        .mkString("|")

      val s = s"$keyVideoName:$simStr\n"
      println(s)
      fileWriter.write(s+"\n")
    }

    fileWriter.close()

    productsVectorRdd.unpersist()
    productsSimilarity.unpersist()
  }
}
