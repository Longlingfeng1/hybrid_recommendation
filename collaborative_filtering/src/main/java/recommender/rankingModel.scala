package recommender

import java.io.{FileInputStream, FileWriter}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.{LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import recommender.recLib.{NowTime, trainTestDataGen}

import scala.collection.Map


object rankingModel {
  def loadProperties():Unit = {
    val properties = new Properties()
    properties.load(new FileInputStream("ALS.properties"))
    recConfig.trainDataPath = properties.getProperty("trainDataPath")
    recConfig.videoTable = properties.getProperty("videoTable")

  }
  def main(args: Array[String]): Unit = {
    loadProperties()
    val conf = new SparkConf().setAppName("ALS").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // Load and parse the data
    val trainingData = sc.textFile("/Users/maclc/hybrid_recommendation/feature_all/feature_all.txt")
    //val data = sc.textFile("../embedding/feature_all_add_embedding.txt")
    //val splits = data.randomSplit(Array(0.7, 0.3))
    //val (trainingData, testData) = (splits(0), splits(1))
    val testData = sc.textFile("/Users/maclc/hybrid_recommendation/feature_all/recommend_feature.txt")

//    val watchHistoryData =sc.textFile(recConfig.trainDataPath)
//    val videoIdMapName:Map[String,String] = sc.textFile(recConfig.videoTable)
//      .map(x=>(x.split(",")(0),x.split(",")(1)))
//      .collectAsMap()
//    val ratings = watchHistoryData.map(_.split(',') match { case Array(user, item, rate) =>
//      Rating(user.toInt, item.toInt, rate.toDouble)
//    }).filter(x=>x.rating>recConfig.minrating)
//    val Array(watchData, testDataBinary, testDataRui) = trainTestDataGen(ratings, 0, false)
//    watchData.persist()
    val train = trainingData.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val test_first = testData.map { line =>
      val parts = line.split(',')
      (parts(0).toDouble,parts(1).toDouble,parts(2).toString(),Vectors.dense(parts(3).split(' ').map(_.toDouble)))//(label,userid,videoname,feature)
      //LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()


    // Building the model
    val numIterations = 1000
    val stepSize = 1
    //val miniBatchFraction = 2.0
    //val model = LogisticRegressionWithLBFGS.train(train, numIterations)
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(train).setThreshold(0.01)
    model.clearThreshold()
//
//    println("weights:%s, intercept:%s".format(model.weights,model.intercept))
//    //model.clearThreshold()//逻辑回归输出概率
//    // Evaluate model on training examples and compute training error
//
    val valuesAndPreds = test_first.map { point =>
      val prediction = model.predict(point._4)
      val userid = point._2
      val videoname = point._3
      (userid,videoname,prediction)
    }
    //观看历史
//    val watchHistoryDict = watchData
//      //每条记录转为元组
//      .map{ x=>
//      val videoPair = (videoIdMapName.getOrElse(x.product.toString,"keyError"),x.rating.formatted("%.3f"))
//      (x.user,List(videoPair))
//    }
//      //按照用户id聚合list
//      .reduceByKey((x,y)=>x++y)
//      .sortByKey()
//      .map{ case(userID , watchList)=>
//        (userID, watchList.sortBy{ case (videoID, score) => score.toDouble }.reverse)
//      }
//      .collectAsMap()
     val user = valuesAndPreds.map(x=>(x._1,(x._2,x._3))).groupByKey().sortByKey()

     val user1 = user.map{x=>
      val i2=x._2.toList
      val i2_2 = i2.sortBy(_._2)(Ordering.Double.reverse)
         (x._1,i2_2)}.collect()
       // .flatMap(x=>{
//      val y=x._2
//      for(w<-y) yield(x._1,w._1,w._2)
//  })

 // user1.foreach(println)
     //按照userid分组 然后 按照各个电影的预测值排序。
//     val group = user.groupBy(recommends => (recommends._1)).map{recommend=>
//       val order_recommends = recommend._2.toList.sortBy(_._3)(Ordering.Double.reverse)
//       (order_recommends)
//     }

    //打印结果
    val logFilePath = "log/watched-recommend-ranking_" + NowTime() + ".log"
    val fileWriter = new FileWriter(logFilePath,true)
    for(recommend<-user1){
      val userid = recommend._1
      val videopair = recommend._2
      fileWriter.write(s"用户ID：$userid\n推荐结果：\n")
      for(i<-videopair){
        fileWriter.write(i._1+","+i._2.formatted("%.3f")+"|")
      }
      fileWriter.write("\n\n")
    }

//    for(recommend_user <- user1){
//      recommend_user
//      val user = recommend_user._1  //recommend_user 是 list，存着 20个未排序的（user，video，predict）
//      fileWriter.write(s"用户ID：$user\n推荐结果：\n")
//        for(i<-recommend_user._2){
//          fileWriter.write(i._1+","+i._2.formatted("%.3f")+"|")
//        }
//      fileWriter.write("\n\n")
//    }
    fileWriter.close()


    //group.foreach(println)



//
//
//
//    print(valuesAndPreds.foreach(x=>
//    print(x._3)
//    ))

    //评价指标
//    val jiaochas = valuesAndPreds.map{case(l,p)=>
//        if(l==1&&p>0) -math.log(p) else 0
//    }.sum()
//
//    print("交叉熵"+jiaochas/valuesAndPreds.count())
//    val count = valuesAndPreds.count()
//    val same = valuesAndPreds.map{ case(l,p)=>
//        if(l==p) 1 else 0
//
//    }.sum()
//    print("same:  "+same)
//    val Positive = valuesAndPreds.map{case(l,p) =>
//      if((l==1)&&p>0.5) 1 else 0
//    }.sum()
//
//
//    val negitive = valuesAndPreds.map{ case(l,p)=>
//      if((l==0)&&p<0.5) 1 else 0
//    }.sum()
//    print("count :"+count)
//    print("positive :"+Positive)
//    print("negitive :"+negitive)
//
//
//    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
//    println("training Mean Squared Error = " + MSE)
//
//    val metrics = new MulticlassMetrics(valuesAndPreds)
//    println("Confusion matrix:")
//    println(metrics.confusionMatrix)
//
//    // Overall Statistics
//    val precision = metrics.precision
//    val recall = metrics.recall // same as true positive rate
//    val f1Score = metrics.fMeasure
//    println("Summary Statistics")
//    println(s"Precision = $precision")
//    println(s"Recall = $recall")
//    println(s"F1 Score = $f1Score")


    //准确率


//    model.save(sc, "myModelPath")
//    val sameModel = LinearRegressionModel.load(sc, "myModelPath")

  }
}
