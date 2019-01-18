package recommender

import java.io.FileWriter

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType, Strategy}
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.{SparkConf, SparkContext}
import recommender.recLib.NowTime


object GBDTandLR {
  //get decision tree leaf's node
  //得到决策树的叶子节点
  def getLeafNodes(node: Node): Array[Int] = {
    var treeLeafNodes = new Array[Int](0)
    if (node.isLeaf) {
      treeLeafNodes = treeLeafNodes.:+(node.id)
    } else {
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.leftNode.get)
      treeLeafNodes = treeLeafNodes ++ getLeafNodes(node.rightNode.get)
    }
    treeLeafNodes
  }

  // predict decision tree leaf's node value
  // 得到新特征
  def predictModify(node: Node, features: DenseVector): Int = {
    val split = node.split
    if (node.isLeaf) {
      node.id
    } else {
      if (split.get.featureType == FeatureType.Continuous) {
        if (features(split.get.feature) <= split.get.threshold) {
          //          println("Continuous left node")
          predictModify(node.leftNode.get, features)
        } else {
          //          println("Continuous right node")
          predictModify(node.rightNode.get, features)
        }
      } else {
        if (split.get.categories.contains(features(split.get.feature))) {
          //          println("Categorical left node")
          predictModify(node.leftNode.get, features)
        } else {
          //          println("Categorical right node")
          predictModify(node.rightNode.get, features)
        }
      }
    }
  }

  def main(args: Array[String]) {

//    val sparkConf = new SparkConf().setAppName("GbdtAndLr")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    //val sampleDir = "/Users/leiyang/IdeaProjects/spark_2.3/src/watermelon3_0_En.csv"
//    val sc = new SparkContext(sparkConf)
    val conf = new SparkConf().setAppName("ALS").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")
    //val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    //val dataFrame = spark.read.format("CSV").option("header", "true").load(sampleDir)
    val trainData = sc.textFile("feature_all_0.1_filter.txt")
    val testData = sc.textFile("recommend_feature.txt")//预测推荐结果的分数，排序
    //train 一部分用来训练gbdt 一部分用来 从gbdt中构造新特征输入到lr中训练lr。
    val splits = trainData.randomSplit(Array(0.5, 0.5)) //trainData划分成两部分，一部分训练gbdt，一部分用训练好的gbdt 构造新特征-->然后输入到lr模型中训练lr模型
    val train_gbdt = splits(0)  //训练gbdt
    val train_lr = splits(1)   //用训练好的gbdt构造新特性


    val train = train_gbdt.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(3).split(' ').map(_.toDouble)))
    }.cache()
    val train_LR = train_lr.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(3).split(' ').map(_.toDouble)))
    }.cache()



    val test = testData.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(3).split(' ').map(_.toDouble)))
    }.cache()




    //
    // GBDT Model
    val numTrees = 2
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(numTrees)
    val treeStratery = Strategy.defaultStrategy("Classification")
    treeStratery.setMaxDepth(5)
    treeStratery.setNumClasses(2)
    //    treeStratery.setCategoricalFeaturesInfo(Map[Int, Int]())
    boostingStrategy.setTreeStrategy(treeStratery)
    val gbdtModel = GradientBoostedTrees.train(train, boostingStrategy)


    val treeLeafArray = new Array[Array[Int]](numTrees)
    for (i <- 0.until(numTrees)) {
      treeLeafArray(i) = getLeafNodes(gbdtModel.trees(i).topNode)
    }
    for (i <- 0.until(numTrees)) {
      println("正在打印第 %d 棵树的 topnode 叶子节点", i)
      for (j <- 0.until(treeLeafArray(i).length)) {
        println(j)
      }

    }

    //gbdt 构造新特征
    val newFeatureDataSet =  train_lr.map { line =>
      val parts = line.split(',')
      (parts(0).toDouble,new DenseVector(parts(1).split(' ').map(_.toDouble)))
    }.map { x =>
      var newFeature = new Array[Double](0)
      for (i <- 0.until(numTrees)) {
        val treePredict = predictModify(gbdtModel.trees(i).topNode, x._2)
        //gbdt tree is binary tree
        val treeArray = new Array[Double]((gbdtModel.trees(i).numNodes + 1) / 2)
        treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
        newFeature = newFeature ++ treeArray
      }
      (x._1, newFeature)
    }
    //测试集数据也要构造新特征 保证与训练数据的维度相同
    val newTestDataSet =  testData.map { line =>
      val parts = line.split(',')
      (parts(0).toDouble,parts(1).toDouble,parts(2).toString,new DenseVector(parts(3).split(' ').map(_.toDouble)))
    }.map { x =>
      var newFeature = new Array[Double](0)
      for (i <- 0.until(numTrees)) {
        val treePredict = predictModify(gbdtModel.trees(i).topNode, x._4)
        //gbdt tree is binary tree
        val treeArray = new Array[Double]((gbdtModel.trees(i).numNodes + 1) / 2)
        treeArray(treeLeafArray(i).indexOf(treePredict)) = 1
        newFeature = newFeature ++ treeArray
      }
      (x._1, x._2,x._3,newFeature)
    }

    val newData = newFeatureDataSet.map(x => LabeledPoint(x._1, new DenseVector(x._2))) //用来训练LR
    val test1Data = newTestDataSet.map(x => (x._1, x._2,x._3,new DenseVector(x._4)) ) //
//    val splits2 = newData.randomSplit(Array(0.8, 0.2))
//    val train2 = splits2(0)
//    val test2 = splits2(1)

    //LR model
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(newData).setThreshold(0.01)
    model.clearThreshold()
    //model.weights
    val predictionAndLabels = test1Data.map { case (label,userid,videoname,features) =>
      val prediction = model.predict(features)
      (userid,videoname,prediction)
    }

    val user = predictionAndLabels.map(x=>(x._1,(x._2,x._3))).groupByKey().sortByKey()

    val user1 = user.map{x=>
      val i2=x._2.toList
      val i2_2 = i2.sortBy(_._2)(Ordering.Double.reverse)
      (x._1,i2_2)}.collect()

    //打印结果
    val logFilePath = "log/watched-recommend-gbdt+lr_" + NowTime() + ".log"
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

    //predictionAndLabels.foreach(printl

    sc.stop()
  }
}
