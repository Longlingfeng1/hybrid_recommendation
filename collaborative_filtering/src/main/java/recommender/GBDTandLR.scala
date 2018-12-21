package recommender

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType, Strategy}
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.{SparkConf, SparkContext}


object GBDTandLR {
  //get decision tree leaf's node
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
    val trainData = sc.textFile("/Users/maclc/hybrid_recommendation/feature_all/feature_all.txt")
    val testData = sc.textFile("/Users/maclc/hybrid_recommendation/feature_all/recommend_feature.txt")
    //train 一部分用来训练gbdt 一部分用来 从gbdt中构造新特征输入到lr中训练lr。
    val splits = trainData.randomSplit(Array(0.5, 0.5))
    val train_gbdt = splits(0)
    val train_lr = splits(1)


    val train = train_gbdt.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val train_LR = train_lr.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()



    val test = testData.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble,Vectors.dense(parts(3).split(' ').map(_.toDouble)))
    }.cache()




    //
    //    // GBDT Model
    val numTrees = 2
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.setNumIterations(numTrees)
    val treeStratery = Strategy.defaultStrategy("Classification")
    treeStratery.setMaxDepth(5)
    treeStratery.setNumClasses(2)
    //    treeStratery.setCategoricalFeaturesInfo(Map[Int, Int]())
    boostingStrategy.setTreeStrategy(treeStratery)
    val gbdtModel = GradientBoostedTrees.train(train, boostingStrategy)
    //    val gbdtModelDir = args(2)
    //    gbdtModel.save(sc, gbdtModelDir)

//    val labelAndPreds = test.map{ point =>
//      val prediction = gbdtModel.predict(point.features)
//      (point.label, prediction)
//    }
//    val jiaochas = labelAndPreds.map{case(l,p)=>
//              if((l==1)&&(p>0)) -math.log(p) else 0
//          }.sum()
//    print("sum:"+labelAndPreds.count())
//    print("GBDTjiaochashang :"+jiaochas)
//    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / test.count()
//    println("Test Error = " + testErr)
//    //    println("Learned classification GBT model:\n" + gbdtModel.toDebugString)toDebugString

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
    //    gbdt 构造新特征
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

    val newTestDataSet =  testData.map { line =>
      val parts = line.split(',')
      (parts(0).toDouble,new DenseVector(parts(3).split(' ').map(_.toDouble)))
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

    val newData = newFeatureDataSet.map(x => LabeledPoint(x._1, new DenseVector(x._2))) //用来训练LR
    val test1Data = newTestDataSet.map(x => LabeledPoint(x._1, new DenseVector(x._2)))  //
//    val splits2 = newData.randomSplit(Array(0.8, 0.2))
//    val train2 = splits2(0)
//    val test2 = splits2(1)

    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(newData).setThreshold(0.01)
    model.clearThreshold()
    //model.weights
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    //predictionAndLabels.foreach(println)
    val jiaochasLR = predictionAndLabels.map{case(p,l)=>
      if((l==1)&&(p>0)) -math.log(p) else 0
    }.sum()
    print("sum:"+predictionAndLabels.count())
    print("LRjiaochashang :"+jiaochasLR/predictionAndLabels.count())
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)

    sc.stop()
  }
}
