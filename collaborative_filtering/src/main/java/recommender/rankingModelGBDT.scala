package recommender

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils


object rankingModelGBDT {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ALSrank").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")
    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/maclc/hybrid_recommendation/feature_all/feature_all_maohao.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    // The defaultParams for Regression use SquaredError by default.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = 5
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
//    val num1 = labelsAndPredictions.count()
//    val rddList =labelsAndPredictions.take(num1)
//    var i = rddList(0)
//    for ( i <- rddList){
//
//    }



    //print(loss.foreach(x=>print(x)))
//  print(labelsAndPredictions.foreach{
//    x => print(x)
//  })
    //println(loss.reduce( (x,y) =>x+y))
    
   //交叉熵
//    val loss = labelsAndPredictions.map{ case(l,p) =>
//      if (l==1) math.log(p) else math.log(1-p) }.sum()
//    val count = labelsAndPredictions.count()
//    println("jiaochashang :"+ -loss  +  "   "+count)

    val Positive = labelsAndPredictions.map{case(l,p) =>
        if((l==1)&&((l-p)<0.3)) 1 else 0
    }.sum()


    val negitive = labelsAndPredictions.map{ case(l,p)=>
        if((l==0)&&((p-l)<0.3)) 1 else 0
    }.sum()
    print("positive :"+Positive)
    print("negitive :"+negitive)

    val testMSE = labelsAndPredictions.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Test Mean Squared Error = " + testMSE)
    println("Learned regression GBT model:\n" + model.toDebugString)

  }

}
