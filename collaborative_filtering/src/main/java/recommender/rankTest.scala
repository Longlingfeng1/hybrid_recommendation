package recommender

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.{LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object rankTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ALSrankTest").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")
    val data = sc.textFile("/Users/maclc/hybrid_recommendation/feature_all/feature_all.txt")
    val testData = sc.textFile("/Users/maclc/hybrid_recommendation/feature_all/recommend_feature.txt")
//    val splits = data.randomSplit(Array(0.7, 0.3))
//    val (trainingData, testData) = (splits(0), splits(1)d)

    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val test = testData.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(3).split(' ').map(_.toDouble)))
    }.cache()

    // Building the model
    val numIterations = 10000
    val stepSize = 1
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)
    //model.clearThreshold()
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

        val jiaochas = valuesAndPreds.map{case(l,p)=>
            if(l==1&&p>0) -math.log(p) else 0
        }.sum()

        print("交叉熵"+jiaochas/valuesAndPreds.count())
        val count = valuesAndPreds.count()
        val same = valuesAndPreds.map{ case(l,p)=>
            if(l==p) 1 else 0

        }.sum()
        print("same:  "+same)
        val Positive = valuesAndPreds.map{case(l,p) =>
          if((l==1)&&p>0.5) 1 else 0
        }.sum()



        val negitive = valuesAndPreds.map{ case(l,p)=>
          if((l==0)&&p<0.5) 1 else 0
        }.sum()
        print("count :"+count)
        print("positive :"+Positive)
        print("negitive :"+negitive)



//        val metrics = new MulticlassMetrics(valuesAndPreds)
//        println("Confusion matrix:")
//        println(metrics.confusionMatrix)
//
//        // Overall Statistics
//        val precision = metrics.precision
//        val recall = metrics.recall // same as true positive rate
//        val f1Score = metrics.fMeasure
//        println("Summary Statistics")
//        println(s"Precision = $precision")
//        println(s"Recall = $recall")
//        println(s"F1 Score = $f1Score")

    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)
  }

}
