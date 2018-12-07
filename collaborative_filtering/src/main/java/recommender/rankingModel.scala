package recommender

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.regression.{LinearRegressionWithSGD,LinearRegressionModel}
import org.apache.spark.mllib.classification.{LogisticRegressionModel,LogisticRegressionWithSGD,SVMModel,SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors


object rankingModel {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ALS").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // Load and parse the data
    val data = sc.textFile("cleaned_data/feature_all.txt")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val train = trainingData.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val test = testData.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    // Building the model
    val numIterations = 100
    val stepSize = 1
    //val miniBatchFraction = 2.0
    val model = LogisticRegressionWithSGD.train(train, numIterations)

    println("weights:%s, intercept:%s".format(model.weights,model.intercept))

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = test.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    print(valuesAndPreds.foreach(x=>print(x)))

    val count = valuesAndPreds.count()
    val same = valuesAndPreds.map{ case(l,p)=>
        if(l==p) 1 else 0

    }.sum()
    print("same:  "+same)
    val Positive = valuesAndPreds.map{case(l,p) =>
      if((l==1)&&((l-p)<0.3)) 1 else 0
    }.sum()


    val negitive = valuesAndPreds.map{ case(l,p)=>
      if((l==0)&&((p-l)<0.3)) 1 else 0
    }.sum()
    print("count :"+count)
    print("positive :"+Positive)
    print("negitive :"+negitive)


    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)

    val metrics = new MulticlassMetrics(valuesAndPreds)
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    val precision = metrics.precision
    val recall = metrics.recall // same as true positive rate
    val f1Score = metrics.fMeasure
    println("Summary Statistics")
    println(s"Precision = $precision")
    println(s"Recall = $recall")
    println(s"F1 Score = $f1Score")


    //准确率


//    model.save(sc, "myModelPath")
//    val sameModel = LinearRegressionModel.load(sc, "myModelPath")

  }
}
