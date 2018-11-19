package recommender

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors

object rankingModel {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ALS").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    // Load and parse the data
    val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble))) //(label, feature)
    }.cache()

    // Building the model
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{
      case(v, p) => math.pow((v - p), 2)
    }.mean()
    println("training Mean Squared Error = " + MSE)

  }
}
