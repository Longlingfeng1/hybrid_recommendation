package recommender

object recConfig {
  var trainDataPath:String = ""
  var videoTable:String = ""
  var rank:Int = 20 // feature数量
  var numIterations:Int = 10 //模型迭代次数
  var lambda:Double = 0.1 //正则项参数
  var alpha:Double = 100 //置信度
  var logFormula:String = "log10(1+x)"
  var minSimilarity:Double = 0.3  //电影最小相似度
}
