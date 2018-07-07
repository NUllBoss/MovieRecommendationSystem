package com.atxiyou.offline

import com.atxiyou.scala.model._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix
import com.atxiyou.java.model.Constant._




object OfflineRecommender {


  val USER_MAX_RECOMMENDATION = 20


  //入口方法
  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop106:27017/recommender",
      "mongo.db" -> "recommender"


    )

    //创建一个sparkConf
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
      .set("spark.executor.memory", "6G").set("spark.driver.memory", "3G")
    //基于sparkconf 创建一个sparkSession

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建mongodbConfig
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    import spark.implicits._
    //读取mongdodb中的业务数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.url)
      .option("collection",MONGO_RATING_COLLECTION )
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)).cache()
    //用户的数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    //电影的数据集
    val movieRDD = spark
      .read
      .option("uri", mongoConfig.url)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    //创建训练数据集

    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    val (rank, iterations, lambda) = (50, 10, 0.01)
    //训练ALS模型
    val model = ALS.train(trainData, rank, iterations, lambda)




    //计算用户推荐矩阵

    //构建一个usersProducts RDD[(Int)(Int)]
    val userMovies = userRDD.cartesian(movieRDD)


    val preRatings = model.predict(userMovies)

    val userRecs = preRatings.map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))


      }.toDF()

    userRecs.write
      .option("uri", mongoConfig.url)
      .option("collection", MONGO_USER_RECS_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()


    //计算电影相似度矩阵
    val movieFeatures = model.productFeatures.map {
      case (mid, freatures) =>
        (mid, new DoubleMatrix(freatures))

    }

    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter { case (a, b) => a._1 != b._1 }
      .map { case (a, b) =>
        val simSCore = this.consinSim(a._2, b._2)
        (a._1, (b._1, simSCore))

      }.filter(_._2._2 > 0.6)
      .groupByKey()
      .map { case (mid, items) =>
        MovieRecs(mid, items.toList.map(x => Recommendation(x._1, x._2)))

      }.toDF()


    movieRecs
      .write
      .option("uri", mongoConfig.url)
      .option("collection", MONGO_MOVIE_RECS_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()



    //关闭spark

    spark.close()
  }


  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {

    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())

  }


}
