package com.atxiyou.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import com.atxiyou.java.model.Constant._
import com.atxiyou.scala.model.{MongoConfig, MovieRecs}

import scala.collection.JavaConversions._

object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("hadoop106")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop106:27017/recommender"))

}



object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20




  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongodb.uri" -> "mongodb://hadoop106:27017/recommender",
      "mongodb.db" -> "recommender",
      "kafka.topic" -> "recommender"

    )

    //创建一个sparkConf
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))


    //创建一个spark对象
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(2))

    implicit val mongConfig = MongoConfig(config("mongodb.uri"), config("mongodb.db"))
    import spark.implicits._
    //*********************广播电影相似度矩阵

    val simMoviesMatrix = spark
      .read
      .option("uri", config("mongodb.uri"))
      .option("collection", MONGO_MOVIE_RECS_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRecs]
      .rdd
      .map { recs =>
        (recs.mid, recs.recs.map(x => (x.rid, x.r)).toMap)
      }.collectAsMap()

    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)



    //********************

    //创建kafka的链接

    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop106:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))

    val ratingStream = kafkaStream.map {
      case msg =>
        var attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }


    ratingStream.foreachRDD { rdd =>
      rdd.map { case (uid, mid, socre, timestamp) =>
        println(">>>>>>>>>>>>>")
        //获取当前用户m次评分
        val userRecentilRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)


        //获取电影p最相似的k个电影
        val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMoviesMatrixBroadCast.value)

        //计算待选电影的推荐优先级
        val streamRecs = computerMovieScores(simMoviesMatrixBroadCast.value, userRecentilRatings, simMovies)

        //保存数据到mongodb
        saveRecsToMongodb(uid, streamRecs)

      }.count()
    }

    //启动streaming 程序
    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 将数据保存到MongoDB中
    *
    * @param streamRecs 流式推荐的结果
    * @param mongConfig Mongodb配置
    */
  def saveRecsToMongodb(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongConfig: MongoConfig): Unit = {
    //到streamRecs的链接
    val streamRecsCollection = ConnHelper.mongoClient(mongConfig.db)(MONGO_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))

    streamRecsCollection.insert(MongoDBObject("uid" -> uid, "recs" -> streamRecs.map(x => x._1 + ":" + x._2).mkString("|")))


  }

  /**
    * 计算待选电影的推荐分数
    *
    * @param simMovies           电影相似度矩阵
    * @param userRecentilRatings 用户最近K次评分
    * @param topsimMovies        当前电影的最相似的K个电影
    * @return
    */
  def computerMovieScores(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRecentilRatings: Array[(Int, Double)], topsimMovies: Array[Int]): Array[(Int, Double)] = {
    //保存每一个带选定电影和最后评分的每一个电影的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //保存每一个电影的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int, Int]()

    //保存每一个电影的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (topsimMovie <- topsimMovies; userRecentilRating <- userRecentilRatings) {
      val simScore = getMoviesSimScore(simMovies, userRecentilRating._1, topsimMovie)
      if (simScore > 0.6) {
        score += ((topsimMovie, simScore * userRecentilRating._2))
        if (userRecentilRating._2 > 3) {
          increMap(topsimMovie) = increMap.getOrDefault(topsimMovie, 0) + 1

        } else {
          decreMap(topsimMovie) = increMap.getOrDefault(topsimMovie, 0) + 1

        }
      }

    }
    score.groupBy(_._1).map { case (mid, sims) =>
      (mid, sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(decreMap(mid)))
    }.toArray


  }

  def log(m: Int): Double = {
    math.log(m) / math.log(2)

  }

  /**
    * 获取两个电影之间的相似度
    *
    * @param simMovies       电影相似矩阵
    * @param userRatingMovie 用户已评分电影
    * @param topSimMovie     候选电影
    * @return
    */
  def getMoviesSimScore(simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRatingMovie: Int, topSimMovie: Int): Double = {

    simMovies.get(topSimMovie) match {
      case Some(sim)=>sim.get(userRatingMovie) match{
        case Some(score)=>score
        case None =>0.0
      }
      case None =>0.0
    }


  }

  //获取当前最近的m次电影评分
  /**
    *
    * @param num 评分的个数
    * @param uid 谁的评分
    * @return
    */
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    //从用户的队列中取数num个评分
    jedis.lrange("uid:" + uid.toString, 0, num).map { item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).toDouble)
    }.toArray

  }

  /**
    * 获取当前电影k个相似的电影
    *
    * @param num        相似电影数量
    * @param mid        当前电影的id
    * @param uid        当前评分的用户
    * @param simMovies  电影相似矩阵广播变量值
    * @param mongConfig Mongod配置
    * @return
    */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongConfig: MongoConfig): Array[Int] = {
    //广播变量的电影相似度矩阵中获取当前电影的所有相似电影
    val allSimMovies = simMovies.get(mid).get.toArray

    //获取用户已经观看过的电影
    val ratingExist = ConnHelper.mongoClient(mongConfig.db)(MONGO_RATING_COLLECTION).find(MongoDBObject("uid" -> uid)).toArray.map { item =>
      item.get("mid").toString.toInt
    }
    //过滤掉已经评分过得电影，并排序输出
    allSimMovies.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)


  }
}
