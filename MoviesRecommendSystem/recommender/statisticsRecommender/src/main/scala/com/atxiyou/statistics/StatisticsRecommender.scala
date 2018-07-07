package com.atxiyou.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.atxiyou.java.model.Constant._
import com.atxiyou.scala.model._




object StatisticsRecommender {




 //入口方法
  def main(args: Array[String]): Unit = {

    val config=Map(
      "spark.cores"->"local[*]",
      "mongo.uri"->"mongodb://hadoop106:27017/recommender",
      "mongo.db"->"recommender"
    )


    //配置sparkConf
    val sparkConf =new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    //配置sparkSession
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()


    val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))

    import spark.implicits._
    //数据加载进来
    val ratingDF=spark
      .read
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_RATING_COLLECTION)
        .format(MONGO_DRIVER_CLASS)
      .load()
      .as[MovieRating]

    val movieDF=spark
      .read
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_MOVIE_COLLECTION)
      .format(MONGO_DRIVER_CLASS)
      .load()
      .as[Movie]
      .toDF()


    ratingDF.createOrReplaceTempView("ratings")
    //统计所有历史数据中每个电影评分数
     val rateMoreMoviesDF=spark.sql("select mid,count(mid) as count from ratings group by mid")

      rateMoreMoviesDF
      .write
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_RATE_MORE_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()
    //统计以月为单位每个电影的评分数
    //创建一个日期格式化工具
    val simpleDateFormat=new SimpleDateFormat("yyyyMM")

    //注册一个UDF函数，用于timestamp转换年月
    spark.udf.register("changeDate",(x:Int)=>simpleDateFormat.format(new Date(x*1000L)).toInt)

    val ratingOfYearMouth=spark.sql("select mid,score, changeDate(timestamp) as yeahmouth from ratings")

    ratingOfYearMouth.createOrReplaceTempView("ratingOfMouth")

    val rateMoreRecentlyMovies=spark.sql("select mid,count(mid) as count , yeahmouth from ratingOfMouth group by yeahmouth,mid")


    rateMoreRecentlyMovies
      .write
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_RATE_MORE_RECENTLY_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()


    //统计每个电影的平均评分
     val averageMoviesDF=spark.sql("select mid,avg(score) as avg from ratings group by mid")

    averageMoviesDF
      .write
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_AVERAGE_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()

    //统计每种电影类别评分最高的10个电影
       val movieWithScore=movieDF.join(averageMoviesDF,Seq("mid","mid"))
       //电影类别
       val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")
       //将电影类别转换RDD
      val genresRDD=spark.sparkContext.makeRDD(genres)

     val genrenTopMovies= genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        case (genres,row)=>row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase)

      }
      .map{
        case (genres,row)=>{
          (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))

        }

      }.groupByKey()
      .map{
        case(genres,items)=>GenresRecommendation(genres,items.toList.sortWith(_._2>_._2).take(10).map(item=>Recommendation(item._1,item._2)))

      }.toDF()

    genrenTopMovies
      .write
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()



    //关闭spark
    spark.close()

  }


}
