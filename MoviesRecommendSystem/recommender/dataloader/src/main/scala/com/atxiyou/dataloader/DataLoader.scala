package com.atxiyou.dataloader



import java.net.InetAddress

import com.atxiyou.scala.model._
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import  com.atxiyou.java.model.Constant._

//数据加载服务
object DataLoader {
  //程序入口
//  val MOVIE_DATA_PATH="C:\\Users\\chenmeng\\Desktop\\IDEA\\MoviesRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\movies.csv"
//  val RATING_DATA_pATH="C:\\Users\\chenmeng\\Desktop\\IDEA\\MoviesRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\ratings.csv"
//  val TAG_DATA_PATH="C:\\Users\\chenmeng\\Desktop\\IDEA\\MoviesRecommendSystem\\recommender\\dataloader\\src\\main\\resources\\tags.csv"




  def main(args: Array[String]): Unit = {
    if(args.length!=7){
      System.err.println("Usage:java -jar dataloader.jar  <mongo_server>  <es_http_server>  <es_trans_server> <es_cluster_name> <movie_data_path> <rating_data_path> <tag_data_path>\n"
        + "<mongo_server> is the mongo server to connect ,eg. hadoop106:27017\n"
        +"<es_http_server>  is the elasticSearch http servers  eg.hadoop:9200,... "
        +"<es_trans_server>     is the elasticSearch transport eg.haoop106:9300,...      "
        +" <es_cluster_name>      is the elasticSearch   cluster_name   "
        +"<movie_data_path>      the location of movie_data     "
        +"<rating_data_path>       the location of rating_data   "
        +"<tag_data_path>    the location of tag_data")
      System.exit(1)


    }

    val  mongo_server=args(0)

    val es_http_server=args(1)

    val  es_trans_server=args(2)

    val es_cluster_name=args(3)

    val movie_data_path=args(4)

    val rating_data_path=args(5)

    val tag_data_path=args(6)





    val config =Map(
      "spark.cores"->"local[*]",
      "mongo.url"->("mongodb://hadoop106:27017/"+MONGODB_DATABASE),
      "mongo.db"->"recommender",
      "es.httpHosts"->"hadoop106:9200",
      "es.transportHosts"->"hadoop106:9300",
      "es.index"->ES_INDEX,
      "es.cluster.name"->"es-cluster"

    )


    //创建spark Conf
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config.get("spark.cores").get)
    //创建sparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    //将Movie,Rating,Tag数据加载过来
    val movieRDD = spark.sparkContext.textFile(movie_data_path)
    //将movieRDD转换为DataFrame
     val movieDF=movieRDD.map(item =>{
       val attr=item.split("\\^")
       Movie(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim,attr(6).trim,attr(7).trim,attr(8).trim,attr(9).trim)
     }).toDF()

    val ratingRDD = spark.sparkContext.textFile(rating_data_path)
   //ratingRDD转换为DataFrame
    val ratingDF=ratingRDD.map(item =>{
      val attr =item.split(",")
      MovieRating(attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)

    }).toDF()

    val tagRDD = spark.sparkContext.textFile(tag_data_path)
   //tagRDD转换为DataFrame
    val tagDF=tagRDD.map(item=>{
      val attr =item.split(",")
      Tag(attr(0).toInt,attr(1).toInt,attr(2).trim,attr(3).toInt)


    }).toDF()

     implicit val mongoConfig= MongoConfig(config.get("mongo.url").get,config.get("mongo.db").get)
    //需要将数据保存到mongoDB中
   // storeDataInMongoDB(movieDF,ratingDF,tagDF)
    //需要将Tag数据集进行处理

    //需要将处理后的tag数据与movie数据融合，产生新的Movie数据
    import org.apache.spark.sql.functions._



    val newTag=tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags")).select("mid","tags")

    val movieWithTagsDF=movieDF.join(newTag,Seq("mid","mid"),"left")
    implicit val esConfig =ESConfig(config.get("es.httpHosts").get,config.get("es.transportHosts").get,config.get("es.index").get,config.get("es.cluster.name").get)
    //需要新的movie数据保存到ES中
    storeDataInES(movieWithTagsDF)
    //关闭spark
    spark.stop()

  }

  //将数据保存到MongoDB中
  def storeDataInMongoDB(movieDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
     //建立链接
    val mongoClient =MongoClient(MongoClientURI(mongoConfig.url))

    //如果MongoDB中有数据库，那么删除
     mongoClient(mongoConfig.db)(MONGO_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGO_TAG_COLLECTION).dropCollection()


    //写入数据
    movieDF
      .write
        .option("uri",mongoConfig.url)
          .option("collection",MONGO_MOVIE_COLLECTION)
            .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()
    ratingDF
      .write
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_RATING_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()
    tagDF
      .write
      .option("uri",mongoConfig.url)
      .option("collection",MONGO_TAG_COLLECTION)
      .mode("overwrite")
      .format(MONGO_DRIVER_CLASS)
      .save()



    //对数据表建立索引
    mongoClient(mongoConfig.db)(MONGO_MOVIE_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).createIndex(MongoDBObject("mid"->1))
    mongoClient(mongoConfig.db)(MONGO_TAG_COLLECTION).createIndex(MongoDBObject("uid"->1))
    mongoClient(mongoConfig.db)(MONGO_TAG_COLLECTION).createIndex(MongoDBObject("mid"->1))

    //关闭链接
    mongoClient.close()

  }

  //将数据保存到ES中
  def storeDataInES(movieDF:DataFrame)(implicit  eSConfig: ESConfig): Unit = {
    //新建配置
    val settings:Settings=Settings.builder().put("cluster.name",eSConfig.clusterName).build()


    //新建一个ES客户端
    val esClient=new PreBuiltTransportClient(settings)


    val REGEX_HOST_PORT="(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach{
      case REGEX_HOST_PORT(host:String,port:String)=>{
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))

      }
    }



      //清除ES中遗留的数据
    if(esClient.admin().indices().exists(new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){

      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

        //写入
    movieDF
      .write
      .option("es.nodes",eSConfig.httpHosts)
      .option("es.http.timeout","100m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format(ES_DRIVER_CLASS)
      .save(eSConfig.index+"/"+ES_TYPE)

  }

}
