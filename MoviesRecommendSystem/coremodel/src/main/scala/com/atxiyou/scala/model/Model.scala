package com.atxiyou.scala.model

case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

case class MovieRating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
  *
  * @param url  MongoDB 的链接
  * @param db   MongoDB 要操作的数据库
  */
case class  MongoConfig(val url:String,val db:String)

/**
  *
  * @param httpHosts  httpd主机列表
  * @param transportHosts  transporthosts主机列表
  * @param index   需要操作的索引
  * @param clusterName   ES集群的名字
  */
case class  ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)

//推荐
case class Recommendation(rid: Int, r: Double)

//用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])

//电影相似度
case class MovieRecs(mid: Int, recs: Seq[Recommendation])
/**
  * 电影类别的推荐
  * @param genres  电影的类别
  * @param recs    top10 电影集合
  */
case  class GenresRecommendation(genres:String,recs:Seq[Recommendation])





