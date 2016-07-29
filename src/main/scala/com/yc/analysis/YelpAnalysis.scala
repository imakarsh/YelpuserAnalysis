package com.yc.analysis

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD

/**
  * Created by Akarsh Konchada on 7/28/2016.
  */

case class verboseuser (user_id: String , verbosity: Long)

object YelpAnalysis {

  def saveDfToCsv(df: DataFrame, tsvOutput: String,
                  sep: String = ",", header: Boolean = false): Unit = {

    df.repartition(1).write.
      format("com.databricks.spark.csv").
      option("header", header.toString).
      option("delimiter", sep).save(tsvOutput)
  }

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("YelpChallenge").setMaster("local[*]")
    //.set("spark.driver.memory","2g").set("spark.executor.memory","2g")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val df_users = sqlContext.read.json("src/main/resources/yelp_academic_dataset_user.json")

    df_users.registerTempTable("users")
    sqlContext.cacheTable("users")


    val query1 = "Select name as username, user_id, votes.funny as funnyvotes from users order by votes.funny desc limit 10"
    val query2 = "Select name as username, user_id, votes.useful as usefulvotes from users order by votes.useful desc limit 10"
    val query3 = "Select name as username, user_id, votes.cool as coolvotes from users order by votes.cool desc limit 10"

    saveDfToCsv(sqlContext.sql(query1),"topfunnyusers",header = true) //pass seperator if not ','
    saveDfToCsv(sqlContext.sql(query2),"topusefulusers",header = true)
    saveDfToCsv(sqlContext.sql(query3),"topcoolusers",header = true)

    val rdd_reviews:RDD[(String,String)] = sqlContext.read.avro("src/main/resources/yelp_review.avro").
                                            select("user_id","text").rdd.
                                            map({case Row(user_id: String, text: String) => (user_id,text)})

    val df_verboseusers = rdd_reviews.flatMapValues(text => text.split(" ")).
                                mapValues(word => (word,1)).
                                reduceByKey((p,q) => ("dummy",p._2 + q._2)). // adding dummy as the word since we are not worried about the words.
                                mapValues({ case (word,value) => value}). // skipping the word
                                sortBy(_._2, false).zipWithIndex().filter(x=>{x._2 < 10}). // zipping to add index later skip the other records before join
                                map(x=>verboseuser(x._1._1,x._1._2)).toDF() // converting to DF for joining with user name

    df_verboseusers.registerTempTable("verboseusers")

    val query4 = "Select u.name as username, u.user_id, v.verbosity as verbositylevel from users u, verboseusers v where u.user_id=v.user_id order by v.verbosity desc"

    saveDfToCsv(sqlContext.sql(query4),"topverboseusers",header=true)

  }

}
