
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2019/12/14
  *
  * 实时推荐引擎.
  *
  *
  */
object StreamingRecommender {

  def main(args: Array[String]): Unit = {

    //声明spark环境
    val config = Map(
      "spark.cores" -> "local[4]",
      "kafka.topic" -> "calllog"
    )


    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(2))

    val kafkaPara = Map(
      "bootstrap.servers" -> "172.16.121.3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test-consumer-group"
    )
    //连接Kafka
    //https://blog.csdn.net/Dax1n/article/details/61917718
    val kafkaStream = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaPara))


    //接收评分流  UID | MID | Score | TIMESTAMP
    //kafka数据：1|2|5.0|1564412033
    val ratingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split(",")
        (attr(0),attr(1),attr(2),attr(3).toInt)
    }

    ratingStream.foreachRDD{
      rdd =>
        rdd.map{
          case (caller,callee,time,dur) =>
            println("get data from kafka --- ratingStreamNext ")

        }.count()
    }

//    ratingStream.foreachRDD{
//      rdd =>
//        rdd.map{
//          case (uid,mid,score,timestamp) =>
//            println("get data from kafka --- ratingStreamNext ")
//
//            //实时计算逻辑实现
//            //从redis中获取当前最近的M次评分
//            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)
//
//            //获取电影P最相似的K个电影 共享变量
//            val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadCast.value)
//
//            //计算待选电影的推荐优先级
//
//            val streamRecs = computMovieScores(simMoviesMatrixBroadCast.value,userRecentlyRatings,simMovies)
//
//
//            //将数据保存到MongoDB中
//            saveRecsToMongoDB(uid,streamRecs)
//
//        }.count()
//    }
    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 我们不能改变世界，但我们可以在有限的空间内改变自己。
    *
    * 在春种秋收之时，储备足够过冬的粮食，技能，和人脉，同时调整好自己的心态。
    *
    * 未来没有那么好，但其实也没有那么糟。答案总会有，只要你耐心寻找。
    */
}
