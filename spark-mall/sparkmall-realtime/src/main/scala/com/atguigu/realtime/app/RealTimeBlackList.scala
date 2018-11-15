package com.atguigu.realtime.app

import java.util
import java.util.Date

import com.atguigu.realtime.model.RealtimeAdsLog
import com.atguigu.sparkmall0529.common.util.{JedisUtil, KafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object RealTimeBlackList {
  //  Dstream      统计之前过滤掉黑名单中的用户
  //    统计 每天 每个用户点击某个广告的次数
  //  判断 这天累计次数达到100次的用户，保存的黑名单中 （redis记录每人每天每广告点击次数  ，记录黑名单的userid）
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("realTime").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    ssc.checkpoint("hdfs://hadoop105:9000/sparkstreaming")
    val consumerInputStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log", ssc)
    val consumerDStream: DStream[String] = consumerInputStream.map { record => record.value() }

    val realtimeAdsLogDStream: DStream[RealtimeAdsLog] = consumerDStream.map { line =>
      val logArray: Array[String] = line.split(" ")
      val millsecs: Long = logArray(0).toLong
      RealtimeAdsLog(new Date(millsecs), logArray(1), logArray(2), logArray(3), logArray(4))
    }

    val realtimeFilteredLogDstream: DStream[RealtimeAdsLog] =realtimeAdsLogDStream.transform{ rdd=>
      val jedis: Jedis = JedisUtil.getJedis
      val blackList: util.Set[String] = jedis.smembers("blackList")
      jedis.close()
      val blackListBC: Broadcast[util.Set[String]] = sparkSession.sparkContext.broadcast(blackList)
      rdd.filter{realTimeLog=>
        !blackListBC.value.contains(realTimeLog.userId)
      }
    }
    realtimeFilteredLogDstream.foreachRDD { rdd =>

      rdd.foreachPartition { realTimeItr =>
        val jedis: Jedis = JedisUtil.getJedis
        for (realTimeLog <- realTimeItr) {

          jedis.hincrBy("user_count_ads_daily", realTimeLog.toUserCountPerdayKey, 1L)
          val count: String = jedis.hget("user_count_ads_daily", realTimeLog.toUserCountPerdayKey)
          if (count.toLong > 100) {
            jedis.sadd("blackList", realTimeLog.userId)
            println("ok")
          }
        }
        jedis.close()

      }

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
