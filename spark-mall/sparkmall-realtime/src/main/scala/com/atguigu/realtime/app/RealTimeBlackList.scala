package com.atguigu.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.realtime.model.RealtimeAdsLog
import com.atguigu.sparkmall0529.common.util.{JedisUtil, KafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

object RealTimeBlackList {
  //  Dstream      统计之前过滤掉黑名单中的用户
  //    统计 每天 每个用户点击某个广告的次数
  //  判断 这天累计次数达到100次的用户，保存的黑名单中 （redis记录每人每天每广告点击次数  ，记录黑名单的userid）
  def main(args: Array[String]): Unit = {
    //需求七
    val sparkConf: SparkConf = new SparkConf().setAppName("realTime").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    ssc.checkpoint("hdfs://hadoop105:9000/sparkstreaming")
    val consumerInputStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("ads_log", ssc)
    val consumerDStream: DStream[String] = consumerInputStream.map { record => record.value() }
    val jedis: Jedis = JedisUtil.getJedis
    val realtimeAdsLogDStream: DStream[RealtimeAdsLog] = consumerDStream.map { line =>
      val logArray: Array[String] = line.split(" ")
      val millsecs: Long = logArray(0).toLong
      RealtimeAdsLog(new Date(millsecs), logArray(1), logArray(2), logArray(3), logArray(4))
    }

    val realtimeFilteredLogDstream: DStream[RealtimeAdsLog] = realtimeAdsLogDStream.transform { rdd =>
      val jedis: Jedis = JedisUtil.getJedis
      val blackList: util.Set[String] = jedis.smembers("blackList")
      jedis.close()
      val blackListBC: Broadcast[util.Set[String]] = sparkSession.sparkContext.broadcast(blackList)
      rdd.filter { realTimeLog =>
        !blackListBC.value.contains(realTimeLog.userId)
      }
    }
    realtimeFilteredLogDstream.foreachRDD { rdd =>

      rdd.foreachPartition { realTimeItr =>
        val jedis: Jedis = JedisUtil.getJedis
        for (realTimeLog <- realTimeItr) {

          jedis.hincrBy("user_count_ads_daily", realTimeLog.toUserCountPerdayKey, 1L)
          val count: String = jedis.hget("user_count_ads_daily", realTimeLog.toUserCountPerdayKey)
          if (count.toLong > 10000) {
            jedis.sadd("blackList", realTimeLog.userId)
            println("ok")
          }
        }
        jedis.close()

      }

    }

    //需求八
    val areaCityTotalCountDStream: DStream[(String, Long)] = clickadsdistrictCitydaily(realtimeAdsLogDStream)
    areaCityTotalCountDStream.foreachRDD { rdd =>
      rdd.foreach { areaCityTotalCountItr =>
        val jedis: Jedis = JedisUtil.getJedis
        for ((areaCityCountKey, count) <- areaCityTotalCountItr) {
          jedis.hset("area_city_ads_count", areaCityCountKey, count)
        }
        jedis.close()
      }
    }
    //需求九
    hotAdsDistrictDailyTop3(areaCityTotalCountDStream).foreachRDD { rdd =>
      val areaAdsCountPerDayArr: Array[(String, Map[String, String])] = rdd.collect()
      for ((dateKey, areaTop3AdsMap) <- areaAdsCountPerDayArr) {

        jedis.hmset(dateKey, areaTop3AdsMap)
      }
    }
    //需求十
    val lastHourMinuCountJsonPerAdsDstream: DStream[(String, String)] = adsCount(realtimeAdsLogDStream)
    lastHourMinuCountJsonPerAdsDstream.foreachRDD { rdd =>
      val lastHourMap: Map[String, String] = rdd.collect().toMap
      jedis.del("last_hour_ads_click")
      jedis.hmset("last_hour_ads_click", lastHourMap)

    }
    ssc.start()
    ssc.awaitTermination()
  }

  //需求八方法
  def clickadsdistrictCitydaily(realtimeDStream: DStream[RealtimeAdsLog]): DStream[(String, Long)] = {
    //    DStream[Realtimelog]=>
    //    转化成 DStream[ (date:area:city:ads,  1L)]
    //    进行reduceByKey()  每5秒的所有key累计值
    //      updateStateByKey 来汇总到历史计数中
    //      输出到redis中

    val areaCityClickDStream: DStream[(String, Long)] = realtimeDStream.map { realtimelog => (realtimelog.toAreaCityCountPerdayKey(), 1L) }
    val areaCityCountDStream: DStream[(String, Long)] = areaCityClickDStream.reduceByKey(_ + _)
    val areaCityTotalCountDStream: DStream[(String, Long)] = areaCityCountDStream.updateStateByKey { case (previousValues: Seq[Long], currentVlaue: Option[Long]) =>
      var previousSum: Long = previousValues.sum
      val currentSum: Long = currentVlaue.getOrElse(0L)
      previousSum += currentSum
      Some(previousSum)
    }
    areaCityTotalCountDStream

  }

  //需求九
  //每天各地区 top3 热门广告
  def hotAdsDistrictDailyTop3(areaCityTotalCountDStream: DStream[(String, Long)]): DStream[(String, Map[String, String])] = {
    //    先把date_area_city_ads 切分开 变成（date，（area,(ads,count））
    //    RDD[(date,  (area,(adsid,count)) ] =>groupbykey
    //    RDD[(date, Iterable(area,(adsid,count)) ]=>groupby
    //    RDD[(date,   Iterable (area,Map(area, (adsid,count))]]] =>整理
    //    RDD[(date,   Iterable (area,Map(adsid,count)]]] => json4s序列化
    //    RDD[(String,Map[String,Json])] 保存 jedis.hmset(key,javamap)
    val areaAdsCountDstrem: DStream[(String, Long)] = areaCityTotalCountDStream.map { case (dateAreaCityAdsKey, count) =>
      val keyArr: Array[String] = dateAreaCityAdsKey.split("_")
      val date: String = keyArr(0)
      val area: String = keyArr(1)
      val ads: String = keyArr(3)
      (date + "_" + area + "_" + ads, count)
    }.reduceByKey(_ + _)
    val areaTop3ItrPerDayDstream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCountDstrem.map { case (dateAreaAdsKey, count) =>
      val keyArr: Array[String] = dateAreaAdsKey.split("_")
      val date: String = keyArr(0)
      val area: String = keyArr(1)
      val ads: String = keyArr(2)
      ("area_top3_ads:" + date, (area, (ads, count)))
    }.groupByKey()
    val areaTop3AdsJsonPerDayDstream: DStream[(String, Map[String, String])] = areaTop3ItrPerDayDstream.map { case (dateKey, areaItr) =>
      val areaMap: Map[String, Iterable[(String, (String, Long))]] = areaItr.groupBy { case (area, (dateKey, count)) => area }
      val top3AdsJsonMap: Map[String, String] = areaMap.map { case (area, areaAdsItr) =>
        val adsTop3List: List[(String, Long)] = areaAdsItr.map { case (area, (ads, count)) => (ads, count) }.toList.sortWith { case (ads1, ads2) => ads1._2 > ads2._2 }.take(3)
        val top3AdsJson: String = compact(render(adsTop3List))
        (area, top3AdsJson)
      }

      (dateKey, top3AdsJsonMap)
    }
    areaTop3AdsJsonPerDayDstream

  }

  //需求十
  def adsCount(realtimeLogDstream: DStream[RealtimeAdsLog]): DStream[(String, String)] = {
    //    1、	窗口函数 => 每隔多长时间，截取到最近一小时的所有访问记录
    //    2、	 rdd放了某段时间的访问记录  RDD[RealtimeAdsLog]
    //      RDD（广告id_小时分钟，1L）=>reducebykey
    //    变成  广告id+小时分钟作为key，进行聚合
    //    => RDD（广告id_小时分钟，count）
    //    =>RDD(广告，（小时分钟，count）) =>groupbykey
    //    =>RDD（广告，iterable(小时分钟，count)）
    //    =>RDD（广告，iterable(小时分钟，count)）排序序列化 转成json .collect
    //    =>Map[广告，每分钟计算json].
    val lastHourAdsLogDStream: DStream[RealtimeAdsLog] = realtimeLogDstream.window(Minutes(60), Seconds(10))
    val lastHourAdsMinuCountDstream: DStream[(String, Long)] = lastHourAdsLogDStream.map { adslog =>
      val hourMinu: String = new SimpleDateFormat("HH:mm").format(adslog.date)
      (adslog.adid + "_" + hourMinu, 1L)
    }.reduceByKey(_ + _)

    val lastHourMinuCountPerAdsDstream: DStream[(String, Iterable[(String, Long)])] = lastHourAdsMinuCountDstream.map { case (adsMinuKey, count) =>
      val keyArr: Array[String] = adsMinuKey.split("_")
      val ads: String = keyArr(0)
      val hourMinu: String = keyArr(1)
      (ads, (hourMinu, count))
    }.groupByKey()
    val lastHourMinuCountJsonPerAdsDstream: DStream[(String, String)] = lastHourMinuCountPerAdsDstream.map { case (ads, minuItr) =>
      val hourminuCountJson: String = compact(render(minuItr))
      (ads, hourminuCountJson)
    }
    lastHourMinuCountJsonPerAdsDstream


  }


}
