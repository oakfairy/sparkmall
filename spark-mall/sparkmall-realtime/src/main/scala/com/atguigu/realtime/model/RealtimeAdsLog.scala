package com.atguigu.realtime.model

import java.text.SimpleDateFormat
import java.util.Date

//timestamp province city userid adid
case class RealtimeAdsLog(date:Date,province:String,city:String,userId:String,adid:String){
  def toUserCountPerdayKey: String ={
    val perDay: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    perDay+"_"+userId+"_"+adid
  }
  def toAreaCityCountPerdayKey(): String ={
    val perDay: String = new SimpleDateFormat("yyyy-MM-dd").format(date)
    perDay+"_"+province+"_"+city+"_"+adid
  }
}


