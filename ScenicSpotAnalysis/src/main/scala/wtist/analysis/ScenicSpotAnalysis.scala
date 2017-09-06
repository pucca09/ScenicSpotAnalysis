package wtist.analysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import wtist.util.Tools._
import scala.collection.mutable.ArrayBuffer
import java.lang.Math
import org.apache.spark.mllib.fpm.PrefixSpan
/**
  * Created by chenqingqing on 2016/5/23.
  */
object ScenicSpotAnalysis {

  /**
    * 得到海南省每天在岛的游客数和其省份统计
    * 输入是没有去除震荡的数据，只要有记录就认为是在岛
    * @param month:String 当前处理的月份
    * @param OtherProvRec:RDD[String] = user +","+ time +","+ cell +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @return  RDD[String] = RDD[day+total+prov(31)]
    */
  def customerFlowCount(sc:SparkContext,month:String,OtherProvRec:RDD[String],CustomerInfo:RDD[String]):RDD[String] ={
    val provList = Array("301","303","331","311","308","325","305","318",
      "310","328","313","322","319","323","304","315","329","306","302",
      "324","312","321","320","307","316","314","326","317","330","300")
    val customer = CustomerInfo.map{x=> val user = x.split(",")(0);(user,1)}
    val res1 = OtherProvRec.map{x=> x.split(",") match {
      case Array(user,time,cell,lng,lat,prov) =>
        (user,(time.substring(0,8),prov))}}
      .join(customer)
      .map{x=> val day = x._2._1._1
        val user = x._1
        val prov = x._2._1._2
        (day,(user,prov))}
      .distinct
      .filter{x=> x._1.substring(0,6).equals(month)}

    val arrBuff1 = new ArrayBuffer[Int]()
    val customerCountByDay = res1.groupByKey().map{x=>
      val arr = x._2.toArray
      val totalCountByDay = arr.length
      arrBuff1 += totalCountByDay
      var totalProvCountByDay = 0
      for(prov <- provList){
        var provCount = 0
        for (item <- arr){
          if(item._2.equals(prov)){
            provCount += 1
          }
        }
        totalProvCountByDay += provCount
        arrBuff1 += provCount
      }
      val elseProvCountByDay = totalCountByDay - totalProvCountByDay
      x._1+","+arrBuff1.toArray.mkString(",")+","+elseProvCountByDay

    }//得到海南省每天在岛的游客数和其省份
    val arrBuff2 = new ArrayBuffer[Int]()
    val res2 = res1.map{case(day,(user,prov))=> (user,prov)}
      .distinct()
      .collect()
    val totalCountByMonth = res2.length
    arrBuff2 += totalCountByMonth
    var totalProvCountByMonth = 0
    for(prov <- provList){
      var provCount = 0
      for (item <- res2){
        if(item._2.equals(prov)){
          provCount += 1
        }
      }
      totalProvCountByMonth += provCount
      arrBuff2 += provCount
    }
    val elseProvCountByMonth = totalCountByMonth - totalProvCountByMonth
    val customerCountByMonth = sc.parallelize(Seq(month+","+arrBuff2.toArray.mkString(",")+","+elseProvCountByMonth),1)
    val result = customerCountByMonth.union(customerCountByDay)
    result
  }

  /**
    * 得到海南省内每个景点这个月内的各省客流量
    * @param month:String 当前处理的月份
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+class+","+scenicBDlng+","+scenicBDlat+","+cellid+","+cellBDlng+","+cellBDlat+","+dist
    * @return  RDD[String] = scenicID,prov[31]
    */
  def scenicCustomerFlowByProvByMonth(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    val provList = Array("301","303","331","311","308","325","305","318",
      "310","328","313","322","319","323","304","315","329","306","302",
      "324","312","321","320","307","316","314","326","317","330","300")
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val user = x.split(",")(0)
        val prov = x.split(",")(1)
        (user,prov)}
    val result = OtherProvStopPoint.map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)}}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        x._1.substring(0,6).equals(month) &&
          dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,cell))}
      .distinct
      .join(customer)//过滤出游客
      .map{case(user,((day,cell),prov)) => (cell,(day,user,prov))}
      .join(scenic)
      .map{case(cell,((day,user,prov),scenicID)) => (scenicID,(day,user,prov))}
      .distinct()
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val totalCount = arr.length
        var totalProvCount = 0
        val arrBuff = new ArrayBuffer[Int]()
        for(prov <- provList){
          var provCount = 0
          for (item <- arr){
            if(item._3.equals(prov)){
              provCount += 1
            }
          }
          totalProvCount += provCount
          arrBuff += provCount
        }
        val elseProv = totalCount - totalProvCount
        x._1+","+arrBuff.toArray.mkString(",")+","+elseProv

      }
    result




    }
  /**
    * 得到海南省内每个景点这个月内每天的各省客流量
    *
    * @param month:String 当前处理的月份
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat
    * @return  RDD[String] = scenicID,day,prov[31]
    */
  def scenicCustomerFlowByProvByDay(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String]={
    val provList = Array("301","303","331","311","308","325","305","318",
      "310","328","313","322","319","323","304","315","329","306","302",
      "324","312","321","320","307","316","314","326","317","330","300")
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val user = x.split(",")(0)
      val prov = x.split(",")(1)
      (user,prov)}
    val result = OtherProvStopPoint.repartition(500)
      .map{x=> x.split(",") match {
        case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)}}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        x._1.substring(0,6).equals(month)&&
          dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,cell))}
      .distinct
      .join(customer)//过滤出游客
      .map{ case(user,((day,cell),prov)) => (cell,(day,user,prov))}
      .join(scenic)
      .map{case(cell,((day,user,prov),scenicID)) => (scenicID+","+day,(user,prov))}
      .distinct
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val totalCount = arr.length
        var totalProvCount = 0
        val arrBuff = new ArrayBuffer[Int]()
        for(prov <- provList){
          var provCount = 0
          for (item <- arr){
            if(item._2.equals(prov)){
              provCount += 1
            }
          }
          totalProvCount += provCount
          arrBuff += provCount
        }
        val elseProv = totalCount - totalProvCount
        x._1+","+arrBuff.toArray.mkString(",")+","+elseProv

      }
    result


  }
  /**
    * 得到海南省内每个景点这个月内每天的客流量
    * @param month:String 当前处理的月份
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat
    * @return  RDD[String] = scenicID,total,dayCustomerCount(31)
    */
  def scenicCustomerFlowByMonth(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String]={
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val user = x.split(",")(0)
      val prov = x.split(",")(1)
      (user,prov)}
    val dayList = Range(1,32,1)
    val result = OtherProvStopPoint.repartition(500)
      .map{x=> x.split(",") match {
        case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)}}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        x._1.substring(0,6).equals(month) &&
          dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,cell))}
      .distinct
      .join(customer)//过滤出游客
      .map{x=> (x._1,x._2._1) match {case(user,(day,cell)) => (cell,(day,user))}}
      .join(scenic)
      .map{case(cell,((day,user),scenicID)) => (scenicID,(day,user))}
      .distinct()
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val arrBuff = new ArrayBuffer[Int]()
        arrBuff += arr.length
        for(day <- dayList){
          var provCount = 0
          for (item <- arr){
            if(item._1.substring(6,8).toInt == day){
              provCount += 1
            }
          }
          arrBuff += provCount
        }
        x._1+","+arrBuff.toArray.mkString(",")

      }
    result



  }
  /**
    * 得到海南省内每个景点这个月内每天每小时的客流量
    * @param month:String 当前处理的月份
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat
    * @return  RDD[String] = scenicID,everyhourCustomerCount(24)
    */
  def scenicCustomerFlowByDay(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String]={
    val hourList = Range(0,24,1)
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val user = x.split(",")(0)
      val prov = x.split(",")(1)
      (user,prov)}
    val result = OtherProvStopPoint.repartition(500).map{x=> x.split(",") match {case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)}}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        x._1.substring(0,6).equals(month) &&
          dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,time,cell))}
      .distinct
      .join(customer)//过滤出游客
      .map{x=> (x._1,x._2._1) match {case(user,(day,time,cell)) => (cell,(day,time,user))}}
      .join(scenic)
      .map{case(cell,((day,time,user),scenicID)) => (scenicID+","+day,(time.substring(8,10),user))}
      .distinct()
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val arrBuff = new ArrayBuffer[Int]()
        for(hour <- hourList){
          var provCount = 0
          for (item <- arr){
            if(item._1.toInt == hour){
              provCount += 1
            }
          }
          arrBuff += provCount
        }
        x._1+","+arrBuff.toArray.mkString(",")

      }
    result


  }
  /**
    * 得到不同交通方式的游客归属地组成
    *
    * @param Transport:RDD[String] = user +","+ transportation
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @return  RDD[String] = transport +"," +prov(31).mkString(",")
    */

  def transportationByProv(Transport:RDD[String],CustomerInfo:RDD[String]):RDD[String] ={
    val provList = Array("301","303","331","311","308","325","305","318",
      "310","328","313","322","319","323","304","315","329","306","302",
      "324","312","321","320","307","316","314","326","317","330","300")
    val transport = Transport.map{x=> x.split(",") match {
      case Array(user,transport) => (user,transport)
    }}
    val prov = CustomerInfo.map{x=> x.split(",") match {
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,prov)
    }}
    val result = transport.join(prov)
      .map{case (user,(transport,prov)) => (transport,prov)}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val totalCount = arr.length
        var totalProvCount = 0
        val arrBuff = new ArrayBuffer[Int]()
        for(prov <- provList){
          var provCount = 0
          for (item <- arr){
            if(item.equals(prov)){
              provCount += 1
            }
          }
          totalProvCount += provCount
          arrBuff += provCount
        }
        val elseProv = totalCount - totalProvCount
        x._1+","+arrBuff.toArray.mkString(",")+","+elseProv

      }
    result

  }
  /**
    * 得到不同归属地游客的交通方式组成
    *
    * @param Transport:RDD[String] = user +","+ transportation
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @return  RDD[String] = prov +","+transport_0_Count +","+transport_1_Count +","+transport_2_Count +","+transport_3_Count
    */
  def provinceByTransMode(Transport:RDD[String],CustomerInfo:RDD[String]):RDD[String] ={
    val transModeList = Range(0,4,1)
    val transport = Transport.map{x=> x.split(",") match {
      case Array(user,transport) => (user,transport)
    }}
    val prov = CustomerInfo.map{x=> x.split(",") match {
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,prov)
    }}
    val result = transport.join(prov)
      .map{case (user,(transport,prov)) => (prov,transport)}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val totalCount = arr.length
        var portion:Double = 0
        val arrBuff = new ArrayBuffer[Int]()
        for(mode <- transModeList){
          var provCount = 0
          for (item <- arr){
            if(item.toInt == mode){
              provCount += 1
            }
          }
          arrBuff += provCount
        }
        //val elseProv = totalCount - totalProvCount
        x._1+","+arrBuff.toArray.mkString(",")

      }
    result

  }
  /**
    * 景区的平均停留时长
    *
    * @param month:String 当前处理的月份
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat

    * @return  RDD[(String,String)] = RDD[scenicSpotId+","+averageStayDur]
    */
  def scenicSpotAveStayDur(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val line = x.split(",")
      val user = line(0)
      (user,1)}
    val result = OtherProvStopPoint.repartition(500).map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)}}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        x._1.substring(0,6).equals(month) &&
          dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,cell,dur))}
      .join(customer)//过滤出游客
      .map{x=> x match {case (user, ((day,cell, dur), 1)) => (cell,(day,user,dur))}}
      .join(scenic)
      .map{x=> x match {case(cell,((day,user,dur),scenicID)) => (scenicID+","+user+","+day,dur.toDouble)}}
      .reduceByKey(_+_) //得到每个用户每天在每个景区的停留时长
      .map{x=>
        val scenicID = x._1.split(",")(0)
        val dur = x._2
        (scenicID,dur)}
      .groupByKey()
      .map{x=> val usersNum = x._2.toArray.length
        val totalDur = x._2.reduce(_+_)
        val ave = totalDur/usersNum
        (x._1,ave)}
      .map{x=> x._1 +","+x._2}

    result




  }
  /**
    * 每个景区的固定停留时长内人数统计
    * @param month:String 当前处理的月份
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat

    * @return  RDD[String] = scenicSpotId+","+dur1-2Count +","+dur2-3Count +","+dur3-4Count +","+dur4-5Count +","+durOver5Count
    */
  def scenicSpotStayDurCount(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    val durList = Range(1,5,1)
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val line = x.split(",")
      val user = line(0)
      (user,1)}
    val result = OtherProvStopPoint.repartition(500).map{x=> x.split(",") match {
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)}}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        x._1.substring(0,6).equals(month)&&
          dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,cell,dur))}
      .join(customer)//过滤出游客
      .map{x=> x match {case (user, ((day,cell, dur), 1)) => (cell,(day,user,dur))}}
      .join(scenic)
      .map{x=> x match {case(cell,((day,user,dur),scenicID)) => (scenicID+","+user+","+day,dur.toDouble)}}
      .reduceByKey(_+_)
      .map{x=>
        val scenicID = x._1.split(",")(0)
        val dur = x._2
        (scenicID,dur)}
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val totalCount = arr.length
        var totalCountWithOutElse = 0
        val arrBuff = new ArrayBuffer[Int]()
        for(durParse <- durList){
          var count = 0
          for (item <- arr){
            if(item.toInt == durParse){
              count += 1
            }
          }
          arrBuff += count
          totalCountWithOutElse += count
        }
        val elseProv = totalCount - totalCountWithOutElse
        x._1+","+arrBuff.toArray.mkString(",")+","+elseProv

      }
    result


  }

  /**
    * 返回用户停留天数统计
    *
    * @param CustomerInfo:RDD[String]
    * @return
    */
  def customerStayDaysCount(sc:SparkContext,CustomerInfo:RDD[String]):RDD[String] ={
    val daysList = sc.parallelize(Range(3,16,1),1).map{x=> (x.toString,1)}
    val daysCount = CustomerInfo.map{x=> x.split(",") match {
      case Array(user,prov,startDate,endDATE,days) => (days,user)}}
      .distinct()
      .map{case (days,user) => (days,1)}
      .reduceByKey(_+_)
      .rightOuterJoin(daysList)
      .map{x=> val count = x._2._1.getOrElse(0);x._1+","+count}
      .repartition(1)
    daysCount


  }
  /**
    * 返回每天入岛和离岛的用户统计
    * @param month:String 当前处理的月份
    * @param CustomerInfo:RDD[String]
    * @return
    */

  def everyDayInAndOutCustomerCount(sc:SparkContext,month:String,CustomerInfo:RDD[String]):RDD[String] ={
    val datesList = sc.parallelize(getDatesArray(month+"01",month+"31"),1).map{x=> (x.toString,1)}
    val inDateCount = CustomerInfo.map{x=> x.split(",") match {
      case Array(user,prov,startDate,endDATE,days) => (startDate,user)}}
      .distinct()
      .map{case (startDate,user) => (startDate,1)}
      .reduceByKey(_+_)
      .rightOuterJoin(datesList)
      .map{x=> val inCount = x._2._1.getOrElse(0);(x._1,inCount)}
    val result = CustomerInfo.map{x=> x.split(",") match {
      case Array(user,prov,startDate,endDate,days) => (endDate,user)}}
      .distinct()
      .map{case (endDate,user) => (endDate,1)}
      .reduceByKey(_+_)
      .rightOuterJoin(inDateCount)
      .map{x=> val outCount = x._2._1.getOrElse(0)
        x._1+","+x._2._2+","+outCount}
    result

  }

  /**
    *
    * @param sc
    * @param month 当前月份
    * @param Thred1 游客停留时间下限
    * @param Thred2 游客停留时间上限
    * @param OtherProvStopPoint
    * @param CustomerInfo
    * @param Scenic
    * @return
    */
  def HotTravelRoute(sc:SparkContext,month:String,Thred1:Int,Thred2:Int,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val line = x.split(",")
      val user = line(0)
      val stayDays = line(4)
      (user,stayDays)}
    val sp = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)
    }}
      .filter{x=> val hour = x._3.substring(8,10);val dur = x._5.toDouble
        dur >= 1 &&
          hour.toInt >= 7 &&
          hour.toInt <= 20 &&
        x._1.substring(0,6).equals(month)
      }
      .map{case(day,user,time,cell,dur) =>(user,(cell,time))}
      .join(customer)//过滤出游客
      .filter{x=> val stayDays = x._2._2.toInt;stayDays >= Thred1 && stayDays <= Thred2}
      .map{case (user, ((cell, time), stayDays)) => (cell,(user,time))}
      .distinct()
      .join(scenic)
      .map{case (cell, ((user,time), scenic)) => (user,(time,scenic))}
      .distinct
      .groupByKey()
      .map{x=> val arr =x._2.toArray.sortWith((a,b) =>
        timetostamp(a._1) < timetostamp(b._1))
        val distinctedArrBuff = new ArrayBuffer[String]()
        for(i <- arr){
          distinctedArrBuff += i._2
        }
        val distinctedArr = distinctedArrBuff.toArray.distinct
        val arrBuff = new ArrayBuffer[Array[String]]()
        for (item <-distinctedArr){
          val itemArr = new Array[String](1)
          itemArr(0) = item
          arrBuff += itemArr

        }

        arrBuff.toArray.distinct
      }


    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.001)
    val model = prefixSpan.run(sp)
    val resultBuff = new ArrayBuffer[String]()
    for (i<- 2 until 5){
      val result = model.freqSequences
        .filter{x=> val len = x.sequence.length;len == i}
        .map{x=> (x.sequence.map(_.mkString(":")).mkString("#"),x.freq)}
        .collect
        .sortWith((a,b)=> a._2 >b._2)
      var index = 0
      for (route <- result){
        if(index<3){
          resultBuff += route._1+","+route._2
          index += 1
        }

      }


    }
    val output = resultBuff.toArray.distinct
    val outputRDD= sc.parallelize(output,1)
    //val aveCount = sp.map{x=> x.length}.reduce(_+_)/sp.count.toDouble
    outputRDD
  }

  /**
    * CustomerHome:RDD[String]  = RDD[user+","+homeCell +"," + baiduLng +","+ baiduLat]
    * @return
    */
  def customerHomePlaceCount(CustomerHome:RDD[String]):RDD[String]={
    val count = CustomerHome.map{x=> (x.split(",").slice(2,4).mkString(","),x.split(",")(0))}
      .distinct()
      .map{x=> (x._1,1)}
      .reduceByKey(_+_)
      .map{x=> x._1+","+x._2}
    count

  }
  /**
    * 指定日期的景区迁徙流
    * @param OtherProvStopPoint:RDD[String] = day+","+user +","+ time +","+ cell +","+ dur +","+ lng +","+ lat +","+ prov
    * @param CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    * @param Scenic:RDD[String] = scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat

    * @return mergedMigrationDataWithDate:RDD[String] = date+","+scenicSpot1+","+scenicSpot2+","+count
    */
  def mergeMigrationDataByDay(OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[String] ={
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val customer = CustomerInfo.map{x=> val line = x.split(",")
      val user = line(0)
      (user,1)}
    val sp = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (day,user,time,cell,dur)
    }}
      .filter{x=>
        val hour = x._3.substring(8,10)
        val dur = x._5.toDouble
        dur >= 1 && hour.toInt >= 7 && hour.toInt <= 20}
      .map{case(day,user,time,cell,dur) =>(user,(day,cell,time))}
      .join(customer)//过滤出游客
      .map{case (user, ((day,cell, time), 1)) => (cell,(day+","+user,time))}
      .join(scenic)
      .map{case(cell,((dayUser,time),sid)) => (dayUser,(sid,time))}
      .groupByKey()
      .flatMap{x=>
        val arr = x._2.toArray.sortWith((a,b) => timetostamp(a._2,"yyyyMMddHHmmss").toLong < timetostamp(b._2,"yyyyMMddHHmmss").toLong)
        val migrateBuffer = new ArrayBuffer[(String,String)]()
        if( arr.length == 1){
          migrateBuffer += Tuple2(x._1,"none")
        }
        else{
          for(i<- 0 until arr.length -1){
            migrateBuffer += Tuple2(x._1,arr(i)._1+","+arr(i+1)._1)
          }
        }
        migrateBuffer.toSeq
      }
      .distinct
      .filter{x=> x._2.equals("none") == false}
      .map{case (dayUser,s1s2) => (dayUser.split(",")(0)+","+s1s2,1)}
      .reduceByKey(_+_)
      .map{x=> x._1 +","+x._2}
    sp

  }

  /**
    * 指定月份，按月求和的景区迁徙流
    * @param mergedMigrationDataWithDate:RDD[String] = date+","+scenicSpot1+","+scenicSpot2+","+count

    * @return mergeMigrationDataByMonth:RDD[String] = scenicSpot1+","+scenicSpot2+","+count
    */
  def mergeMigrationDataByMonth(mergedMigrationDataWithDate:RDD[String]):RDD[String] ={

    val result = mergedMigrationDataWithDate.map{x=> x.split(",") match{
      case Array(day,scenicSpot1,scenicSpot2,count) => ((scenicSpot1,scenicSpot2),count.toInt)
    }}
      .reduceByKey(_+_)
      .map{case ((scenicSpot1,scenicSpot2),count) => scenicSpot1+","+scenicSpot2+","+count}
    result

  }

  /**
    * 景区的Rank指标-三种：流量，停留时间，累计停留时间
    * @param scenicCusFlowByMonth：RDD[String] = scenicID,total,dayCustomerCount(31)
    * @param scenicAveStayDur：RDD[(String,String)] = RDD[scenicSpotId+","+averageStayDur]
    * @return
    */
  def scenicRank(Scenic:RDD[String],scenicCusFlowByMonth:RDD[String],scenicAveStayDur:RDD[String]):RDD[String]={
    val scenic = Scenic.map{x=> x.split(",") match{
      case Array(sid,name,status,slng,slat,cid,clng,clat,dist) => (cid,sid)}}
    val flow= scenicCusFlowByMonth.map{x=> val line = x.split(",")
      val scenicID = line(0)
      val totalCount = line(1).toInt
      (scenicID,totalCount)}
      .rightOuterJoin(scenic)
      .map{x=>
        val count = x._2._1.getOrElse(0)
        val scenicID= x._1
        (scenicID,count)
      }

    val aveStayDur = scenicAveStayDur.map{x=> x.split(",") match {
      case Array(scenicID,aveDur) => (scenicID,aveDur.toDouble)
    }}
      .rightOuterJoin(scenic)
      .map{x=> val avedur = x._2._1.getOrElse(0.0)
        val scenicID= x._1
        (scenicID,avedur)
      }
    val accumDur = flow.join(aveStayDur)
      .map{case(scenicID,(count,dur)) => (scenicID,count*dur/1000)}
      .rightOuterJoin(scenic)
      .map{x=> val accumDur = x._2._1.getOrElse(0.0)
        val scenicID= x._1
        (scenicID,accumDur)}
    val result = flow.join(aveStayDur).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(accumDur).map{x=> x._1+","+x._2._1+","+x._2._2}
    result


  }



}
