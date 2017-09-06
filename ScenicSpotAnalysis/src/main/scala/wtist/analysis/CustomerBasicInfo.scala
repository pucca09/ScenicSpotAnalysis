package wtist.analysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import wtist.util.Tools._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenqingqing on 2016/7/1.
  */
object CustomerBasicInfo {
  /**
    * 识别游客,在岛内停留3-15天
    *
    * @param OtherProvTrueUserInfo:RDD[String] = RDD[user +","+ time +","+ cell+","+lng +","+ lat+","+prov]
    * @return CustomerInfo:RDD[String]  = user+","+prov+","+firstDate+","+lastDate+","+stayDays
    */
  def customerDetect(month:String,OtherProvTrueUserInfo:RDD[String]):RDD[String]={
    val customerInfo = OtherProvTrueUserInfo.map{x=> x.split(",") match {
      case Array(user,time,cell,lng,lat,prov) => (user+","+prov,time)
    }}
      .filter(x=> x._2.substring(0,6).equals(month))
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b) =>
        timetostamp(a).toLong < timetostamp(b).toLong)
        val len = arr.length
        val firstDate = arr(0).substring(0,8)
        val lastDate = arr(len-1).substring(0,8)
        val days = lastDate.substring(6,8).toInt - firstDate.substring(6,8).toInt + 1
        (x._1,firstDate,lastDate,days)}
      .filter{x=> val days = x._4;days >= 3 && days <= 15}
      .map{case(userAndProv,firstDate,lastDate,days) =>
        (userAndProv+","+firstDate+","+lastDate+","+days)}
      .repartition(1)
    customerInfo

  }
  /**
    * 返回当前用户来琼方式
    *
    * @param gps:(String,String) 游客的最后一个在岛停留点的百度经纬度
    * @return Int:0表示自驾或游轮，1表示飞机，2表示火车，3表示无法识别
    *PS:0：自驾港口 南港售票处百度坐标 （110.165757,20.04579） 秀英港百度坐标 （110.466232,19.945878）
    *   1：飞机场 海口美兰国际机场百度坐标 （110.466232,19.945878） 三亚机场（109.414883,18.313457）
    *   2：海口站（110.168949,20.033696） 三亚站（109.499415,18.302055）
    *算法描述：第一个/最后一个停留点（0.5h）离这些位置哪个近，且距离必须小于2Km,如果到所有点的距离都大于2km就判定为无法识别
    */
  def TransportationDetect(gps:(String,String)):Int ={
    val xiuyingPort = ("110.293096","20.026518")
    val southPort = ("110.165718","20.04586")
    val haikouAirPort = ("110.466232","19.945878")
    val sanyaAirPort = ("109.414883","18.313457")
    val haikouStation = ("110.168949","20.033696")
    val sanyaStation = ("109.499415","18.302055")
    val disArrayBuffer = new ArrayBuffer[(Int,Double)]()
    val disToPort = if(GetDistance(gps,xiuyingPort)<=GetDistance(gps,southPort)) GetDistance(gps,xiuyingPort) else GetDistance(gps,southPort)
    val disToAirport = if(GetDistance(gps,haikouAirPort)<=GetDistance(gps,sanyaAirPort)) GetDistance(gps,haikouAirPort) else GetDistance(gps,sanyaAirPort)
    val disToRailwayStation = if(GetDistance(gps,haikouStation)<=GetDistance(gps,sanyaStation)) GetDistance(gps,haikouStation) else GetDistance(gps,sanyaStation)
    disArrayBuffer += Tuple2(0,disToPort)
    disArrayBuffer += Tuple2(1,disToAirport)
    disArrayBuffer += Tuple2(2,disToRailwayStation)
    val sortedArr = disArrayBuffer.toArray.sortWith((a,b) => a._2 < b._2)
    var res = 3
    if(sortedArr(0)._2 <= 10 ){
      //如果距离最小的位置点的距离满足小于2km,就认定为相应的交通方式，否则认定为无法判断
      res = sortedArr(0)._1
    }
    res

  }
  /**
    * 计算外地游客的来琼方式
    *
    * @param  OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  CustomerInfo:RDD[String]  = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param  cellTrans:RDD[String]
    * @return  RDD[(String,String)] = RDD[user+"," +transportation]
    */
  def transportationIn(OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],cellTrans:RDD[String]):RDD[String] ={
    val bd = cellTrans.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        (cell,(bdLng,bdLat))}//(cell,(bdlng,bdlat))
    val customer = CustomerInfo.map{x=> x.split(",") match
      {case Array(user,prov,firstDay,lastDay,days) =>(user,(firstDay,lastDay))}}
    //计算外地游客在岛最后一天的记录
    val recAtLastDay = OtherProvStopPoint.map{x=>
      val line =x.split(",");(line(3),x)}//(cell,line)
      .join(bd)
      .map{x=>
        val line= x._2._1
        val user = line.split(",")(1)
        val time = line.split(",")(2)
        val dur = line.split(",")(4)
        val bdlnglat = x._2._2
        (user,(time,dur,bdlnglat))}
      .join(customer)
      .filter{x=>
        val day = x._2._1._1.substring(0,8)
        val firstDay = x._2._2._1
        val lastDay = x._2._2._2
        day.equals(firstDay)
      }
      .map{case((user,((time,dur,(lng,lat)),(firstDay,lastDay)))) => (user,(time,dur,(lng,lat)))}
    val transport = recAtLastDay.map{case((user,(time,dur,(lng,lat)))) =>
      (user,(time,(lng,lat)))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>
        timetostamp(a._1) < timetostamp(b._1))
        (x._1,TransportationDetect(arr(0)._2))
      }
    val completeSet = customer.leftOuterJoin(transport)
      .map{x=> x._1+","+x._2._2.getOrElse(3)}

    completeSet



  }
  /**
    * 计算外地游客的离琼方式
    *
    * @param  OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat] ；
    * @param  CustomerInfo:RDD[String]  = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param  cellTrans:RDD[String]
    * @return  RDD[(String,String)] = RDD[user+"," +transportation]
    */
  def transportationOut(OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],cellTrans:RDD[String]):RDD[String] ={
    val bd = cellTrans.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        (cell,(bdLng,bdLat))}//(cell,(bdlng,bdlat))
    val customer = CustomerInfo.map{x=> x.split(",") match
      {case Array(user,prov,firstDay,lastDay,days) =>(user,(firstDay,lastDay))}}
    //计算外地游客在岛第一天的记录
    val recAtFirstDay = OtherProvStopPoint.map{x=>
      val line =x.split(",");(line(3),x)}//(cell,line)
      .join(bd)
      .map{x=>
        val line= x._2._1
        val user = line.split(",")(1)
        val time = line.split(",")(2)
        val dur = line.split(",")(4)
        val bdlnglat = x._2._2
        (user,(time,dur,bdlnglat))}
      .join(customer)
      .filter{x=>
        val day = x._2._1._1.substring(0,8)
        val firstDay = x._2._2._1
        val lastDay = x._2._2._2
        day.equals(lastDay)
      }
      .map{case((user,((time,dur,(lng,lat)),(firstDay,lastDay)))) => (user,(time,dur,(lng,lat)))}
    val transport = recAtFirstDay.map{case((user,(time,dur,(lng,lat)))) =>
      (user,(time,(lng,lat)))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=>
        timetostamp(a._1) > timetostamp(b._1))
        (x._1,TransportationDetect(arr(0)._2))
      }
    val completeSet = customer.leftOuterJoin(transport)
      .map{x=> x._1+","+x._2._2.getOrElse(3)}

    completeSet



  }
  /**
    * 识别游客住宿地点的基站并返回百度坐标,规则：00：00-06：00时间段内停留时间最长的基站
    *
    * @param OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat]
    * @param cellTrans:RDD[String]
    * @param CustomerInfo:RDD[String]= RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @return CustomerHome:RDD[String]  = RDD[user+","+homeCell +"," + baiduLng +","+ baiduLat]
    */
  def homePlaceForCustomer(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],cellTrans:RDD[String]):RDD[String] ={
    val bd = cellTrans.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        (cell,(bdLng,bdLat))}//(cell,(bdlng,bdlat))
    val customer = CustomerInfo.map{x=> val line = x.split(",")
        val user = line(0)
        (user,1)}
    val result = OtherProvStopPoint.map{x=> val time = x.split(",")(2);(time,x)}
      .filter{x=> val hour = x._1.substring(8,10).toInt
        val day = x._1.substring(0,6)
        hour >=0 && hour <=6 &&
        day.equals(month)
      }
      .map{x=> val user = x._2.split(",")(1)
        val cell = x._2.split(",")(3)
        val dur = x._2.split(",")(4)
        (user+","+cell,dur.toDouble)}
      .filter(x=> x._2 >=1)//停留时间>=1h才算停留点
      .reduceByKey(_+_)
      .map{x=> val user = x._1.split(",")(0)
        val cell = x._1.split(",")(1)
        val dur = x._2
        (user,(cell,dur))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b)=> a._2> b._2)
        (arr(0)._1,x._1)}
      .join(bd)
      .map{case(cell,(user,(bdLng,bdLat))) => (user,cell+","+bdLng+","+bdLat)}
      .join(customer)
      .map{case(user,(home,1)) => (user,home)}
      .rightOuterJoin(customer)
      .map{x=> x._1+","+x._2._1.getOrElse("None,None,None")}
      .repartition(1)
    result
  }
  /**
    * 识别游客类型
    *
    * @param LocalTrueUserInfo:RDD[String] = RDD[user +","+ time +","+ cell+","+lng +","+ lat+","+prov]
    * @param CustomerInfo:RDD[String] = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param CDRData:RDD[String]
    * @return CustomerType = RDD[user+"|"+type]
    */
  def CustomerTypeDetect(LocalTrueUserInfo:RDD[String],CustomerInfo:RDD[String],CDRData:RDD[String]):RDD[(String,String)]={
    val recUsers = CDRData.flatMap{x=> val slice = x.split(",")
      Seq((slice(3),slice(4)),(slice(4),slice(3)))}
      .distinct()
      .filter{x=> x._1.equals("") == false && x._2.equals("") == false}
    val contactorNum = recUsers.groupByKey().map{x=> val arr = x._2.toArray;(x._1,arr.length)}
    val otherProvUser =  CustomerInfo.map{x=> x.split(",") match {
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,stayDays)
    }}
    val trueUser = LocalTrueUserInfo.map{x=> x.split(",") match{
      case Array(user,time,cell,lng,lat,prov) => (user,1)
    }}
    //birdUser1 = 呆的时间超过25天的外地人
    val birdUser1 = otherProvUser.filter{x=> x._2.toInt >= 25}.map{x=>x._1}
    val otherProContactorNum = trueUser.join(recUsers)
      .map{x=> (x._2._2,x._1)}
      .join(otherProvUser)
      .distinct
      .map{x=>(x._2._1,1)}
      .reduceByKey(_+_)
    val birdUser2 = contactorNum.leftOuterJoin(otherProContactorNum)
      .map{x=> val otherProNum = x._2._2.getOrElse(0)
        val user = x._1
        val totalNum = x._2._1
        (user,totalNum,otherProNum)}
      .filter{x=> x._3.toInt >= x._2.toDouble/2}
      .map{x=> x._1}
    val birdUser = birdUser1.union(birdUser2)
      .map{x=> (x,"bird")}
      .distinct()

    val customer = otherProvUser.filter{x=> x._2.toInt <= 7}
    //val CDRData = sc.textFile("/user/tele/hainan/location/CC/8_CCData")
    val businessUser = CDRData.flatMap{x=> val slice = x.split(",")
      Seq((slice(3),slice(1)),(slice(4),slice(1)))}
      .filter{x=> x._1.equals("") == false }
      .join(customer)
      .map{x=>val user = x._1;val day = x._2._1.substring(0,8)
        (user+","+day,1)}
      .reduceByKey(_+_)
      .map{x=> val user = x._1.split(",")(0);val dayCall = x._2
        (user,dayCall)}
      .groupByKey()
      .map{x=>
        val user = x._1
        val total = x._2.reduce(_+_)
        val days = x._2.toArray.length.toDouble
        val res = total/days
      (user,res)}
      .filter{x=> x._2 >=3}
      .map{x=> (x._1,"business")}
      .distinct()
    val customerType = otherProvUser.leftOuterJoin(birdUser)
      .map{x=> val cusType = x._2._2.getOrElse("none")
        (x._1,cusType)}
      .leftOuterJoin(businessUser)
      .map{x=> (x._1,x._2._1,x._2._2.getOrElse("none"))}
      .map{_ match {
        case (user,"none","business") => (user,"business")
        case (user,"bird","none") => (user,"bird")
        case x => (x._1,"bird")
      }}

    customerType


  }


  /**
    * 游客是否访问过三亚免税店
    *
    * @param month:String,当前月份
    * @param OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat]
    * @param CustomerInfo:RDD[String] = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param Scenic:RDD[String] = RDD[scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat]
    * @return customerVisitedDutyFreeShop
    */
  def DutyFreeShopVisit(month:String,CustomerInfo:RDD[String],OtherProvStopPoint:RDD[String],Scenic:RDD[String]):RDD[(String,String)] ={
    val scenic = Scenic.map{x=> val line = x.split(",")
      val cell = line(5).trim
      val scenicId = line(0).trim
      (cell,scenicId)}
      .filter{x=> x._2.matches("[0-9]+")}
    val dutyFreeShopCell = scenic.filter{x=> x._2.equals("19")}.map{x=> x._1}.distinct().collect()
    val customer = CustomerInfo.map{x=> x.split(",") match{
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,1)}
    }
    val result = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,time,cell,dur)
    }}
      .filter{x=> x._2.substring(0,6).equals(month) && x._4.toDouble >= 1}
      .map{case(user,time,cell,dur) => (user,cell)}
      .distinct
      .filter{x=> dutyFreeShopCell.contains(x._2)}
      .map{x=> x._1}
      .distinct()
      .map{x=> (x,"1")}
      .rightOuterJoin(customer)
      .map{x=> (x._1,x._2._1.getOrElse("0"))}

    result

  }
  /**
    * 游客通话强度
    *
    * @param CustomerInfo:RDD[String] = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param CDRData:RDD[String]
    * @return customerCallStrength
    */
  def CustomerCallStrength(CustomerInfo:RDD[String],CDRData:RDD[String]):RDD[(String,String)]={
    val customer = CustomerInfo.map{x=> x.split(",") match{
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,1)}
    }
    val callStrength = CDRData.flatMap{x=> val slice = x.split(",")
      Seq((slice(3),slice(1)),(slice(4),slice(1)))}
      .filter{x=> x._1.equals("") == false }
      .join(customer)
      .map{x=>val user = x._1;val day = x._2._1.substring(0,8)
        (user+","+day,1)}
      .reduceByKey(_+_)
      .map{x=> val user = x._1.split(",")(0);val dayCall = x._2
        (user,dayCall)}
      .groupByKey()
      .map{x=>
        val user = x._1
        val total = x._2.reduce(_+_)
        val days = x._2.toArray.length.toDouble
        val aveDayCall = total/days
        val res = if(aveDayCall < 3){
          "1"

        }else if(aveDayCall>= 3 && aveDayCall<= 6){
          "2"
        }else{
          "3"
        }
        (user,res)}
      .rightOuterJoin(customer)
      .map{x=>(x._1,x._2._1.getOrElse("1"))}

    callStrength


  }
  /**
    * 游客白天TOP3停留点经纬度
    *
    * @param CustomerInfo:RDD[String] = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat]
    * @return customerTop3LocAtDay:RDD[(String,String)] = (user,top1GPS+","+top2GPS+","+top3GPS)
    */
  def CustomerTop3LocAtDay(month:String,CustomerInfo:RDD[String],OtherProvStopPoint:RDD[String]):RDD[(String,String)]={
    val customer = CustomerInfo.map{x=> x.split(",") match{
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,1)}
    }
    val top3 = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,time,lng+","+lat,dur)}
    }.filter{x=> x._2.substring(0,6).equals(month) && x._4.toDouble >= 1}
      .map{case(user,time,cellGps,dur) => (user+","+cellGps,1)}
        .reduceByKey(_+_)
        .map{x=> val user = x._1.split(",")(0)
        val cellGps = x._1.split(",").slice(1,3).mkString(",")
          (user,(cellGps,x._2))}
      .groupByKey()
      .map{x=> val arr = x._2.toArray.sortWith((a,b) => a._2 >b._2)
      if(arr.length >= 3){
        (x._1,arr(0)._1+","+arr(1)._1+","+arr(2)._1)
      }else if(arr.length == 2){
        (x._1,arr(0)._1+","+arr(1)._1+","+"None,None")
      }else{
        (x._1,arr(0)._1+","+"None,None,None,None")

      }
    }
      .rightOuterJoin(customer)
      .map{x=> (x._1,x._2._1.getOrElse("None,None,None,None,None,None"))}
    top3

  }

  /**
    * 游客访问的景点个数和总时长
    *
    * @param CustomerInfo:RDD[String] = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat]
    * @param Scenic:RDD[String] = RDD[scenicid+","+name+","+scenicBDlng+","+scenicBDlat+","+class+","+cellid+","+cellBDlng+","+cellBDlat]
    * @return
    */
  def customerScenicVisitCount(month:String,CustomerInfo:RDD[String],OtherProvStopPoint:RDD[String],Scenic:RDD[String]):RDD[(String,String)]={
    val customer = CustomerInfo.map{x=> x.split(",") match{
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,1)}
    }
    val scenic = Scenic.map{x=> val line = x.split(",")
      val cell = line(5).trim
      val scenicId = line(0).trim
      (cell,scenicId)}.filter{x=> x._2.matches("[0-9]+")}
    val durAndCount = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (cell,(user,time,dur.toDouble))}
    }.filter{x=> x._2._2.substring(0,6).equals(month) && x._2._3 >= 1}
      .join(scenic)
      .map{case(cell,((user,time,dur),scenicId)) => (user,scenicId,dur)}
    val scenicDur = durAndCount.map{case(user,scenicId,dur) =>(user,dur)}
      .reduceByKey(_+_)
      .rightOuterJoin(customer)
      .map{x=>(x._1,x._2._1.getOrElse(0))}
    val scenicCount = durAndCount.map{case(user,scenicId,dur) =>(user,scenicId)}
        .distinct()
        .map{case(user,scenicId)=>(user,1)}
        .reduceByKey(_+_)
        .rightOuterJoin(customer)
        .map{x=>(x._1,x._2._1.getOrElse(0))}
    val result = scenicCount.join(scenicDur)
      .map{case(user,(count,dur)) => (user,count+","+dur)}
    result

  }

  /**
    * 游客最大离开住宿地点距离
    * @param month:String
    * @param CustomerInfo:RDD[String] = RDD[user+"," +prov+"," +firstDate+"," +lastDate+"," +stayDays]
    * @param OtherProvStopPoint:RDD[String] = RDD[day+"," + user + "," + timestamp + "," + cell_id+","+dur+","+lng+","+lat]
    * @param CustomerHome:RDD[String]  = RDD[user+","+homeCell +"," + baiduLng +","+ baiduLat]
    * @return
    */
  def maxDisFromHome(month:String,CustomerInfo:RDD[String],OtherProvStopPoint:RDD[String],CustomerHome:RDD[String]):RDD[(String,Double)]={
    val customer = CustomerInfo.map{x=> x.split(",") match{
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,1)}
    }
    val home = CustomerHome.map{x=> x.split(",") match{
      case Array(user,cell,lng,lat) =>(user,(lng,lat))
    }}
    val sp = OtherProvStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,((lng,lat),time,dur.toDouble))}
    }.filter{x=> x._2._2.substring(0,6).equals(month) && x._2._3 >= 1}
      .join(customer)
      .map{x=>val user = x._1
        val lnglat = x._2._1._1
        (user,lnglat)}
      .leftOuterJoin(home)
      .map{x=> val current = x._2._1
        val home = x._2._2.getOrElse("None","None")
        (x._1,GetDistance(current,home))
      }
      .filter{x=> x._2 < 100000.0}
      .groupByKey()
      .map{x=>val arr = x._2.toArray.sortWith(_>_)
        (x._1,arr(0))}
      .rightOuterJoin(customer)
      .map{x=> (x._1,x._2._1.getOrElse(-1.0))}

    sp



  }

  /**
    * 返回游客住宿地点附近酒店类型
    * @param sc
    * @param pois:RDD[String] POI信息，|分割
    * @param cellTrans:RDD[String] 百度转码过后的基站
    * @param CustomerHome:RDD[String] = RDD[user,cell,lng,lat]
    * @return RDD[(String,String)] = RDD[(User,HotelType)]
    */
  def hotelClassify(sc:SparkContext,pois:RDD[String],cellTrans:RDD[String],CustomerHome:RDD[String]):RDD[(String,String)]={
    val hotels = pois.map{_.split("\\|")}.filter{x=> val tags = x(7).split(";")(0);tags.equals("酒店")}.map{x=> x.mkString("|")}.distinct
    val house = pois.map{_.split("\\|")}.filter{x=> val tags = x(7).split(";")(0);tags.equals("房地产")}.map{x=> x.mkString("|")}.distinct
    val processed_hotels = hotels.map{x=> val flags = x.split("\\|");val arr = flags(7).split(";");val name = flags(0)
      val label = new ArrayBuffer[String]()
      if(arr.length >1 && arr(1).equals("其他") == false){
        if(arr(1).equals("星级酒店")){
          label += "0"
        }
        else if (arr(1).equals("快捷酒店")){
          label += "1"
        }
        else if (arr(1).equals("公寓式酒店")){
          if(name.contains("家庭旅馆") == true){
            label += "3"
          }
          else{
            label += "none"
          }

        }
      }else{
        if(name.contains("快捷") == true){
          label += "1"

        }
        if(name.contains("公寓") == true){
          label += "2"
        }
        if(name.contains("家庭旅馆") == true){
          label += "3"
        }
        if(name.contains("客栈") == true|| name.contains("民宿") == true){
          label += "4"
        }
        if(name.contains("青年") == true){
          label += "5"
        }
        if(name.contains("商务") == true){
          label += "6"
        }
        if(name.contains("招待所") == true){
          label += "7"
        }
        if(name.contains("温泉") == true){
          label += "8"
        }
        if(name.contains("海景") == true || name.contains("海滨") == true){
          label += "9"
        }
        if(name.contains("情侣") == true){
          label += "10"
        }
        if(name.contains("出租") == true || name.contains("住宿") == true || name.contains("短租") == true){
          label += "11"
        }
        if(name.contains("别墅") == true ||name.contains("公馆") == true || name.contains("山庄") == true){
          label += "12"
        }

      }
      if(label.toArray.length == 0){
        label += "none"
      }
      val labelArr = if(label.toArray.length >1 && label.toArray.contains("none")){
        label.toArray.filter{x=> x.equals("none") == false}.distinct

      }else{
        label.toArray.distinct

      }
      val result = if(flags.length == 18){
        "hotel"+"|"+flags(0)+"|"+flags(3)+"|"+flags(2)+"|"+flags(8)+"|"+flags.slice(10,19).mkString("|")+"|"+labelArr.mkString("#")
      }else{
        "hotel"+"|"+flags(0)+"|"+flags(3)+"|"+flags(2)+"|"+Array(-1,-1,-1,-1,-1,-1,-1,-1,-1).mkString("|")+"|"+labelArr.mkString("#")
      }
      result
    }.map{_.split("\\|")}.map{x=> val arr = new ArrayBuffer[String]()
      for (item <- x){
        if (item.equals("")){
          arr += "-1"
        }else{
          arr += item
        }
      }
      arr.toArray.mkString("|")
    }
      .distinct
      .repartition(1)

    //processed_hotels.filter{x=>x.split("\\|")(13).equals("none")}.count
    //val secondLabels = hotels.filter{x=> val len = x.split("\\|")(7).split(";").length;len > 1}.count
    //得到基站和酒店的对应关系表

    val Thred = processed_hotels.count.toInt
    val numberList =sc.parallelize(Range(0,Thred),1)
    //hotelInfo:hotelID|name|lng|lat|price|....|flags("#")
    val hotelInfo = processed_hotels.zip(numberList).map{x=> x._2 +"|"+x._1.split("\\|").slice(1,14).mkString("|")}
    val cell_trans = cellTrans.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        val radius = if(x(11).equals("")) 1.0 else x(11).toDouble/1000
        (cell,(bdLng,bdLat),radius)}
    val hotelArr = hotelInfo.map{_.split("\\|")}.map{x=> (x(0),x(1),(x(2),x(3)),x(13))}.collect
    //return cellid,hotelid,hotelname,flags("#")
    val cellHotelMap = cell_trans.map{x=>
      val cell_id = x._1
      val cell_gps = x._2
      val cell_radius = x._3
      val arrBuff = new ArrayBuffer[String]()
      for (hotel <- hotelArr){
        val hotel_id = hotel._1
        val hotel_name = hotel._2
        val hotel_gps = hotel._3
        val hotel_flag = hotel._4
        val dis = GetDistance(cell_gps,hotel_gps)
        //println(dis)
        if(dis <= cell_radius){
          arrBuff += hotel_id+"|"+hotel_name+"|"+hotel_flag
        }


      }
      val res = if (arrBuff.toArray.length == 0){
        (cell_id,Array("-1"))
      }
      else{
        (cell_id+"|"+cell_radius,arrBuff.toArray)

      }
      res

    }
      .flatMapValues(x=>x)
      .filter{x=> x._2.equals("-1") == false}
      .map{x=> x._1 + "|"+x._2}
      .distinct
      .map{x=> x.split("\\|") match {case Array(cellid,radius,hotelid,name,hotelType) =>
        (cellid,hotelType)}}
      .groupByKey()
      .map{x=>
        val hotelType = x._2.toArray.filter{x=> x.equals("none") == false}.mkString("|")
        (x._1,hotelType)
      }

    //hotelInfo.repartition(1).saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/hotelInfo.csv")
    //result.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/cellHotelMap.csv")
    val home = CustomerHome.map{x=> x.split(",") match{
      case Array(user,home,lng,lat) => (home,user)
    }}
    val result = home.leftOuterJoin(cellHotelMap)
      .map{case(home,(user,hotelType)) => (user,hotelType.getOrElse("None"))}
    result
  }


  /**
    * 标签选择：标签+画像
    * @param sc:SparkContext
    * @param pois:RDD[String]
    * @param cellTrans:RDD[String]
    * @param month:String
    * @param CustomerInfo:RDD[String]
    * @param CustomerHome:RDD[String]
    * @param OtherProvTrueUserInfo:RDD[String]
    * @param OtherProvStopPoint:RDD[String]
    * @param LocalTrueUserInfo:RDD[String]
    * @param CDRData:RDD[String]
    * @param Scenic:RDD[String]
    * @return
    */
  def tagsForSelection(sc:SparkContext,month:String,pois:RDD[String],cellTrans:RDD[String],CustomerInfo:RDD[String],CustomerTransport:RDD[String],CustomerHome:RDD[String],OtherProvTrueUserInfo:RDD[String],OtherProvStopPoint:RDD[String],LocalTrueUserInfo:RDD[String],CDRData:RDD[String],Scenic:RDD[String]): RDD[String] = {
    val cus_prov = CustomerInfo.map{x => x.split(",") match {
      case Array(user,prov,firstDate,lastDate,stayDays) => (user,prov)}
    }.repartition(1)
    val cus_trans = CustomerTransport.map{x=> x.split(",") match{
        case Array(user,transport) => (user,transport)}}
    val cus_type = CustomerTypeDetect(LocalTrueUserInfo,CustomerInfo,CDRData)
    val cus_dutyFreeVisit = DutyFreeShopVisit(month,CustomerInfo,OtherProvStopPoint,Scenic)
    val cus_callStrength = CustomerCallStrength(CustomerInfo,CDRData)
    val cus_scenicInfo = customerScenicVisitCount(month,CustomerInfo,OtherProvStopPoint,Scenic)
    val cus_maxdis = maxDisFromHome(month,CustomerInfo,OtherProvStopPoint,CustomerHome)
    val cus_top3LocAtDay = CustomerTop3LocAtDay(month,CustomerInfo,OtherProvStopPoint)
    val cus_homePlace = CustomerHome.map{x=> x.split(",") match {
        case Array(user,cell,lng,lat)=> (user,cell+","+lng+","+lat)
      }}
    val cus_hotel = hotelClassify(sc,pois,cellTrans,CustomerHome)
    val output = cus_prov.join(cus_trans).map{x=> (x._1,x._2._1+","+x._2._2)}.repartition(1)
      .join(cus_type).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(cus_dutyFreeVisit).map{x=> (x._1,x._2._1+","+x._2._2)}.repartition(1)
      .join(cus_callStrength).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(cus_scenicInfo).map{x=> (x._1,x._2._1+","+x._2._2)}.repartition(1)
      .join(cus_maxdis).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(cus_top3LocAtDay).map{x=> (x._1,x._2._1+","+x._2._2)}.repartition(1)
      .join(cus_homePlace).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(cus_hotel)
      .map{x=> x._1+","+x._2._1+","+x._2._2}
      .repartition(1)
    output
  }


}
