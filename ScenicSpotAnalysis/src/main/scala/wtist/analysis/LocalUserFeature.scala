package wtist.analysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.immutable.Range
import wtist.util.Tools._
/**
  * Created by chenqingqing on 2016/7/19.
  */
object LocalUserFeature {
  /*

  def ExtractFeatures(sc:SparkContext,CDRData:RDD[String],LocalTrueUserInfo:RDD[String],LocalStopPoint:RDD[String],UserHome:RDD[String],UserWork:RDD[String]):RDD[String]= {
    val users = LocalTrueUserInfo.map { x => x.split(",") match {
      case Array(user, time, cell, lng, lat, prov) => (user, 1)
    }
    }
      .distinct()
      .repartition(1)
    val Rec = CDRData.flatMap { x => val slice = x.split(",")
      Seq(((slice(3), slice(4)), (1, slice(5).toInt)), ((slice(4), slice(3)), (1, slice(5).toInt)))
    }
      .filter { x => x._1._1.equals("") == false && x._1._2.equals("") == false }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    val recInfo = Rec.map { x => (x._1._1, x._2) }
    val recUsers = Rec.map { x => x._1 }
    //a1.全部联系人个数
    val totalContactorNum = recUsers.groupByKey()
      .map { x => val arr = x._2.toArray; (x._1, arr.length) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0).toString) }
      .repartition(1)
    //a4&a5.全部联系人通话频次和时长
    val totalContactorFreDur = recInfo.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (user, (freq, dur)) => (user, freq + "," + dur) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0,0")) }
      .repartition(1)

    val callinContactorRec = CDRData.map { x => val slice = x.split(",")
      (slice(4), (slice(3), slice(5)))
    }
      .filter { x => x._1.equals("") == false && x._2._1.equals("") == false }
    //a2.打入联系人个数
    val callinContactorNum = callinContactorRec.map { x => (x._1, x._2._1) }
      .distinct()
      .map { x => (x._1, 1) }
      .reduceByKey(_ + _)
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
      .repartition(1)
    //a6&a7.打入联系人通话频次和时长
    val callinContactorFreqDur = callinContactorRec.map { case (user1, (user2, dur)) =>
      (user1, (1, dur.toDouble))
    }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (user, (freq, dur)) => (user, freq + "," + (dur / 3600000).toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0,0")) }
      .repartition(1)


    val calloutContactorRec = CDRData.map { x => val slice = x.split(",")
      (slice(3), (slice(4), slice(5)))
    }
      .filter { x => x._1.equals("") == false && x._2._1.equals("") == false }
    //a3.打出联系人个数
    val calloutContactorNum = calloutContactorRec.map { x => (x._1, x._2._1) }
      .distinct()
      .map { x => (x._1, 1) }
      .reduceByKey(_ + _)
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
      .repartition(1)

    //a8&a9.打出联系人通话频次和时长
    val calloutContactorFreqDur = calloutContactorRec.map { case (user1, (user2, dur)) =>
      (user1, (1, dur.toDouble))
    }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { case (user, (freq, dur)) => (user, freq + "," + (dur / 3600000).toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0,0")) }
      .repartition(1)
    //用户的工作地点
    val work = UserWork.map { x => x.split(",") match {
      case Array(user, work, lng, lat) => (user, work)
    }
    }

    val colleague = CDRData.flatMap { x => val slice = x.split(",")
      Seq((slice(3), slice(4)), (slice(4), slice(3)))
    }
      .distinct()
      .join(work)
      .map { case (user1, (user2, user1work)) => (user2, (user1, user1work)) }
      .join(work)
      .map { case (user2, ((user1, user1work), user2work)) => (user1, user2, user1work, user2work) }
      .filter { x => x._3.equals(x._4) == true }
      .map { case (user1, user2, user1work, user2work) => (user1, user2) }
      .distinct()
      .repartition(1)
    //a10.同事圈人数
    val colleagueNum = colleague.map { x => (x._1, 1) }
      .reduceByKey(_ + _) //计算用户家人的个数
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
    //a11&a12.同事圈通话频次和时长
    val colleagueFreqDur = colleague.map { x => (x, 1) }
      .join(Rec)
      .map { case ((user1, user2), (1, (freq, dur))) => (user1, (freq, dur)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { x => (x._1, x._2._1 + "," + x._2._2) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0,0")) }
    //用户的家
    val home = UserHome.map { x => x.split(",") match {
      case Array(user, home, lng, lat) => (user, home)
    }
    }
    val family = CDRData.flatMap { x => val slice = x.split(",")
      Seq((slice(3), slice(4)), (slice(4), slice(3)))
    }
      .distinct()
      .join(home)
      .map { case (user1, (user2, user1home)) => (user2, (user1, user1home)) }
      .join(home)
      .map { case (user2, ((user1, user1home), user2home)) => (user1, user2, user1home, user2home) }
      .filter { x => x._3.equals(x._4) == true }
      .map { case (user1, user2, user1home, user2home) => (user1, user2) }
      .distinct() //family就是用户的家人圈
      .repartition(1)
    //a13.家庭圈人数
    val familyNum = family.map { x => (x._1, 1) }
      .reduceByKey(_ + _) //计算用户家人的个数
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
    //a14&a15.家庭圈通话频次和时长
    val familyFreqDur = family.map { x => (x, 1) }
      .join(Rec)
      .map { case ((user1, user2), (1, (freq, dur))) => (user1, (freq, dur)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { x => (x._1, x._2._1 + "," + x._2._2) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0,0")) }

    val friend = recUsers.subtract(colleague)
      .subtract(family)
      .distinct
      .repartition(1)

    //a16.朋友圈人数
    val friendNum = friend.map { x => (x._1, 1) }
      .reduceByKey(_ + _) //计算用户家人的个数
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
    //a17&a18.朋友圈通话频次和时长
    val friendFreqDur = friend.map { x => (x, 1) }
      .join(Rec)
      .map { case ((user1, user2), (1, (freq, dur))) => (user1, (freq, dur)) }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map { x => (x._1, x._2._1 + "," + x._2._2) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0,0")) }
    //a19.聚集系数
    val label = sc.parallelize(1 to users.count.toInt, 1).map(_.toLong)
    val vertices = users.map { x => x._1 }.zip(label)
    val user_KV: RDD[(VertexId, String)] = vertices.map { x => (x._2, x._1) }
    val relationships: RDD[Edge[Int]] = CDRData.flatMap { x => val slice = x.split(",")
      Seq((slice(3), slice(4)), (slice(4), slice(3)))
    }
      .distinct()
      .join(vertices)
      .map { case (user1, (user2, v1)) => (user2, v1) }
      .join(vertices)
      .map { case (user2, (v1, v2)) => (v1, v2) }
      .map { x => Edge(x._1, x._2, 1) }
    val graph: Graph[String, Int] = Graph(user_KV, relationships).partitionBy(PartitionStrategy.RandomVertexCut)
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val triCountByUsername = user_KV.join(triCounts).map { case (vertexid, (md5id, tc)) =>
      (md5id, tc.toString)
    }
    //join features
    val socialFeat = totalContactorNum.join(callinContactorNum).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(calloutContactorNum).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(totalContactorFreDur).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(callinContactorFreqDur).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(calloutContactorFreqDur).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(colleagueNum).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(colleagueFreqDur).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(familyNum).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(familyFreqDur).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(friendNum).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(friendFreqDur).map { x => (x._1, x._2._1 + "," + x._2._2) }
      .join(triCountByUsername).map { x => x._1 + "," + x._2._1 + "," + x._2._2 }

    val weekdaySP = LocalStopPoint.map { x => x.split(",") match {
      case Array(day, user, time, cell, dur, lng, lat) => (user, (day, (time, cell, dur)))
    }
    }
      .filter { x => val day = x._2._1; isWeekday(day) }.cache()

    val weekendSP = LocalStopPoint.map { x => x.split(",") match {
      case Array(day, user, time, cell, dur, lng, lat) => (user, (day, (time, cell, dur)))
    }
    }
      .filter { x => val day = x._2._1; isWeekday(day) == false }.cache()

    //得到每个用户的工作日平均离家最早时间,返回的是离00:00：00的小时数
    val earlistHomeLeftTime = home.join(weekdaySP).map {
      case (user, (hm, (day, (time, cell, dur)))) => (user + "," + day + "," + hm, (time, cell))
    }.groupByKey()
      .map { x =>
        val arr = x._2.toArray.sortWith((a, b) =>
          timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        val day = x._1.split(",")(1)
        val home = x._1.split(",")(2)
        (user, homeLeftTime(arr, home, day)) //user,homeLeftTime
      }.filter { x => x._2 != 0 }
      .groupByKey()
      .map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }


    //得到每个用户的工作日平均离开工作地点最晚时间,返回的是离这一天23:59:59的小时数
    val latesWorkLeftTime = work.join(weekdaySP).map {
      case (user, (wk, (day, (time, cell, dur)))) => (user + "," + day + "," + wk, (time, cell))
    }.groupByKey()
      .map { x =>
        val arr = x._2.toArray.sortWith((a, b) =>
          timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        val day = x._1.split(",")(1)
        val work = x._1.split(",")(2)
        (user, workLeftTime(arr, work, day)) //user,workLeftTime
      }.filter { x => x._2 != 0 }.groupByKey().map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
    //得到每个用户的工作日平均到达工作地点最早时间,返回的是离这一天00:00：00的小时数
    val earlistWorkArrivalTime = work.join(weekdaySP).map {
      case (user, (wk, (day, (time, cell, dur)))) => (user + "," + day + "," + wk, (time, cell))
    }.groupByKey()
      .map { x =>
        val arr = x._2.toArray.sortWith((a, b) =>
          timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        val day = x._1.split(",")(1)
        val work = x._1.split(",")(2)
        (user, workArrivalTime(arr, work, day)) //user,workArrivalTime
      }.filter { x => x._2 != 0 }.groupByKey().map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
    //得到每个用户的工作日平均下班后最早回家时间,返回的是离这一天23:59:59的小时数
    val earlistHomeArrivalTime = home.join(weekdaySP).map {
      case (user, (hm, (day, (time, cell, dur)))) => (user, (day + "," + hm, (time, cell)))
    }.join(latesWorkLeftTime)
      .map { case (user, ((dayAndhm, (time, cell)), latesWT)) => (user + "," + dayAndhm + "," + latesWT, (time, cell)) }
      .groupByKey()
      .map { x =>
        val arr = x._2.toArray.sortWith((a, b) =>
          timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        val day = x._1.split(",")(1)
        val home = x._1.split(",")(2)
        val latesWorkLeftTime = x._1.split(",")(3).toDouble
        (user, homeArrivalTime(arr, home, day, latesWorkLeftTime)) //user,workArrivalTime
      }.groupByKey().map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
    //工作日平均每天工作时长
    val aveWorkDurAtWeekday = work.join(weekdaySP).filter {
      x => val workplace = x._2._1
        val cell = x._2._2._2._2
        workplace.equals(cell)
    }
      .map {
        case (user, (wk, (day, (time, cell, dur)))) => (user + "," + day + "," + wk, dur.toDouble)
      }
      .reduceByKey(_ + _)
      .map { x => val user = x._1.split(",")(0)
        val everyDayDurAtWork = x._2
        (user, everyDayDurAtWork)
      }
      .groupByKey()
      .map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
    //休息日平均每天工作时长
    val aveWorkDurAtWeekend = work.join(weekendSP).filter {
      x => val workplace = x._2._1
        val cell = x._2._2._2._2
        workplace.equals(cell)
    }
      .map {
        case (user, (wk, (day, (time, cell, dur)))) => (user + "," + day + "," + wk, dur.toDouble)
      }
      .reduceByKey(_ + _)
      .map { x => val user = x._1.split(",")(0)
        val everyDayDurAtWork = x._2
        (user, everyDayDurAtWork)
      }
      .groupByKey()
      .map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
    //“工作圈”到达单位时间排名
    // 工作圈到达工作地点时间排名,返回的是在用户的工作圈中比用户来得早的人的人数占工作圈总人数的比例
    val WorkLocArrivalRank = colleague.join(earlistWorkArrivalTime)
      .map{x=> (x._2._1,(x._1,x._2._2))}
      .join(earlistWorkArrivalTime)
      .groupByKey().map{x=>
        val arr = x._2.toArray;var count:Double = 0
        for(i<- 0 until arr.length){
          val time1 = arr(i)._1._2
          val time2 = arr(i)._2
          if( time1 < time2 ){
            count += 1
          }


        }
        (x._1,count/arr.length)

      }
    //“工作圈”工作日平均每天工作小时排名
    // 工作圈工作日在工作地点时长排名,返回的是在用户的工作圈中比用户呆的更久的人的人数占工作圈总人数的比例

    val aveWorkDurAtWeekdayRank = colleague.join(aveWorkDurAtWeekday)
      .map{x=> (x._2._1,(x._1,x._2._2))}
      .join(aveWorkDurAtWeekday)
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray;var count:Double = 0
        for(i<- 0 until arr.length){
          val time1 = arr(i)._1._2
          val time2 = arr(i)._2
          if( time1 > time2 ){
            count += 1
          }


        }
        (x._1,count/arr.length)

      }
    //“工作圈”休息日平均在单位工作时间排名
    //工作圈周末在工作地点时长排名,返回的是在用户的工作圈中比用户呆的更久的人的人数占工作圈总人数的比例
    val aveWorkDurAtWeekendRank = colleague.join(aveWorkDurAtWeekend)
      .map{x=> (x._2._1,(x._1,x._2._2))}
      .join(aveWorkDurAtWeekend)
      .groupByKey().map{x=>
      val arr = x._2.toArray;var count:Double = 0
      for(i<- 0 until arr.length){
        val time1 = arr(i)._1._2
        val time2 = arr(i)._2
        if( time1 > time2 ){
          count += 1
        }


      }
      (x._1,count/arr.length)

    }
    //通勤距离
    //通勤时间

  }
  */

  /**
    * 返回用户每天离家最早的时间
    * 首先找到当日用户首次出现的家的记录，然后计算离家时间，如果离家时间早于4：00，则认为是异常；继续寻找家的位置记录，计算离家时间。重复以上步骤
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Home:String 用户的家的位置
    * @param day:String 当日的日期，201508xx
    * @return Long:当前时间换算得到的小时数
    *
    */
  def homeLeftTime(arr:Array[(String,String)],Home:String,day:String): Double ={
    var i = 0;var flag =false;var home = 0;var leftTime:Long = 0;var result:Double = 0
    while(i<arr.length && !flag ){
      if(arr(i)._2.equals(Home) == true && home == 0){
        home = 1

      }
      if(arr(i)._2.equals(Home) == false && home == 1){
        leftTime = (timetostamp(arr(i)._1).toLong + timetostamp(arr(i-1)._1).toLong)/2
        home = 0
        if (stamptotime(leftTime,"yyyyMMddHHmmss").substring(8,10).toInt >= 4){
          flag = true
        }

      }

      i = i  + 1
    }
    if(flag){
      result = (leftTime - timetostamp(day+"000000").toLong)/3600000.0
    }

    result





  }


  /**
    * 返回用户每天离开工作地点最晚的时间
    * 遍历用户得到当日time_sorted记录中最后一次在工作地点出现的记录，然后计算和后一个记录的中间时间作为离开工作地点的时间
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Work:String 用户的家的位置
    * @return Long:当前时间距离当日235959的小时数
    *
    *
    */
  def workLeftTime(arr:Array[(String,String)],Work:String,day:String): Double ={
    var restore = "";var pointer = 0;var result:Double = 0;var leftTime:Long = 0
    for (i<- 0 until arr.length){
      if(arr(i)._2.equals(Work)){
        restore = arr(i)._1
        pointer = i

      }
    }
    if(restore.equals("") == false){
      if(pointer == (arr.length-1)){
        leftTime = timetostamp(arr(pointer)._1).toLong


      }else{
        leftTime  = (timetostamp(arr(pointer)._1).toLong + timetostamp(arr(pointer+1)._1).toLong)/2

      }
      result =  (timetostamp(day+"235959").toLong - leftTime)/3600000.0


    }

    result

  }
  /**
    * 返回用户每天到达工作地点的最早时间
    * 遍历用户得到当日time_sorted记录，然后返回第一次匹配工作地点的记录
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Work:String 用户的工作地点的位置
    * @return Long:当前时间距离当日00：00:00的小时数
    *
    *
    */
  def workArrivalTime(arr:Array[(String,String)],Work:String,day:String):Double ={
    var i = 0;var flag = false;var restore = "";var result:Double = 0;var arriveTime:Long = 0
    while(i<arr.length && !flag){
      if(arr(i)._2.equals(Work)){
        flag = true
        restore = arr(i)._1

      }
      i = i+1
    }
    if(flag){
      if(i == 1){
        arriveTime = timetostamp(arr(i-1)._1).toLong

      }else{
        arriveTime = (timetostamp(arr(i-1)._1).toLong + timetostamp(arr(i-2)._1).toLong)/2

      }
      result = (arriveTime - timetostamp(day+"000000").toLong)/3600000.0


    }
    result




  }
  /**
    * 返回用户每天回家的最早时间，
    * 遍历用户得到当日time_sorted记录，从用户最晚从工作地点离开的时间以后找到的最早的家的记录和上一个位置取时间中间值
    *
    * @param arr:Array[(String,String)] = time,cellId
    * @param Home:String 用户的家的位置
    * @param day:String
    * @param latesWorkLeftTime:Double 由先前计算得到的
    * @return Long:当前时间距离当日23：59:59的小时数
    *
    *
    */
  def homeArrivalTime(arr:Array[(String,String)],Home:String,day:String,latesWorkLeftTime:Double):Double ={
    var i = 0;var flag = false;var baseline  = timetostamp(day+"235959").toLong;var time :Double= 0;

    while( i< arr.length && !flag){
      if(arr(i)._2.equals(Home)){
        time = (baseline -timetostamp(arr(i)._1).toLong)/3600000.0
        if(time < latesWorkLeftTime){
          flag = true
        }

      }
      i += 1


    }
    if(!flag){time = 0}
    time

  }

  def temp(CDRData:RDD[String],Scenic:RDD[String],LocalTrueUserInfo:RDD[String],LocalStopPoint:RDD[String],UserHome:RDD[String],UserWork:RDD[String]):RDD[String]={
    //社交标签
    //全部联系人个数
    val users = LocalTrueUserInfo.map { x => x.split(",") match {
      case Array(user, time, cell, lng, lat, prov) => (user, 1)}}
      .distinct()
      .cache()
    val Rec = CDRData.flatMap { x => val slice = x.split(",")
      Seq(((slice(3), slice(4)), (1, slice(5).toInt)), ((slice(4), slice(3)), (1, slice(5).toInt)))}

    //a1.全部联系人个数
    val totalContactorNum = CDRData.flatMap{ x => val slice = x.split(",")
      Seq((slice(3), slice(4)), (slice(4), slice(3)))}
      .filter{x=> x._1.equals("") == false}
      .groupByKey()
      .map { x => val arr = x._2.toArray; (x._1, arr.length) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0).toString) }
      .repartition(1)
    //a2.打入联系人个数
    val callinContactorRec = CDRData.map { x => val slice = x.split(",")
      (slice(4), (slice(3), slice(5)))
    }
      .filter { x => x._1.equals("") == false && x._2._1.equals("") == false }
    val callinContactorNum = callinContactorRec.map { x => (x._1, x._2._1) }
      .distinct()
      .map { x => (x._1, 1) }
      .reduceByKey(_ + _)
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
      .repartition(1)
    //a3.打出联系人个数
    val calloutContactorRec = CDRData.map { x => val slice = x.split(",")
      (slice(3), (slice(4), slice(5)))
    }
      .filter { x => x._1.equals("") == false && x._2._1.equals("") == false }

    val calloutContactorNum = calloutContactorRec.map { x => (x._1, x._2._1) }
      .distinct()
      .map { x => (x._1, 1) }
      .reduceByKey(_ + _)
      .map { x => (x._1, x._2.toString) }
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse("0")) }
      .repartition(1)
    //工作类标签
    val weekdaySP = LocalStopPoint.map{x => x.split(",") match{
      case Array(day, user, time, cell, dur, lng, lat) => (user, (day, (time, cell, dur)))}}
      .filter{ x => val day = x._2._1; isWeekday(day)}
      .cache()

    val home = UserHome.map { x => x.split(",") match {
      case Array(user, home, lng, lat) => (user, home, lng, lat)}}
      .filter{x=> x._3.equals("None") == false && x._4.equals("None") == false}
      .map{case (user, home, lng, lat) => (user,home)}
    val work = UserWork.map { x => x.split(",") match {
      case Array(user, work, lng, lat) => (user, work, lng, lat)}}
      .filter{x=> x._3.equals("None") == false && x._4.equals("None") == false}
      .map{case (user, work, lng, lat) => (user,work)}
    //得到每个用户的工作日平均离家最早时间,返回的是离00:00：00的小时数
    val earlistHomeLeftTime = home.join(weekdaySP).map {
      case (user, (hm, (day, (time, cell, dur)))) => (user + "," + day + "," + hm, (time, cell))
    }.groupByKey()
      .map { x =>
        val arr = x._2.toArray.sortWith((a, b) =>
          timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        val day = x._1.split(",")(1)
        val home = x._1.split(",")(2)
        (user, homeLeftTime(arr, home, day)) //user,homeLeftTime
      }.filter { x => x._2 != 0 }.groupByKey().map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
      .repartition(1)

    //得到每个用户的工作日平均离开工作地点最晚时间,返回的是离这一天23:59:59的小时数
    val latesWorkLeftTime = work.join(weekdaySP).map {
      case (user, (wk, (day, (time, cell, dur)))) => (user + "," + day + "," + wk, (time, cell))
    }.groupByKey()
      .map { x =>
        val arr = x._2.toArray.sortWith((a, b) =>
          timetostamp(a._1).toLong < timetostamp(b._1).toLong)
        val user = x._1.split(",")(0)
        val day = x._1.split(",")(1)
        val work = x._1.split(",")(2)
        (user, workLeftTime(arr, work, day)) //user,workLeftTime
      }.filter { x => x._2 != 0 }.groupByKey().map { x => (x._1, x._2.reduce(_ + _) / x._2.toArray.length) }
      .repartition(1)
    //生活圈半径
    val homeWithGps = UserHome.map { x => x.split(",") match {
      case Array(user, home, lng, lat) => (user, home, lng, lat)}
    }.filter{x=> x._3.equals("None") == false && x._4.equals("None") == false}
      .map{case (user, home, lng, lat) => (user,(lng,lat))}
    val workWithGps = UserWork.map { x => x.split(",") match {
      case Array(user, work, lng, lat) => (user, work, lng, lat)}
    }.filter{x=> x._3.equals("None") == false && x._4.equals("None") == false}
      .map{case (user, work, lng, lat) => (user,(lng,lat))}

    val lifeCircle = LocalStopPoint.map { x => x.split(",") match {
      case Array(day, user, time, cell, dur, lng, lat) => (user,(day,cell,dur,lng,lat))}}
      .filter { x => val day = x._2._1;val dur = x._2._3
        isWeekday(day) && dur.toDouble >= 1}.cache()
      .join(work)
      .filter{x=> x._2._1._2.equals(x._2._2) == false}
      .map{case (user,((day,cell,dur,lng,lat),wk)) => (user,(day,(lng,lat)))}
      .join(homeWithGps)
      .map{x=> val distance = GetDistance(x._2._1._2,x._2._2);val day = x._2._1._1;val user = x._1
        (day+","+user,distance)}
      .filter{x=> x._2 < 100000.0}
      .groupByKey().map{x=>
      val arr = x._2.toArray.sortWith((a,b) => a > b )
      val user = x._1.split(",")(1)
      (user,arr(0))
    }
      .groupByKey()
      .map{x=> val ave = x._2.reduce(_+_)/x._2.toArray.length;(x._1,ave)}
      .repartition(1)

    //出游记录
    val weekendSPwithoutWork = LocalStopPoint.map{ x => x.split(",") match {
      case Array(day, user, time, cell, dur, lng, lat) => (user,(day,cell,dur,lng,lat))
    }
    }
      .filter { x => val day = x._2._1;val dur = x._2._3
        isWeekday(day) == false && dur.toDouble >= 6}
      .join(work)
      .filter{x=> x._2._1._2.equals(x._2._2) == false}
      .map{case (user,((day,cell,dur,lng,lat),wk)) => (user,(dur,(lng,lat)))}
      .join(homeWithGps)
      .map{x=> val distance = GetDistance(x._2._1._2,x._2._2)
        val user = x._1
        val dur = x._2._1._1
        (user,(dur,distance))}
      .filter{x=> val dist = x._2._2; dist >= 10}
      .repartition(1)

    val travelCount = weekendSPwithoutWork.groupByKey().map{x=> (x._1,x._2.toArray.length)}
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0)) }

    val travelDur = weekendSPwithoutWork.map{case (user,(dur,distance)) =>(user,dur.toDouble)}
      .groupByKey().map{x=> (x._1,x._2.reduce(_+_))}
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0)) }

    //游览景点个数
    val scenic = Scenic.map{x=> val line = x.split(",")
      val cell = line(5).trim
      val scenicId = line(0).trim
      (cell,scenicId)}
      .filter{x=> x._2.matches("[0-9]+")}
    val scenicSP = LocalStopPoint.map{ x => x.split(",") match {
      case Array(day, user, time, cell, dur, lng, lat) => (cell,(user,dur))
    }
    }
      .join(scenic)
      .map{case (cell,((user,dur),scenicId)) => (user,scenicId,dur)}
      .repartition(1)
    val scenicCount = scenicSP.map{case(user,scenicId,dur) => (user,scenicId)}
      .distinct
      .map{x=> (x._1,1)}
      .reduceByKey(_+_)
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0)) }
    val scenicDur = scenicSP.map{case(user,scenicId,dur) => (user,dur.toDouble)}
      .reduceByKey(_+_)
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0)) }
    //通勤距离
    val commuteDist = homeWithGps.join(workWithGps)
      .map{x=> val dist = GetDistance(x._2._1,x._2._2);(x._1,dist)}
      .rightOuterJoin(users)
      .map { x => (x._1, x._2._1.getOrElse(0)) }
      .repartition(1)

    val result = totalContactorNum.join(callinContactorNum).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(calloutContactorNum).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(earlistHomeLeftTime).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(latesWorkLeftTime).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(lifeCircle).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(travelCount).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(travelDur).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(scenicCount).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(scenicDur).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(commuteDist).map{x=> (x._1,x._2._1+","+x._2._2)}
      .join(homeWithGps).map{x=> (x._1,x._2._1+","+x._2._2._1+","+x._2._2._2)}
      .join(workWithGps).map{x=> x._1+","+x._2._1+","+x._2._2._1+","+x._2._2._2}
    result


  }





}
