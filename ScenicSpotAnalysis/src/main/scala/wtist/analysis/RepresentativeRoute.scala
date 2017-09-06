package wtist.analysis
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import wtist.util.Tools._
/**
  * Created by chenqingqing on 2016/8/27.
  */
object RepresentativeRoute {
  type Path[Key] = (Double, List[Key])
  //只保留最大路径
  def Dijkstra[Key](lookup: Map[Key, List[(Double, Key)]], fringe: List[Path[Key]], dest: Key, visited: Set[Key]): Path[Key] = fringe match {
    case (dist, path) :: fringe_rest => path match {case current_node :: path_rest =>
      if (current_node == dest) (dist, path.reverse)
      else {
        val paths = lookup(current_node).flatMap {case (d, key) => if (!visited.contains(key)) List((dist + d, key :: path)) else Nil}
        val sorted_fringe = (paths ++ fringe_rest).sortWith {case ((d1, _), (d2, _)) => d1 > d2}
        Dijkstra(lookup, sorted_fringe, dest, visited + current_node)
      }
    }
    case Nil => (0, List())
  }

  def TrajectoriesMap(trajectories:Array[Array[String]],end:String):Map[String, List[(Double, String)]]={
    var temp = new ArrayBuffer[String]()
    for (arr <- trajectories){
      for (i <- 0 until arr.length-1){
        val pair = arr(i)+":"+arr(i+1)
        temp += pair
      }
    }
    val route = temp.groupBy(x=>x).mapValues(_.size.toDouble).toArray.map{x=>
      val pre = x._1.split(":")(0)
      val next = x._1.split(":")(1)
      val count = x._2
      (pre,(count,next))}
    val route_map = Map[String,ListBuffer[(Double, String)]]()
    for (i <- route){
      if (route_map.getOrElse(i._1,-1) != -1){
        route_map(i._1) += Tuple2(i._2._1,i._2._2)
      }else{
        route_map(i._1) = ListBuffer(i._2)
      }
    }
    val result = route_map.map{case (k,v) => (k,v.toList)}
    result += (end -> Nil)
    result
  }

  def EveryDayTraj(arr:Array[(String,String)],hm:String,wk:String):Array[String]={
    val resultBuffer = new ArrayBuffer[String]()
    var flag = 0;var i =0
    while(i < arr.length && flag != 2){
      val cell = arr(i)._2
      if ( cell.equals(hm)){
          if (flag == 0){
            flag = 1
            resultBuffer += cell

          }
          i += 1
      }
      else if ( cell.equals(wk)){
          if (flag == 1){
            flag = 2
            resultBuffer += cell
          }
          i += 1
        }
      else {
        if( flag == 1){
          resultBuffer += cell
      }
        i += 1


      }

    }
    if (flag != 2){
      resultBuffer.clear()
    }
    resultBuffer.distinct.toArray

  }
  def RepresentativeRtFromHmToWk(sc:SparkContext,CellInfo:RDD[String],LocalStopPoint:RDD[String],UserHome:RDD[String],UserWork:RDD[String]):RDD[String]={
    //抽样
    //    val sampleUsers = LocalStopPoint.map{x=> x.split(",") match{
    //      case Array(day,user,time,cell,dur,lng,lat) => user
    //    }}.distinct()
    val CellMap = CellInfo.map(_.split(","))
      .filter{x=> x.length ==14}
      .map{x=> val cell = x(3)
        val bdLng = x(12)
        val bdLat = x(13)
        (cell,bdLng+","+bdLat)}
      .distinct()
      .collectAsMap()


    val home = UserHome.map { x => x.split(",") match {
      case Array(user, home, lng, lat) => (user, home)
    }}
    val work = UserWork.map { x => x.split(",") match {
      case Array(user, work, lng, lat) => (user, work)
    }}
    val longestPath = LocalStopPoint.map{x=> x.split(",") match{
      case Array(day,user,time,cell,dur,lng,lat) => (user,(day,time,cell))
    }}.join(home)
      .map{case(user,((day,time,cell),home)) => (user,(day,time,cell,home))}
      .join(work)
      .filter{x=> x._2._1._4.equals(x._2._2) == false}//只选取家和工作地点不同的人
      .map{case (user,((day,time,cell,home),work)) => (user+","+day+","+home+","+work,(time,cell))}
      .groupByKey()
      .map{x=> val user = x._1.split(",")(0)
        val home = x._1.split(",")(2)
        val work = x._1.split(",")(3)
        val arr = x._2.toArray.sortWith((a,b) => timetostamp(a._1) < timetostamp(b._1))
        (user+","+home+","+work,EveryDayTraj(arr,home,work))}
      .filter{x=> val len = x._2.length; len != 0 && len != 1 && len != 2}
      .groupByKey()
      .map{x=> val arr = x._2.toArray
        val user = x._1.split(",")(0)
        val start = x._1.split(",")(1)
        val end = x._1.split(",")(2)
        val start_path = start +: Nil
        val start_fringe = (0.0,start_path)+:Nil
        val path = Dijkstra[String](TrajectoriesMap(arr,end),start_fringe, end, Set())
        val pathCordination = new ArrayBuffer[String]()
        for (i <- path._2){
          pathCordination += CellMap.getOrElse(i,"None,None")
        }
        val home = CellMap.getOrElse(start,"None,None")
        val work = CellMap.getOrElse(end,"None,None")
        user+","+home+","+work+","+pathCordination.toArray.mkString(",")
      }
    longestPath




  }

}
