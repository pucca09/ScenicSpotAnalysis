package wtist.analysis
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by chenqingqing on 2016/9/11.
  */
object SocialRelated {
  def socialCircleCall(CDRData:RDD[String],UserHome:RDD[String],UserWork:RDD[String]):RDD[String]={
    val Rec = CDRData.flatMap{x=> val slice = x.split(",")
      Seq(((slice(3),slice(4)),(1,slice(5).toInt)),((slice(4),slice(3)),(1,slice(5).toInt)))
    }.filter{x=> x._1._1.equals("") == false && x._1._2.equals("") == false}.reduceByKey((a,b)=>(a._1+b._1,a._2+b._2))
      .map{case ((u1,u2),(freq,dur)) => ((u1,u2),(freq,dur.toDouble/60000))}
    val recInfo = Rec.map{x=> (x._1._1,x._2)}
    val recUsers = Rec.map{x=> x._1}
    val home = UserHome.map{x=>
      val line = x.split(",")
      (line(0),line(1))}
      .distinct()//得到用户的家,(user,home)
    val family0 = recUsers.join(home)
        .map{x=> (x._2._1,(x._1,x._2._2))}
        .join(home)
        .map{x=> (x._1,x._2._1._1,x._2._2,x._2._1._2)}
        .filter{x=> x._3.equals(x._4) == true}
        .map{x=> ((x._1,x._2),1)}
        .distinct
    val family = family0.join(Rec)
      .map{case ((u1,u2),(1,(freq,dur))) => "0,"+freq+","+dur}
    val work = UserWork.map{x=>
      val line = x.split(",")
      (line(0),line(1))}
      .distinct()//得到用户的家,(user,workplace)
    val colleague0 = recUsers.join(work)
        .map{case(user1,(user2,workplace1)) =>
          (user2,(user1,workplace1))}
        .join(work)
        .map{case(user2,((user1,workplace1),workplace2)) =>
          (user1,user2,workplace1,workplace2)}
        .filter{x=> x._3.equals(x._4) == true}
        .map{x=> ((x._1,x._2),1)}
        .distinct()
    val colleague = colleague0.join(Rec)
      .map{case ((u1,u2),(1,(freq,dur))) => "1,"+freq+","+dur}
    val friend = recUsers.map{(_,1)}.subtract(family0).subtract(colleague0).join(Rec)
      .map{case ((u1,u2),(1,(freq,dur))) => "2,"+freq+","+dur}
    val total = family.union(colleague).union(friend)
    total
    //val df = total.map{x=> x.split(",");(line(0),line(1).toInt,line(2).toDouble)}.toDF("type","freq","dur")

  }


}
