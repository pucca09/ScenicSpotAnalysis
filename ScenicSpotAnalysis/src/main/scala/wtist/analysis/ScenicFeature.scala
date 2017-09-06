package wtist.analysis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.lang.Math._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by chenqingqing on 2016/9/1.
  */
object ScenicFeature {
  def CustomerDistributionDegree(month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[(String,Double)]={
    val provList = Array("301","303","331","311","308","325","305","318",
      "310","328","313","322","319","323","304","315","329","306","302",
      "324","312","321","320","307","316","314","326","317","330","300")
    val scenic = Scenic.map{x=> val line = x.split(",")
      val cell = line(5).trim
      val scenicId = line(0).trim
      (cell,scenicId)}
      .filter{x=> x._2.matches("[0-9]+")}
    val customer = CustomerInfo.map{x=> val user = x.split(",")(0)
      val prov = x.split(",")(1)
      (user,prov)}
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
      .map{case(user,((day,cell),prov)) => (cell,(day,user,prov))}
      .join(scenic)
      .map{case(cell,((day,user,prov),scenicID)) => (scenicID,(day,user,prov))}
      .distinct()
      .groupByKey()
      .map{x=>
        val arr = x._2.toArray
        val totalCount = arr.length.toInt
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
        arrBuff += elseProv
        val distributionDegree = Math.sqrt(arrBuff.toArray.map(_.toDouble).map{x=> (x/totalCount)*(x/totalCount)}.reduce(_+_))*100
        (x._1,distributionDegree)}
      .repartition(1)
    result

  }
  /*
  def NearestShoppingMallDist(sc:SparkContext,month:String,OtherProvStopPoint:RDD[String],CustomerInfo:RDD[String],Scenic:RDD[String]):RDD[(String,Double)]={
    val pois = sc.textFile("/user/tele/Suhang/Pois")
    val shopping = pois.map{_.split("\\|")}.filter{x=> val tags = x(7).split(";")(0);tags.equals("购物")}.map{x=> (x(3),x(2))}.distinct
    val scenic = Scenic.map{x=> val line = x.split(",")
      val cell = line(5).trim
      val scenicId = line(0).trim
      (cell,scenicId)}
      .filter{x=> x._2.matches("[0-9]+")}





  }
  */

}
