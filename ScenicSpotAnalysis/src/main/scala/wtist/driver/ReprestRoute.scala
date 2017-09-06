package wtist.driver

import org.apache.spark.{SparkContext, SparkConf}
import wtist.analysis.CustomerBasicInfo._
import wtist.analysis.RepresentativeRoute._
import wtist.analysis.ScenicSpotAnalysis._

/**
  * Created by chenqingqing on 2016/9/7.
  */
object ReprestRoute {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("LongestPath").set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val rootDir = args(0)
    val output_dir = args(1)
    val province = args(2)
    val month = args(3)
    val LocalStopPoint = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"LocalStop.csv")
    val UserHome = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"Home.csv")
    val UserWork = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"Work.csv")
    val cellTrans = sc.textFile(rootDir+"/BasicInfo/BaseStation/"+province+".csv")
    val longestRT = RepresentativeRtFromHmToWk(sc,cellTrans,LocalStopPoint,UserHome,UserWork)
    longestRT.saveAsTextFile(output_dir)

    sc.stop()
  }

}
