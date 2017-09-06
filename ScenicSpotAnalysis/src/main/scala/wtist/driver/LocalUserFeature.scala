package wtist.driver

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import wtist.analysis.LocalUserFeature._

/**
  * Created by chenqingqing on 2016/9/7.
  */
object LocalUserFeature {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("LocalUserFeature").set("spark.driver.maxResultSize","2g").set("spark.akka.frameSize0","100").set("spark.yarn.executor.memoryOverhead","4096")
    val sc = new SparkContext(conf)
    val rootDir = args(0)
    val output_dir = args(1)
    val province = args(2)
    val month = args(3)
    val OtherProvTrueUserInfo = sc.textFile(rootDir+"/DataClean/"+province+"/"+month+"/"+month+"Other/"+month+"TrueOther.csv")
    val OtherProvStopPoint = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"OtherStop.csv")
    val LocalTrueUserInfo = sc.textFile(rootDir+"/DataClean/"+province+"/"+month+"/"+month+"Local/"+month+"TrueLocal.csv")
    val LocalStopPoint = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"LocalStop.csv")
    val UserHome = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"Home.csv")
    val UserWork = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"Work.csv")
    val CDRData = sc.textFile(rootDir+"/SplitData/"+province+"/"+month+"/"+month+"CC.csv").persist()
    val Scenic = sc.textFile(rootDir+"/BasicInfo/ScenicSpot/"+province+".csv")
    val cellTrans = sc.textFile(rootDir+"/BasicInfo/BaseStation/"+province+".csv")
    val result = temp(CDRData,Scenic,LocalTrueUserInfo,LocalStopPoint,UserHome,UserWork)
    result.saveAsTextFile(output_dir)
    sc.stop()
  }

}
