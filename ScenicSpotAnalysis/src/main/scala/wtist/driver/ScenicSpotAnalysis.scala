package wtist.driver

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import wtist.analysis.CustomerBasicInfo._
import wtist.analysis.RepresentativeRoute._
import wtist.analysis.ScenicSpotAnalysis._

/**
  * Created by chenqingqing on 2016/7/3.
  */
object ScenicSpotAnalysis {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("ScenicSpotAnalysis").set("spark.eventLog.enabled","false").set("spark.yarn.executor.memoryOverhead","4096")
    val sc = new SparkContext(conf)
    val rootDir = args(0)
    val output_dir = args(1)
    val province = args(2)
    val month = args(3)
    val OtherProvTrueUserInfo = sc.textFile(rootDir+"/DataClean/tmp/"+province+"/"+month+"/"+month+"TotalOther.csv")
    val OtherProvStopPoint = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"OtherStop.csv")
    val LocalTrueUserInfo = sc.textFile(rootDir+"/DataClean/tmp/"+province+"/"+month+"/"+month+"TrueLocal.csv")
    val LocalStopPoint = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"LocalStop.csv")
    val UserHome = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"Home.csv")
    val UserWork = sc.textFile(rootDir+"/Extraction/"+province+"/"+month+"/"+month+"Work.csv")
    val CDRData = sc.textFile(rootDir+"/SplitData/"+province+"/"+month+"/"+month+"CC.csv")
    val Pois = sc.textFile(rootDir+"/BasicInfo/POI/"+province+"/"+province+".csv")
    val Scenic = sc.textFile(rootDir+"/BasicInfo/ScenicSpot/"+province+".csv")
    val cellTrans = sc.textFile(rootDir+"/BasicInfo/BaseStation/"+province+".csv")
    val CustomerInfo  = customerDetect(month,OtherProvTrueUserInfo)
    CustomerInfo.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/CustomerBasicInfo/customerStayDaysCount.csv")
    val CustomerTransport = transportationOut(OtherProvStopPoint,CustomerInfo,cellTrans)
    CustomerTransport.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/CustomerBasicInfo/customerTransMode.csv")
    val CustomerHome = homePlaceForCustomer(month,OtherProvStopPoint,CustomerInfo,cellTrans)
    CustomerHome.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/CustomerBasicInfo/customerHome.csv")
    val hotRouteShort = HotTravelRoute(sc,month,3,5,OtherProvStopPoint,CustomerInfo,Scenic)
    hotRouteShort.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/HotRouteForShort.csv")
    val hotRouteLong = HotTravelRoute(sc,month,6,10,OtherProvStopPoint,CustomerInfo,Scenic)
    hotRouteLong.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/HotRouteForLong.csv")
    val hotRouteDeep = HotTravelRoute(sc,month,11,15,OtherProvStopPoint,CustomerInfo,Scenic)
    hotRouteDeep.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/HotRouteForDeep.csv")
    val customerFlowByProvByMonth = customerFlowCount(sc,month,OtherProvTrueUserInfo,CustomerInfo)
    customerFlowByProvByMonth.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/customerFlowByProvByMonth.csv")
    val customerHomePlace = customerHomePlaceCount(CustomerHome)
    customerHomePlace.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/customerHomePlaceCount.csv")
    val customerInOutDateCount = everyDayInAndOutCustomerCount(sc,month,CustomerInfo)
    customerInOutDateCount.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/customerInOutDateCount.csv")
    val cusStayDaysCount = customerStayDaysCount(sc,CustomerInfo)
    cusStayDaysCount.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/customerStayDaysCount.csv")
    val migrationBetweenScenicSpotTotalWithDate = mergeMigrationDataByDay(OtherProvStopPoint,CustomerInfo,Scenic)
    migrationBetweenScenicSpotTotalWithDate.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/migrationBetweenScenicSpotTotalWithDate.csv")
    val migrationBetweenScenicSpotTotal = mergeMigrationDataByMonth(migrationBetweenScenicSpotTotalWithDate)
    migrationBetweenScenicSpotTotal.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/migrationBetweenScenicSpotTotal.csv")
    val provTransMode = provinceByTransMode(CustomerTransport,CustomerInfo)
    provTransMode.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/provinceByTransMode.csv")
    val scenicCusFlowByDay = scenicCustomerFlowByDay(month,OtherProvStopPoint,CustomerInfo,Scenic)
    scenicCusFlowByDay.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicCustomerFlowByDay.csv")
    val scenicCusFlowByMonth = scenicCustomerFlowByMonth(month,OtherProvStopPoint,CustomerInfo,Scenic)
    scenicCusFlowByMonth.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicCustomerFlowByMonth.csv")
    val scenicCusFlowByProvByDay = scenicCustomerFlowByProvByDay(month,OtherProvStopPoint,CustomerInfo,Scenic)
    scenicCusFlowByProvByDay.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicCustomerFlowByProvByDay.csv")
    val scenicCusFlowByProvByMonth = scenicCustomerFlowByProvByMonth(month,OtherProvStopPoint,CustomerInfo,Scenic)
    scenicCusFlowByProvByMonth.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicCustomerFlowByProvByMonth.csv")
    val scenicSpotAveDur = scenicSpotAveStayDur(month,OtherProvStopPoint,CustomerInfo,Scenic)
    scenicSpotAveDur.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicSpotAveStayDur.csv")
    val scenicSpotDurCount = scenicSpotStayDurCount(month,OtherProvStopPoint,CustomerInfo,Scenic)
    scenicSpotDurCount.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicSpotStayDurCount.csv")
    val TagsForSelection = tagsForSelection(sc,month,Pois,cellTrans,CustomerInfo,CustomerTransport,CustomerHome,OtherProvTrueUserInfo,OtherProvStopPoint,LocalTrueUserInfo,CDRData,Scenic)
    TagsForSelection.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/tagsForSelection.csv")
    val scenicSpotRank = scenicRank(Scenic,scenicCusFlowByMonth,scenicSpotAveDur)
    scenicSpotRank.saveAsTextFile(rootDir+"/BackEnd/ScenicSpotAnalysis/"+province+"/"+month+"/ForeEndVisData/scenicSpotRankMetric.csv")
    sc.stop()
  }

}
