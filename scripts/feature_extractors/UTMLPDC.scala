//######################################################################################
// Extract User_Topic_Median_Location_Partition for Social Network + Topic Analysis
// Topics are extracted before using a topic modeling algorithm like LDA
// Topics are in a file containing "Statusid	TopicID" named as sid_topic.txt
//######################################################################################
def extract_UTMLPDC(stats: String, topics: String , partitions: String, mutual_friends: String, output: String) ={
val SUL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(0), (x(1), (x(4).toDouble, x(3).toDouble))))
val ST = sc.textFile(topics).map(_.split("\t")).map(x => (x(0), x(1)))
val UTL = SUL.join(ST).map({case(s,((u,loc), top)) => ((u, top), loc)})
val UTML = UTL.groupByKey().map({case((u, top), locList) => (u,(top, geometric_median(locList.toList)))})
val UP = sc.textFile(partitions).map(_.split(",")).map(x => (x(0),x(1)))
val UD = sc.textFile(mutual_friends).map(x => (x.split(",")(0), 1)).reduceByKey(_+_)
val UMC = UTL.map(x => (x._1._1, 1)).reduceByKey(_+_)
val UTMLPDC = UTML.join(UP).join(UD).join(UMC)
UTMLPDC.map({case(u, ((((top, (lat, lon)), p), deg), mc)) => u + "," + top + "," + lat + "," + lon + "," + p + "," + deg + "," + mc}).saveAsTextFile(output)
}

//######################################################################################
//######################################################################################
def extract_CDF_UTMLPDC(in: String, res: String, minDeg: Int = 0, maxDeg: Int = Int.MaxValue, minMsgCnt: Int = 0, maxMsgCnt: Int = Int.MaxValue) = {
val UTMLPDC = sc.textFile(in).map(_.split(",")).map(x => (x(0), (x(1), (x(2).toDouble, x(3).toDouble), x(4), x(5).toInt, x(6).toInt)))
var ML_filt_deg_cnt = UTMLPDC.filter({case(u, (t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt})
val split = ML_filt_deg_cnt.map({case(u,(top, ml, p, deg, cnt)) => (p,u)}).groupByKey().map(x => (x._1, x._2.toList.distinct)).filter(_._2.size > 4).map({case(p,u) => (p, u.splitAt((u.size * 0.7).toInt))})
val train = split.map({case(p,(tr,ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p,(tr,ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PTML = ML_filt_deg_cnt.join(train).map({case(u,((top, ml, p, deg, cnt),x)) => ((p, top), ml)}).groupByKey().map({case(ptop, mlList) => (ptop, geometric_median(mlList.toList))})
val U_PE = ML_filt_deg_cnt.join(test).map({case(u, ((top, ml, p, deg, cnt),x)) => ((p, top), (u, ml))}).join(PTML).map({case(ptop, ((u, ml), pml)) => ((u, ptop), geoDistance_points(ml, pml))})

val AED = U_PE.map({case(u,e) => (1, (e, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).collect
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)
val Recall = U_PE.count * 1.0/ML_filt_deg_cnt.join(test).count

val pw = new java.io.PrintWriter(new File(res + "_values.txt"))
pw.write("AED\t" + AED + "\n")
pw.write("MED\t" + MED + "\n")
pw.write("REC\t" + Recall + "\n")
pw.close

val temp1 = U_PE.map(x => (Math.floor(x._2 * 10)/10, 1.0)).reduceByKey(_+_)
val temp2 = sc.parallelize(Array(0.0 to 60.0 by 0.1)).flatMap(x => x).map(x => (Math.floor(x*10)/10,0.0))
val temp3 = temp1.union(temp2).reduceByKey(_+_).sortBy(_._1)
temp3.map(x => (0, x)).groupByKey().map(x => CDF(x._2.toList)).flatMap(x => x).map(x => x._2).saveAsTextFile(res)
}