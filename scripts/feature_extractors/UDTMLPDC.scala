
//######################################################################################
// Extract User_Median_Location_Partition for Social Network + Time Analysis
// Day => {WE, WD} = {0, 1}
// Hour => {0-7, 8-18, 19-23} = {0, 1, 2}
// Hour_3H => H/3 = {0, 1, 2, 3, 4, 5, 6, 7, 8}
// Hour_1H => H = {0, ..., 23}
// timeSpan = {"N","3H","1H"}
// daySpan = {"DAILY", Default = "WE/WD"}
//######################################################################################
def extract_UDTMLPDC(stats: String, partitions: String, mutual_friends: String, output: String, daySpan: String = "WE/WD", timeSpan: String = "N") = {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val UDL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(1),(dateparser.parse(x(2)), (x(4).toDouble, x(3).toDouble))))
var UDTL = UDL.map({case(u, (d, l)) => (u, (d.getDay, d.getHours, l))}).map({case(u,(d,t,l)) => (u, (if(daySpan == "DAILY") d else if(d % 6 > 0) 1 else 0, if(t - 2 < 6) 0 else if (t - 7 < 12) 1 else 2, l))})
if(timeSpan == "3H"){
UDTL = UDL.map({case(u,(d,l)) => (u, (d.getDay, d.getHours, l))}).map({case(u,(d,t,l)) => (u, (if(daySpan == "DAILY") d else if(d % 6 > 0) 1 else 0, t/3 , l))})
}else if(timeSpan == "1H"){
UDTL = UDL.map({case(u,(d,l)) => (u, (d.getDay, d.getHours, l))}).map({case(u,(d,t,l)) => (u, (if(daySpan == "DAILY") d else if(d % 6 > 0) 1 else 0, t , l))})
}

val UDTML = UDTL.map({case(u,(d, t, l)) => ((u, d, t), l)}).groupByKey().map({case((u, d, t),ls) => (u,(d, t, geometric_median(ls.toList)))})
val UP = sc.textFile(partitions).map(_.split(",")).map(x => (x(0),x(1)))
val UD = sc.textFile(mutual_friends).map(x => (x.split(",")(0), 1)).reduceByKey(_+_)
val UMC = UDL.map(x => (x._1, 1)).reduceByKey(_+_)
val UDTMLPDC = UDTML.join(UP).join(UD).join(UMC)
UDTMLPDC.map({case(u, ((((d, t, (lat, lon)), p), deg),mc)) => u + "," + d + "," + t + "," + lat + "," + lon + "," + p + "," + deg + "," +mc}).saveAsTextFile(output + "_" + daySpan + "_" + timeSpan)
}

//######################################################################################
// Social Netork + Time
// extract_CDF_UDTMLP
// Comulative Density of sorted error in KM
// day = {0="WE", 1="WD"}
// time = {H, W, L = 0, 1, 2} | {3H = 0, 1, ..., 8} | {1H = 0, 1, ..., 23}
//######################################################################################
def extract_CDF_UDTMLPDC(in: String, res: String, minDeg: Int = 0, maxDeg: Int = Int.MaxValue, minMsgCnt: Int = 0, maxMsgCnt: Int = Int.MaxValue, day: Int = -1, time: Int = -1, pid: Int, minParSize: Int, min_par_msg_cnt: Int, max_par_msg_cnt: Int) = {
val ML = sc.textFile(in).map(_.split(",")).map(x => (x(0), (x(1), x(2), (x(3).toDouble,x(4).toDouble), x(5), x(6).toInt, x(7).toInt)))
var ML_filt_deg_cnt = ML.filter({case(u, (d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt})
if(day != -1)
 if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day && t.toInt == time})
 else
  ML_filt_deg_cnt = ML.filter({case(u, (d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day})
else if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && t.toInt == time})

val temp = ML_filt_deg_cnt.map({case(u, (d, t, ml, p, deg, cnt)) => (p, (d, t, ml, u))})
val valid_partitions = extract_valid_partitions("partitions/partitions_inf.txt", "stats.txt", min_par_msg_cnt, max_par_msg_cnt, day, time, minParSize)
var UDTMLP = temp.join(valid_partitions).map({case(p, ((d, t, ml, u), x)) => (u, (d, t, ml, p))})
//var UDTMLP = ML_filt_deg_cnt.map({case(u, (d, t, ml, p, deg, cnt)) => (u, (d, t, ml, p))})
if(pid != -1)
 UDTMLP = ML_filt_deg_cnt.map({case(u, (d, t, ml, p, deg, cnt)) => (u, (d, t, ml, p))}).filter({case (u, (d, t, ml, p)) => p.toInt == pid })

val split = UDTMLP.map({case(u,(d,t, ml,p)) => (p,u)}).groupByKey().map(x => (x._1, x._2.toList.distinct)).filter(_._2.size > minParSize).map({case(p,u) => (p, u.splitAt((u.size * 0.7).toInt))})
val train = split.map({case(p,(tr,ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p,(tr,ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PDTML = UDTMLP.join(train).map({case(u,((d, t, ml, p),x)) => ((p, d, t), ml)}).groupByKey().map({case(pdt, ls) => (pdt, geometric_median(ls.toList))})
val U_PE = UDTMLP.join(test).map({case(u, ((d, t, ml, p),x)) => ((p, d, t), (u, ml))}).join(PDTML).map({case(pdt, ((u, ml), pml)) => ((u,pdt), geoDistance_points(ml, pml))})
val AED = U_PE.map({case(u,e) => (1, (e, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).first.toDouble
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)(0).toDouble
val REC_DT = UDTMLP.groupByKey().count * 1.0 / ML_filt_deg_cnt.groupByKey().count
val REC_ALL = UDTMLP.groupByKey().count * 1.0 / ML.groupByKey().count

val pw = new java.io.PrintWriter(new java.io.File(res + "_values.txt"))
pw.write("AED\t" + AED + "\n")
pw.write("MED\t" + MED + "\n")
pw.write("RECDT\t" + REC_DT + "\n")
pw.write("RECALL\t" + REC_ALL + "\n")
pw.close

val temp1 = U_PE.map(x => (Math.floor(x._2 * 10)/10, 1.0)).reduceByKey(_+_)
val temp2 = sc.parallelize(Array(0.0 to 60.0 by 0.1)).flatMap(x => x).map(x => (Math.floor(x*10)/10,0.0))
val temp3 = temp1.union(temp2).reduceByKey(_+_).sortBy(_._1)
temp3.map(x => (0, x)).groupByKey().map(x => CDF(x._2.toList)).flatMap(x => x).map(x => x._2).saveAsTextFile(res)
}

//######################################################################################
// Extract and returns MED for combination of all features
//######################################################################################
def extract_MED_UDTMLPDC(in: String, minDeg: Int = 0, maxDeg: Int = Int.MaxValue, minMsgCnt: Int = 0, maxMsgCnt: Int = Int.MaxValue, day: Int = -1, time: Int = -1, pid: Int, minParSize: Int): (Double, Double) = {
val ML = sc.textFile(in).map(_.split(",")).map(x => (x(0), (x(1), x(2), x(3), (x(4).toDouble, x(5).toDouble), x(6), x(7).toInt, x(8).toInt)))
var ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt})
if(day != -1)
 if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day && t.toInt == time})
 else
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && d.toInt == day})
else if(time != -1)
  ML_filt_deg_cnt = ML.filter({case(u, (top, d, t, ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt && t.toInt == time})

var UTDTMLP = ML_filt_deg_cnt.map({case(u, (top, d, t, ml, p, deg, cnt)) => (u, (top, d, t, ml, p))})
if(pid != -1)
 UTDTMLP = ML_filt_deg_cnt.map({case(u, (top, d, t, ml, p, deg, cnt)) => (u, (top, d, t, ml, p))}).filter({case (u, (top, d, t, ml, p)) => p.toInt == pid})

val split = UTDTMLP.map({case(u, (top, d, t, ml, p)) => (p, u)}).groupByKey().map(x => (x._1, x._2.toList.distinct)).filter(_._2.size > minParSize).map({case(p, ul) => (p, ul.splitAt((ul.size * 0.7).toInt))})
val train = split.map({case(p, (tr, ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p, (tr, ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PTDTML = UTDTMLP.join(train).map({case(u, ((top, d, t, ml, p), x)) => ((p, top, d, t), ml)}).groupByKey().map({case(ptdt, ls) => (ptdt, geometric_median(ls.toList))})
val U_PE = UTDTMLP.join(test).map({case(u, ((top, d, t, ml, p), x)) => ((p, top, d, t), (u, ml))}).join(PTDTML).map({case(ptdt, ((u, ml), pml)) => ((u, ptdt), geoDistance_points(ml, pml))})
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)(0).toDouble
val Recall = (U_PE.count * 1.0/UTDTMLP.join(test).count).toDouble
return (MED, Recall)
}


