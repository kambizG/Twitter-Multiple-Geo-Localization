
//######################################################################################
// Extract User_Median_Location_Partition_Degree_MessageCount for Social Network Analysis
//######################################################################################
def extract_UMLPDC(stats: String, partitions: String, mutual_friends: String, output: String) ={
val UL = sc.textFile(stats).map(_.split(",",7)).map(x => (x(1),(x(4).toDouble, x(3).toDouble)))
val UML = UL.groupByKey().map({case(u,ls) => (u, geometric_median(ls.toList))})
val UP = sc.textFile(partitions).map(_.split(",")).map(x => (x(0),x(1)))
val UD = sc.textFile(mutual_friends).map(x => (x.split(",")(0), 1)).reduceByKey(_+_)
val UMC = UL.map(x => (x._1, 1)).reduceByKey(_+_)
val UMLPDC = UML.join(UP).join(UD).join(UMC)
UMLPDC.map({case(u, ((((lat, lon), p), deg), mc)) => u + "," + lat + "," + lon + "," + p + "," + deg + "," + mc}).saveAsTextFile(output)  
}

//######################################################################################
// Social Netork only
// extract_CDF_UMLP
// Comulative Density of sorted error in KM
//######################################################################################
def extract_CDF_UMLPDC(in: String, res: String, minDeg: Int = 0, maxDeg: Int = Int.MaxValue, minMsgCnt: Int = 0, maxMsgCnt: Int = Int.MaxValue) = {
val ML = sc.textFile(in).map(_.split(",")).map(x => (x(0), ((x(1).toDouble,x(2).toDouble), x(3), x(4).toInt, x(5).toInt))).filter({case(u, (ml, p, deg, cnt)) => deg > minDeg && deg < maxDeg && cnt > minMsgCnt && cnt < maxMsgCnt})
val UDTMLP = ML.map({case(u, (ml, p, deg, cnt)) => (u, (ml, p))})
val UMLP = sc.textFile(in).map(_.split(",")).filter(x => x(4).toInt > minDeg && x(4).toInt < maxDeg && x(5).toInt > minMsgCnt && x(5).toInt < maxMsgCnt).map(x => (x(0), ((x(1).toDouble,x(2).toDouble), x(3))))
val split = UMLP.map({case(u,(ml,p)) => (p,u)}).groupByKey().filter(_._2.size > 4).map({case(p,u) => (p, u.splitAt((u.size * 0.7).toInt))})
val train = split.map({case(p,(tr,ts)) => (tr)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val test = split.map({case(p,(tr,ts)) => (ts)}).flatMap(x => x).map(x => (x,1)).reduceByKey(_+_)
val PML = UMLP.join(train).map({case(u,((ml, p),x)) => (p, ml)}).groupByKey().map({case(p, ls) => (p, geometric_median(ls.toList))})
val U_PE = UMLP.join(test).map({case(u, ((ml,p),x)) => (p, (u, ml))}).join(PML).map({case(p, ((u, ml), pl)) => (u, geoDistance_points(ml, pl))})
val AED = U_PE.map({case(u,e) => (1, (e, 1))}).reduceByKey((a,b) => (a._1 + b._1, a._2 + b._2)).map(x => (x._2._1 * 1.0)/x._2._2).collect
val cnt = (U_PE.count / 2.0).toInt
val MED = U_PE.map(_._2).sortBy(x => x).take(cnt).drop(cnt -1)
val Recall = U_PE.count * 1.0/ML_filt_deg_cnt.join(test).count

val pw = new java.io.PrintWriter(new java.io.File(res + "_values.txt"))
pw.write("AED\t" + AED + "\n")
pw.write("MED\t" + MED + "\n")
pw.write("REC\t" + Recall + "\n")
pw.close

val temp1 = U_PE.map(x => (Math.floor(x._2 * 10)/10, 1.0)).reduceByKey(_+_)
val temp2 = sc.parallelize(Array(0.0 to 60.0 by 0.1)).flatMap(x => x).map(x => (Math.floor(x*10)/10,0.0))
val temp3 = temp1.union(temp2).reduceByKey(_+_).sortBy(_._1)
//temp3.map(x => (0, x)).groupByKey().map(x => CDF(x._2.toList)).flatMap(x => x).map(x => x._1 + "\t" + x._2).saveAsTextFile(res)
temp3.map(x => (0, x)).groupByKey().map(x => CDF(x._2.toList)).flatMap(x => x).map(x => x._2).saveAsTextFile(res)
}
