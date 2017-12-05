//######################################################################################
// Preprocessing
// Clean Documents and Remove stopwords using the longstopwords.txt
//######################################################################################
def cleanRemoveStopWords(document: String, sw: Array[String], minLen: Integer, maxLen: Integer): String = {
var words = document.trim.toLowerCase().replaceAll("[!\"“”$%&'*+,./:;<=>?\\[\\]^`{\\|}~()]", " ").replaceAll("http", "").replaceAll("\\\\", "").replaceAll("\\s+", " ").split("\\s")
var res = ""
for(w <- words){
if(w.length > minLen && w.length < maxLen && !sw.contains(w))
res += w + " "
}
return res.trim
}

//######################################################################################
// Extract Location Count Frequency
//####################################################################################
def extract_location_count_frequency(status_File: String, outPut: String) ={
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val stats = sc.textFile(status_File).map(_.split(",",6)).map(x => (x(1), x(4).toDouble, x(3).toDouble))
val loc_freq = stats.map(x => (x,1)).reduceByKey(_+_).map(x => (x._1._1, 1)).reduceByKey(_+_).map(x => (x._2, 1)).reduceByKey(_+_).sortBy(_._1)
loc_freq.map(x => x._1  + "," + x._2).saveAsTextFile(outPut)
}

//######################################################################################
// Extract Ego Network By User_Id
//######################################################################################
def extract_ego(uid: String, mutual_friends: String, outPut: String) = {
val friends = sc.textFile(mutual_friends).map(_.split(",")).map(x => (x(0), x(1)))
val mf = friends.map(x => (x._2, x._1)).union(friends)
val direct_friends = mf.filter(_._1 == uid)
val fr_fr = direct_friends.map(x => (x._2, x._1)).join(mf).map(x => (x._1, x._2._2)).filter(_._2 != uid)
val egoNet = fr_fr.map(x => (x._2, x._1)).join(direct_friends.map(x => (x._2, x._1))).map(x => (x._2._1, x._1))
direct_friends.union(egoNet).map(x => x._1 + "," + x._2).saveAsTextFile(outPut)	
}

//Test Case
//extract_ego("17057819", "mutual_friends.txt", "ego_17057819")

//##############################################################################################################
// Check Temporal Distribution on different partitions using Leuven Partitioning.
//##############################################################################################################
//The community detection algorithm to partition the graph into multiple communities
// python /home/kambiz/data/tw_data_all_clean/clustering_programs_5_2/select.py -n mf_lo.txt -p 4 -f result -c 1
def top_n_partitions(n: Int, partition: String): Array[(String, Int)] = {
//return sc.textFile(partitions).map(x => (x.split(",")(1), 1)).reduceByKey(_+_).sortBy(_._2, false).take(n)
return null
}

def partition_temporal_distribution(pid: Long, stats_file: String, partitions: String) = {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val stats = sc.textFile(stats_file).map(_.split(",",7)).map(x => (x(1),(dateparser.parse(x(2)).getHours)))
val parts = sc.textFile(partitions).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x.split("\\s"))
val par_n = parts.filter(_._1 == pid).map(x => (x._2, x._1))
val sta_n = stats.join(par_n).map(x => (x._2._1, 1))
val count = sta_n.count.toDouble
val hourD = sta_n.reduceByKey(_+_).map(x => (x._1, x._2 * 1.0 / count)).sortBy(_._1)
hourD.map(_._2).collect
}
//Test Case
//top_n_partitions(10, "tp")
//partition_temporal_distribution(3119, "tw_lo.txt", "tp")

//#####################################################################################
// Get Partition Message Count: Given a partition id return the count of messages published by users in that partition
//#####################################################################################
def get_partition_message_count(stats: String, partitions: String, pid: String): Double ={
val user_stats = sc.textFile(stats).map(_.split(",",7)).map(x => (x(1),1)).reduceByKey(_+_)
val parts = sc.textFile(partitions).map(_.split(",")).map(x => (x(0), x(1))).filter(_._2 == pid)
return parts.join(user_stats).map(_._2._2).sum
}


//#####################################################################################
// geoMetric Distance
//#####################################################################################
def deg2rad(deg:Double): Double = {return (deg * Math.PI / 180.0)}

def rad2deg(rad: Double ): Double = {return (rad * 180 / Math.PI)}

def geoDistance(lat1:Double, lon1:Double , lat2:Double, lon2:Double): Double = {
if(lat1 == lat2 && lon1 == lon2) return 0.0
var theta = lon1 - lon2
var geoDist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta))
geoDist = Math.acos(geoDist)
geoDist = rad2deg(geoDist)
geoDist = geoDist * 60 * 1.1515
return (geoDist * 1.609344)
}

def geoDistance_points(gp1:(Double,Double), gp2:(Double, Double)): Double = {
return geoDistance(gp1._1, gp1._2, gp2._1, gp2._2)
}

//######################################################################################
// Get Frequent locations for each user with less than X km threshold
//######################################################################################
case class Cons[+A] (val value: A, val next: Cons[A])

def get_freq_locations(locations:List[(Double, Double)]): List[(Double, Double)] = {
var freqLocations = Cons(locations(0), null)
for(i <- 1 to locations.size -1){
val l = locations(i)
var found = false
var current = freqLocations
while(current != null && !found){
 val fl = current.value
 if(geoDistance_points(l, fl) < 5){
  found = true
}
 current = current.next
}
if(!found){
 freqLocations = Cons(l, freqLocations)
}}
var result = List(freqLocations.value)
var next = freqLocations.next
while(next != null){
 result = result :+  next.value
 next = next.next
}
return result
}

//####################################################################################
// Extract Frequent locations for each user with less than X km threshold
//####################################################################################

case class Cons[+A] (val value: A, val next: Cons[A])

def get_frq_loc(locations:List[(Double, Double)], threshold: Integer): List[(Double, Double)] = {
var freqLocations = Cons(locations(0), null)
for(i <- 1 to locations.size -1){
val l = locations(i)
var found = false
var current = freqLocations
while(current != null && !found){
 val fl = current.value
 if(geoDistance_points(l, fl) < threshold){
  found = true
} 
 current = current.next
}
if(!found){
 freqLocations = Cons(l, freqLocations)
}}
var result = List(freqLocations.value)
var next = freqLocations.next
while(next != null){
 result = result :+  next.value
 next = next.next
}
return result
}

//####################################################################################
// Average Location Distance
//####################################################################################
def get_avg_dist(locations:List[(Double, Double)]): Double = {
if(locations.size == 1) return 0
var totalDist = 0.0
var count = 0
for(i <- 0 to locations.size -2){
for(j <- 1 to locations.size - 1){
count = count + 1
totalDist = totalDist + geoDistance_points(locations(i), locations(j))
}}
return totalDist/count
}

//######################################################################################
// geometric_median
//######################################################################################
def geometric_median(neighbors:List[(Double, Double)]): (Double,Double) = {
if(neighbors.size < 3) return (neighbors(0))
var distArray = Array.fill[Double](neighbors.size, neighbors.size)(-1)
var minDist = java.lang.Double.MAX_VALUE
var result = (-1.0, -1.0)
for(i <- 0 to neighbors.size - 1){
 var dist = 0.0
 val n1 = neighbors(i)
 for(j <- 0 to neighbors.size - 1){
   if(i != j){
    if(distArray(j)(i) != -1.0){
      dist += distArray(j)(i)
    }else{
      val n2 = neighbors(j)
      val newDist = geoDistance_points(n1, n2)
      distArray(i)(j) = newDist
      dist += newDist
    }}}
 if(dist < minDist) {
   minDist = dist
   result = n1
 }}
return result }

//######################################################################################
// geometric_median with text
//######################################################################################
def geoMetric_median_with_text(neighbors:List[((Double, Double), String)]): ((Double,Double),String) = {
if(neighbors.size == 1) return (neighbors(0))
if(neighbors.size == 2) return (neighbors(0)._1, neighbors(0)._2 + " " + neighbors(1)._2)
var distArray = Array.fill[Double](neighbors.size, neighbors.size)(-1)
var minDist = java.lang.Double.MAX_VALUE
var result = (-1.0, -1.0)
var text = ""
for(i <- 0 to neighbors.size - 1){
text += neighbors(i)._2 + " "
var dist = 0.0
val n1 = neighbors(i)
for(j <- 0 to neighbors.size - 1){
  if(i != j){
   if(distArray(j)(i) != -1.0){
    dist += distArray(j)(i)
   }else{
    val n2 = neighbors(j)
    val newDist = geoDistance_points(n1._1, n2._1)
    distArray(i)(j) = newDist
    dist += newDist
   }}}
if(dist < minDist) {
minDist = dist
result = n1._1
}}
return (result,text) }

//######################################################################################
// Extract Sample Data and Network between two dates "from" and "to"
// Sample date: "EEE MMM dd HH:mm:ss zzz yyyy" = "Tue Sep 23 23:52:13 CEST 2014"
//######################################################################################
def createSample(dateFrom:String , dateTo: String, stats: String, mutualFriends: String, outPutDir: String)= {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val from = dateparser.parse(dateFrom).getTime
val to = dateparser.parse(dateTo).getTime
val st = sc.textFile(stats).map(x => (dateparser.parse(x.split(",")(2)).getTime, x))
val sample_stats = st.filter(x => x._1 > from && x._1 < to)
sample_stats.map(_._2).saveAsTextFile(outPutDir + "sample_data")
val sample_ids = sample_stats.map(x => (x._2.split(",")(1), 1)).reduceByKey(_+_)
val mf = sc.textFile(mutualFriends).map(_.split(",")).map(x => (x(0), x(1)))
val temp = mf.join(sample_ids).map(x => (x._1, x._2._1))
temp.map(x => x._1 + "," + x._2).saveAsTextFile(outPutDir + "mf")
}

//Test Case - Autumn 2014
//:load /home/kambiz/data/tw_data_all_clean/tw_loi/scripts/extra.scala
//createSample("Sun Aug 31 00:00:00 CEST 2014","Mon Dec 01 00:00:00 CEST 2014","tw_lo.txt","mf_lo.txt","sample_data/")

//######################################################################################
// Extract CDF of a count array 
//######################################################################################
def CDF(arr: List[(Double, Double)]): Array[(Double, Double)] ={
var res = Array.fill[(Double, Double)](arr.size)(-1.0, -1.0)
res(0) = arr(0)
var sum = arr(0)._2
for(i <- 1 to arr.size - 1){
sum += arr(i)._2
res(i) = (arr(i)._1, res(i-1)._2 + arr(i)._2)
}
for(i <- 0 to arr.size - 1)
res(i) = (res(i)._1, res(i)._2/sum)
return res
}

//######################################################################################
// Extract partiotions with count more than a threshold and with message count limist.
// The limit on message count is specified as min and max limits. 
// The limit is on ratio of the message counts in each partition in specific time-slot
//######################################################################################
def extract_valid_partitions(partitions: String, stats: String, min_par_avg_msg_cnt: Int, max_par_avg_msg_cnt: Int, day: Int, time: Int, min_par_cnt: Int): org.apache.spark.rdd.RDD[(String, Double)]= {
val user_part = sc.textFile(partitions).map(_.split(",")).map(x => (x(0), x(1)))
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
val status = sc.textFile(stats).map(_.split(",",7)).map(x => (x(1), dateparser.parse(x(2)))).map({case(id,d) => (id, (d.getDay, d.getHours))}).map({case(u,(d,t)) => (u, (if(d % 6 > 0) 1 else 0, if(t - 2 < 6) 0 else if (t - 7 < 12) 1 else 2))})
val st_filt = status.filter({case(id, (d, t)) => d == day && t == time})
val par_msg_cnt = user_part.join(st_filt).map(x => (x._2._1, 1)).reduceByKey(_+_)
val part_count = user_part.map(x => (x._2, 1)).reduceByKey(_+_).filter(_._2 > min_par_cnt)
val par_msg_ratio = par_msg_cnt.join(part_count).map(x => (x._1, x._2._1 * 1.0 / x._2._2)).filter(x => x._2 > min_par_avg_msg_cnt && x._2 < max_par_avg_msg_cnt)
return par_msg_ratio
}

//######################################################################################
// Extract MED and Recall for all partitions separately the result is a textFile containing: PID CNT MED REC
//######################################################################################
/*def extract_partitions_MED_REC(partitions: String, in: String, output: String, min_par_size: Int) = {
val parts = sc.textFile(partitions).map(_.split(",")).map(x => (x(1),1)).reduceByKey(_+_)
var arr = Array[(String, Int, (Double, Double))]()
parts.filter(_._2 > min_par_size).collect.foreach(x => arr :+= (x._1, x._2, extract_MED_UDTMLPDC(in, 0, 100000, 0, 100000, 1, 1, x._1.toInt, 4, 0, 100000)))
val PCER = sc.parallelize(arr)
PCER.map(x => x._1 + "\t" + x._2 + "\t" + x._3._1 + "\t" + x._3._2).saveAsTextFile(output)
}*/

//Unit Case
//extract_partitions_MED_REC("partitions/partitions_inf.txt", "UDTMLPDC/UDTMLPDC_inf_WE_WD_N",  "CDF/PCER", 100)

// ################################################################
// Extract First and Last Date in the dataset.
// ################################################################
def get_First_Date(stats: String): String = {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
return sc.textFile(stats).map(_.split(",")).map(x => (dateparser.parse(x(2)).getTime,x(2))).sortBy(_._1).map(_._2).first
}

def get_Last_Date(stats: String): String = {
val dateparser = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
return sc.textFile(stats).map(_.split(",")).map(x => (dateparser.parse(x(2)).getTime,x(2))).sortBy(_._1, false).map(_._2).first
}