import scala.math.min

// Either use the serializable for the object class or
// ------------------------------------------------------------
object EditDistance extends java.io.Serializable{
  def editDist[A](a: Iterable[A], b: Iterable[A]) =
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
          case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
        }) last
}
// or use is as a function instead of a mehtod in a serializable class.
// ------------------------------------------------------------
def editDist[A](a: Iterable[A], b: Iterable[A]) =
    ((0 to b.size).toList /: a)((prev, x) =>
      (prev zip prev.tail zip b).scanLeft(prev.head + 1) {
          case (h, ((d, v), y)) => min(min(h + 1, v + 1), d + (if (x == y) 0 else 1))
        }) last

//------------------------------------------------------------------------------------------------------------------------------
//------------------------------------------------------------------------------------------------------------------------------
val user_location = sc.textFile("user_location.txt").filter(x => x.split("\t").size > 1).map(x => (x.split("\t")(0), x.split("\t")(1).trim().toLowerCase())).filter(x => x._2 != "")
val ul_clean = user_location.map(x => (x._1, x._2.replaceAll("los angeles", "").replaceAll("california", "").replaceAll(" la ", "").replaceAll("la ", "").replaceAll("la$", "").replaceAll(" ca ", "").replaceAll("ca ", "").replaceAll("ca$", "").replaceAll(",", "").trim())).filter(x => x._2 != "")
val zipcodes = sc.textFile("zipcodes.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toLowerCase()))
val temp = ul_clean.cartesian(zipcodes)
//This option can be used instead of method, but the class needs to be serializable
//val ed = EditDistance
val temp2 = temp.map(x => (x._1._1, (x._2._1, editDist(x._1._2, x._2._2))))
//keep only userids that have address with less than 4 editDist 
val temp3 = temp2.filter(x => x._2._2 < 4)
val leftids = temp3.map(x => (x._1, 1)).reduceByKey((a,b) => a+b).map(x => (x._1.toLong, x._2))

val friends = sc.textFile("friends.txt").map(x => (x.split("\t")(0).toLong, x.split("\t")(1).toLong))
val friends_rev = friends.map(x => (x._2, x._1))
val friends_lr = friends.union(friends_rev)
val valid_edges = friends_lr.join(leftids)
val valid_graph = valid_edges.map(x => ((x._1, x._2._1), 1)).reduceByKey((a,b) => a+b).filter(x => x._2 > 1).map(x => x._1).filter(x => x._1 < x._2)
val graph = valid_graph.map(x => x._1 + "\t" + x._2)
graph.saveAsTextFile("graph")
