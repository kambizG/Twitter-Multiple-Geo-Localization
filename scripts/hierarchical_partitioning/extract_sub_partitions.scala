import org.apache.spark.rdd.RDD
import java.io._

@throws(classOf[Exception])
def extract_sub_network(key: Long, edges: RDD[(Long, (String, String))]) ={
//edges = (Long, (String, String)) = (4,(316497600,1345180357)) 
val valid_edges = edges.filter(x => x._1 == key).map(_._2)
valid_edges.map(x => x._1 + " " + x._2).saveAsTextFile("new_partitions/" + key)
}

var max_comm_size = sc.textFile("max_comm_size.txt").first.toInt
val tp = sc.textFile("tp").map(x => (x, x.split("\t")))
val small_tp = tp.filter(x => x._2.size < max_comm_size).map(_._1)
small_tp.saveAsTextFile("tp_small")
val id_com = tp.filter(_._2.size > max_comm_size).map(_._2).zipWithIndex().map(x => (x._2, x._1)).flatMapValues(x => x).map(x => (x._2, x._1))
val mf = sc.textFile("mf.txt").map(_.split(",")).map(x => (x(0), x(1)))
val com_ed = id_com.join(mf).map(x => (x._2._1, (x._1, x._2._2)))
val com_de = com_ed.map(x => (x._1,(x._2._2, x._2._1)))
val com_edges = com_ed.union(com_de).map(x => (x,1)).reduceByKey(_+_).filter(_._2 == 2).map(_._1)

val tp_star = com_edges.groupByKey().filter(_._2.size < max_comm_size * 5).map(x => (x._1, 1))
id_com.map(x => (x._2, x._1)).join(tp_star).map(x => (x._1, x._2._1)).reduceByKey((a,b) => a + "\t" + b).map(_._2).saveAsTextFile("tp_star")

val com_non_star = com_edges.map(_._1).distinct().subtract(tp_star.map(_._1)).collect
val pw = new PrintWriter(new File("count.txt" ))
pw.write(com_non_star.size + "\n")
pw.close
com_non_star.foreach(x => extract_sub_network(x, com_edges))
exit