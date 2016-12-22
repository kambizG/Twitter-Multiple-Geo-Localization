val friends = sc.textFile("friends.txt").map(x => (x.split("\t")(0).toLong, x.split("\t")(1).toLong))
val friends_rev = friends.map(x => (x._2, x._1))
val friends_lr = friends.union(friends_rev)



val valid_edges = friends_lr.join(leftids)
val valid_graph = valid_edges.map(x => ((x._1, x._2._1), 1)).reduceByKey((a,b) => a+b).filter(x => x._2 > 1).map(x => x._1).filter(x => x._1 < x._2)
val graph = valid_graph.map(x => x._1 + "\t" + x._2)
graph.saveAsTextFile("graph")
