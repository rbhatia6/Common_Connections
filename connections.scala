import scala.collection.mutable.{Map, ArrayBuffer, TreeMap, ListMap, TreeSet}
import java.io._

object connections {
  def readCSV(filename: String): ArrayBuffer[Array[Int]] = {
    println("Reading CSV")
    var rows = ArrayBuffer[Array[Int]]()
    val bufferedSource = io.Source.fromFile(filename)
    for (line <- bufferedSource.getLines.drop(1))
      rows += line.split(",").map(_.trim).map(_.toInt)
    bufferedSource.close
    rows
  }

  def getHM(rows: ArrayBuffer[Array[Int]]): TreeMap[Int, TreeSet[Int]] = {
    var hm = new TreeMap[Int, TreeSet[Int]]()
    println("Constructing HashMap")
    
    var i=0
    for(row <- rows) {
      println("i = %d".format(i))
      i += 1
      if (hm.keySet.exists(_ == row(0))) {
        var keyval = hm.get(row(0)).get
        keyval.add(row(1))
        hm += (row(0)-> keyval)
      }
      else
        hm += (row(0) -> TreeSet(row(1)))
    }
    hm
  }

  def directlyConnected(hm: TreeMap[Int, TreeSet[Int]], k: Int, v: Int): Boolean = {
    hm.get(k).contains(v)
  }

  def getConnIntersections(set1: TreeSet[Int], set2: TreeSet[Int]): TreeSet[Int] = {
    set1.intersect(set2)
  }

  def writeHM(hm: Map[(Int, Int), Int], filename: String): Unit = {
    println("Writing SortedMap....")
    val file = new File(filename);
    val bw = new BufferedWriter(new FileWriter(file))
    var line = ""
    var i = 0
    for ((k,v) <- hm) {
      println("i = %d".format(i))
      i += 1
      line = "%d, %d, %d".format(k._1, k._2, v)
      bw.write(line)
      bw.newLine();
    }
    bw.close()
  }

  def sortHM(hm: TreeMap[(Int, Int), Int]): ListMap[(Int, Int), Int] = {
    println("Sorting HashMap....")
    ListMap(hm.toSeq.sortWith(_._2 >= _._2):_*)
  }

  def main(args: Array[String]): Unit = {
    val rows = readCSV("../common_connection_200k.csv")
    val hm = getHM(rows)
    
    var Conn2HM = new TreeMap[(Int, Int), Int]()
    var i=0
    for ((k1,v1) <- hm) {                    // k1: member_id, v1: level 1 connections set
      i += 1
      var j = 0
      for (k2 <- v1) {                       // k2: level 1 connections keys
        j += 1
        val v2 = hm.getOrElse(k2, TreeSet[Int]())        // v2: level 2 connection set
        var k = 0
        for (k3 <- v2) {                     // k3: level 2 connection keys
          k += 1
          if ((k1 != k3) & !directlyConnected(hm, k1, k3) & !Conn2HM.contains((k3,k1))) {
            println("i = %d; j = %d; k = %d".format(i, j, k))
            val v3 = hm.getOrElse(k3, TreeSet[Int]())
            val connSet = getConnIntersections(v1, v3)
            if (connSet.size > 0)
              Conn2HM += ((k1,k3) -> connSet.size)
          }
        }
      }
    }

    writeHM(Conn2HM, args(0))
    println("Sorting.....")
    val sortedMap = sortHM(Conn2HM)
    writeHM(sortedMap, args(1))
  }
}
