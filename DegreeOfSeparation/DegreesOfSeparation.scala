package com.shabbir.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer


object DegreesOfSeparation2 {
  
  //Starting Node Id
  val startCharID = 5306 //SpiderMan
  val targetCharID = 14 //ADAM 3,031
  
  //Creating a global variable (Accumulator)
  var hitCounter : Option[LongAccumulator] = None
  
  //User defined data type
  type nodeData = (Array[Int],Int, String) // contains adjacency list, min distance, color
  type Node = (Int, nodeData) //A node has hero ID and node data

  //Convert each line from text file to a graph node
  // (nodeID, NodeData)
  def convertToNode(line:String): Node = {
    val fields = line.split("\\s")
    val nodeID  = fields(0).toInt
    var adjacencyList : ArrayBuffer[Int] = ArrayBuffer()
    
    for(i <- 1 to (fields.length -1)){
      adjacencyList += fields(i).toInt
    }
    
    //Default distance and color
    var dist:Int = 9999
    var color:String = "WHITE"
    
    //If this node is starting node
    if(nodeID == startCharID){
      color = "GRAY"
      dist =0
    }
    
    return (nodeID, (adjacencyList.toArray,dist,color))
    
  }
  
  //Create initial RDD for iteration 0 from text file
  def createStartingRDD(sc: SparkContext): RDD[Node]={
    
    val lines = sc.textFile("../SparkScalaMaterial/Marvel-graph.txt")
    return lines.map(convertToNode)
  }
  
  def bfsMap(node: Node): Array[Node] ={
    
    val nodeID:Int = node._1
    val data: nodeData = node._2
    val adjacencyList : Array[Int] = data._1
    val dist: Int = data._2
    var color: String = data._3
    
    var result: ArrayBuffer[Node] = ArrayBuffer()
    //Expand the node if color is GRAY
    if(color == "GRAY"){
      
      //Make the current node BLACK(i.e Visited)
      color =="BLACK"
      
      //Create new GRAY nodes for all the neighbours
      for(neighbour <- adjacencyList){
        
        val neighbourID = neighbour
        val minDist = dist+1
        val neighbourColor = "GRAY"
        
        //If current neighbour is target node then hit the global counter
        if(neighbourID == targetCharID){
          hitCounter.get.add(1)
        }
        
        val newNode : Node = (neighbourID,(Array(),minDist,neighbourColor))
        result += newNode
      }
      
    }
    // Add the original node back in, so its connections can get merged with 
    // the gray nodes in the reducer.
    val newNode = (nodeID,(adjacencyList,dist,color))
    result += newNode
    return result.toArray    
  }
  
  /*
   * Reduce function
   */
    
  def bfsReduce(data1:nodeData, data2:nodeData): nodeData ={
    
    //Extracting the data from both node
    val adjList1: Array[Int] = data1._1
    val adjList2:Array[Int] = data2._1
    val dist1:Int = data1._2
    val dist2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3
    
    //Resultant data fields
    var adjList:ArrayBuffer[Int] = ArrayBuffer()
    var dist:Int = 9999
    var color:String = "WHITE"
    
    // Get adjacencyList
    if(adjList1.length > adjList2.length){
      adjList ++= adjList1
    }
    if(adjList2.length >adjList1.length){
      adjList ++= adjList2
    }
    
    //Get minimum distance
    if(dist1 < dist2 ){
      dist = dist1
    }
    if(dist2 < dist1){
      dist = dist2
    }
    
    //Get darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
    
    return (adjList.toArray,dist,color)    
  }
  
  def main(args : Array[String]){
    
    //Set log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //Create a spark context
    val sc = new SparkContext("local[*]","DegreesOfSeparation")
    
    //Initialize RDD from text file
    var iterationRDD = createStartingRDD(sc)
    
    //Initilaie gloabl accumulator value
    hitCounter = Some(sc.longAccumulator("Hit Counter"))
    
    //Run BFS Algorithm till 10 iteration
    for(i <- 1 to 10){
      println("Running BFS Iteration# " + i)
      
      //Expand Nodes
      val mapped = iterationRDD.flatMap(bfsMap)
 
      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.  
      println("Processing " + mapped.count() + " values.")
      
      //Check if we found the target node
      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount + 
              " different direction(s).")
          return
        }
      }
      
      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.  
      iterationRDD = mapped.reduceByKey(bfsReduce)
    }
    
    
  }
}