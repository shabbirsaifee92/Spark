package org.shabbir.hw4

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Shabbir Saifee
 */
object PageRank {

  def main(args : Array[String]){
      
      val conf = new SparkConf()
                              .setAppName("PageRank")
                             .setMaster("local")
                              //.setMaster("yarn")  
      val sc = new SparkContext(conf)
//      
      val input = args(0)
      val output = args(1)

      /*
       * Pre-Processing 
       * 
       * Operations:
       * 1. Reads input file
       * 2. for each line in input file removes null objects 
       * 3. for each line in collection splits t get page name and adjacency list
       * 4. for each line in collections returns a pairRDD (pageName , adjacencyList)
       */
      
      val pages = sc.textFile(input)
                      .map(line => Bz2WikiParser.parseHTML(line))
                      .filter(line => line!=null)
                      .map(line => line.split("&&"))
                      .map(line => if(line.length ==1){
                                       (line(0),List())
                                    }
                                    else{
                                     (line(0),line(1).split("<>").toList)
                                     })
                                     
      /*
       * To persist collection in the memory                 
       */
      pages.persist()  
      
      
      val pairRDDPage = pages
                          .flatMap 
                          {case (name,adjcencyList) => adjcencyList.map(node => (node,List[String]()))}
                          .reduceByKey((list1,list2) => list1 ::: list2)
                          .union(pages)
                          .reduceByKey((list1,list2)=> list1:::list2)
                     

     /*
      * Gets total number of nodes in the graph                     
      */
     val TOTAL_PAGES = pairRDDPage.count()
                      
     var pageWithPageRank = pairRDDPage.map(node => (node._1,1.0/TOTAL_PAGES)) 
     
     val LAMBDA = 0.85
     val TOPK = 100
     var DELTA = 0.0
     
     for( k <- 1 to 10)
     {
       
       
       /*
        * Delta calculation
        * 
        * operations
        * 1. Get all the sink nodes
        * 2. Get the page ranks of all sink nodes
        * 3. Convert to pairRdd and calculate the sum
        */
       DELTA = pairRDDPage.filter(node => node._2.isEmpty)
                          .join (pageWithPageRank) 
                          .map (node => node._2._2)
                          .reduce((sum,n)=> sum+n)
                          
       /*
        * Page Rank Calculation
        * 
        * Operations
        * 1. join adjacency list and page rank
        * 2. for each node in adjacency list of a particular page, emit the contribution
        * 3. for each node , calculate the sum of in-link contribution 
        * 4. calculate total page rank for each node  
        */
       pageWithPageRank = pairRDDPage.join(pageWithPageRank)
                                     .flatMap{ case(name,(adjcencyList,pr)) =>{
                                       
                                       List(List((name,0.0)),adjcencyList.map(node=> (node, pr/adjcencyList.size))).flatten
                                     }
                                       }
                                       .reduceByKey((sum,n) => sum+n)
                                       .map(node => (node._1, (1-LAMBDA)/TOTAL_PAGES + (LAMBDA)*((DELTA/TOTAL_PAGES)+node._2)))
     
     }
                          
     /*
      * Top K Pages
      * 1. swap position of page rank and page name
      * 2. sort and take top k pages according to page rank
      * 3. swap back
      */
     val topKPages = pageWithPageRank.map(node => node.swap).top(TOPK).map(node => node.swap)
     sc.parallelize(topKPages, 1).saveAsTextFile(output)                     
                                                   
                                      
                                   
  }


}
