package org.falabella.text

import java.util.Properties
import java.text.Normalizer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object PruebasText {
  val HIVE_SAVE_MODE = SaveMode.Overwrite
  val nameInteractions = "falabella.interacciones"
  val nameDictionary = "falabella.dictionary"
  val outputTable = "falabella.frequencyWords"
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("myapp")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)
    doProcess
    sc.stop()
  }
  /*
  case class InteractionSub(
    val tipo       : String,
    val comentarios: String
  )
  
  case class FrequencyWords(
    val words: Array[String],
    val count: Long
  )
  */
  
  def doProcess(implicit hive: HiveContext){
    implicit val sqlContext = hive
    
    import sqlContext.implicits._
    val interactions = sqlContext.read.table(nameInteractions)
    val dictionary = sqlContext.read.table(nameDictionary)
    println("printing read table: ")
    interactions.show()
    
    val comments = interactions.filter(!$"comentarios".contains("NULO")).select("tipo", "comentarios").as[InteractionSub]
    println("printing filtered comments: ")
    comments.show()
    
    // first test getting the length of the comment and show it
    comments.map{comm => 
      val lengthComment = comm.comentarios.size
      (comm.comentarios, lengthComment)
    }.show(2)
    
    // second test passing functions into map
    comments.map{comm =>
      val cost = List(0)
      val lengthComment = comm.comentarios.size
      (comm.comentarios, lengthComment, bestMatch(comm.comentarios, 1, lengthComment-2, cost))
    }.show(2)
    
    
    // second way to filter data 
    // just using a list of fields
    
    // read words with similar words desired to match
    val words = sqlContext.sparkContext.textFile("file:///home/cloudera/dataFalabella/wordToSearch.txt")
    val desiredWords = words.map{x => x.split(",")}.collect
    
    
    val comments2 = interactions.select($"tipo", lower($"comentarios").alias("comentarios")
        ).as("df2"
            ).filter(!$"df2.comentarios".contains("nulo")).as[InteractionSub]
    
    /*	Example how use Normalize function
     * 
     * val str = Normalizer.normalize("téléphone",Normalizer.Form.NFD)
    	 val exp = "\\p{InCombiningDiacriticalMarks}+".r
       exp.replaceAllIn(str,"")
     */
    
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    
    val commentWithoutAccent = comments2.map{x =>
      val str = Normalizer.normalize(x.comentarios, Normalizer.Form.NFD)
      InteractionSub(x.tipo, exp.replaceAllIn(str,""))}
    
    val fields = Array("cliente", "casa")
    commentWithoutAccent.filter(comm => stringContains(comm.comentarios, fields))
    
    val out = desiredWords.map{ListWord => 
      val numOcurrences = commentWithoutAccent.filter(comm => 
        stringContains(comm.comentarios, ListWord)
        ).count()
      new FrequencyWords(ListWord, numOcurrences)  
    }
    
    sqlContext.sparkContext.parallelize(out).toDF.write.mode(HIVE_SAVE_MODE).saveAsTable(outputTable)
    
  }
  
  
  def bestMatch(s: String, start: Int, stop: Int, cost: List[Int]) = {
    val candidates = cost.reverse.zipWithIndex
   // val c = a.getOrElse("aaa", 9E4).toString.toDouble
    s.slice(start, stop)
  }
  
  def stringContains(comment: String, fields: Array[String]) = {
    fields.exists(comment.contains)
  }
  
}


