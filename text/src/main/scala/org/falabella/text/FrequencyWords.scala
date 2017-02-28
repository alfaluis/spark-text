package org.falabella.text

import java.util.Properties
import java.text.Normalizer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


case class InteractionSub(
    val tipo       : String,
    val comentarios: String
  )
  
  case class FrequencyWords(
    val words: Array[String],
    val count: Long
  )
  
object FrequencyWords {
  val HIVE_SAVE_MODE = SaveMode.Overwrite
  //var nameInteractions = "falabella.interacciones"
  //var outputTable = "falabella.frequencyWords"
  //var pathFile = "file:///home/cloudera/dataFalabella/wordToSearch.txt"
  var nameInteractions = ""
  var outputTable = ""
  var pathFile = ""
  
  def main(args: Array[String]) {
    if(args.size == 3){
      nameInteractions = args(0)
      outputTable = args(1)
      pathFile = args(2)
      args.foreach(arg => println("Arguments passed -> " + arg.toString))
    } else {
      println ("Hubo un error al parsear los parametros:")
      args.foreach (arg => println ("- " + arg.toString))
      System.exit (1)
    }
    
    val conf = new SparkConf().setAppName("myapp")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new HiveContext(sc)
   
    
    doProcess
    sc.stop()
  }
  
  def doProcess(implicit hive: HiveContext){
    implicit val sqlContext = hive
    
    import sqlContext.implicits._
    val interactions = sqlContext.read.table(nameInteractions)
    println("printing read table: ")
    interactions.show()
    
    
    // second way to filter data 
    // just using a list of fields
    
    // read words with similar words desired to match
    val words = sqlContext.sparkContext.textFile(pathFile)
    val desiredWords = words.map{x => x.split(",")}.collect
    
    
    val comments = interactions.select($"tipo", lower($"comentarios").alias("comentarios")
        ).as("df2"
            ).filter(!$"df2.comentarios".contains("nulo")).as[InteractionSub]
    
    /*	Example how use Normalize function
     * 
     * val str = Normalizer.normalize("téléphone",Normalizer.Form.NFD)
    	 val exp = "\\p{InCombiningDiacriticalMarks}+".r
       exp.replaceAllIn(str,"")
     */
    
    val exp = "\\p{InCombiningDiacriticalMarks}+".r
    
    val commentWithoutAccent = comments.map{x =>
      val str = Normalizer.normalize(x.comentarios, Normalizer.Form.NFD)
      InteractionSub(x.tipo, exp.replaceAllIn(str,""))}
    
    val out = desiredWords.map{ArrayWord => 
      val numOcurrences = commentWithoutAccent.filter(comm => 
        stringContains(comm.comentarios, ArrayWord)
        ).count()
      new FrequencyWords(ArrayWord, numOcurrences)  
    }
    
    sqlContext.sparkContext.parallelize(out).toDF.write.mode(HIVE_SAVE_MODE).saveAsTable(outputTable)
    
  }
  
  def stringContains(comment: String, fields: Array[String]) = {
    fields.exists(comment.contains)
  }
  
  
  
  
}