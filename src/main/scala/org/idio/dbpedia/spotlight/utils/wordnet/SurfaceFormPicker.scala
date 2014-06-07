package org.idio.dbpedia.spotlight.utils.wordnet

import org.idio.dbpedia.spotlight.CustomSpotlightModel
import org.dbpedia.spotlight.model.TokenType
import scala.collection.JavaConverters._
import java.io.{InputStreamReader, BufferedReader, File, PrintWriter}
import net.liftweb.json.JsonParser
import java.net.{HttpURLConnection, URL}


/**
 * Copyright 2014 Idio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author David Przybilla david.przybilla@idioplatform.com
 **/

object CosineSimilarity {

  /*
   * This method takes 2 equal length arrays of integers
   * It returns a double representing similarity of the 2 arrays
   * 0.9925 would be 99.25% similar
   * (x dot y)/||X|| ||Y||
   */
  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }

  /*
   * Return the dot product of the 2 arrays
   * e.g. (a[0]*b[0])+(a[1]*a[2])
   */
  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }

  /*
   * Return the magnitude of an array
   * We multiply each element, sum it, then square root the result.
   */
  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }

}



class CandidateTopicSurfaceForm(surfaceForm:String, topicId:String, matchedTopics:List[String],  spotlightModel:CustomSpotlightModel){




  val topicVector = spotlightModel.customDbpediaResourceStore.resStore.getResourceByName(topicId)

  val topicsVectors = matchedTopics.flatMap{

                matchedTopicId:String =>

                    try{
                      Some(spotlightModel.customDbpediaResourceStore.resStore.getResourceByName(matchedTopicId))
                    }
                    catch{
                      case e:Exception => None
                    }

  }


  def overlapVectors(): scala.collection.mutable.HashMap[TokenType, Double] ={

    val contextVectors = topicsVectors.map{
         dbpediaTopic =>
           spotlightModel.customContextStore.contextStore.getContextCounts(dbpediaTopic)
    }



   try{

    val intersectKeys = contextVectors.map(_.asScala.keySet).reduceLeft[scala.collection.Set[TokenType]]{
           (accSet, newSet) => accSet.intersect(newSet)
    }

    val overlappedVector = scala.collection.mutable.HashMap[TokenType, Double]()

    contextVectors.foreach{
       contextVector =>
         intersectKeys.foreach{
              key =>
                overlappedVector.put(key, overlappedVector.getOrElse(key, 0.0) + contextVector.get(key))
         }

    }

    overlappedVector

   }catch{

     case e:Exception => scala.collection.mutable.HashMap[TokenType, Double]()
   }

  }

  def getSimilarity(overlappedVector:scala.collection.mutable.HashMap[TokenType, Double], topicVector:java.util.Map[TokenType, Int]): Double ={


    if (overlapVectors().size<1){

      println("\t sf:"+surfaceForm+" - "+ topicId+ " .similarity:"+0.0)
        0.0
    }else{

    val allKeys = overlappedVector.keySet.union(topicVector.asScala.keySet)

    val listOfKeys =  allKeys.toList
    val overlappedVectorCalculation:Array[Double] =listOfKeys.map( overlappedVector.getOrElse(_, 0.0) ).toArray
    val overlappedVectortopic:Array[Double] = listOfKeys.map( topicVector.asScala.getOrElse(_,0) * 1.0 ).toArray



    val cosineSimilarity = CosineSimilarity.cosineSimilarity(overlappedVectorCalculation, overlappedVectortopic)
    println("\t sf:"+surfaceForm+" - "+ topicId+ " .similarity:"+cosineSimilarity)


    cosineSimilarity
   }

  }

  val isCandidate :Boolean=  {
    val overlappedVector = overlapVectors()
    val topicVectorCounts = spotlightModel.customContextStore.contextStore.getContextCounts(topicVector)
    val  sim = getSimilarity(overlappedVector, topicVectorCounts )
    if (sim>0.53){
      true
    }else{
       false
    }

  }

}

class SurfaceFormPicker( val spotlightModel: CustomSpotlightModel, lines:List[String]){

  private def  parseFile():List[(String,String, List[String])]={
   lines.flatMap(parseLine)
 }

  def getPicks():List[(Boolean, String, String)] ={

    val parsedLines = parseFile()

    val results = parsedLines.par.map{

      case (topic:String,surfaceForm:String, matchedTopics:List[String])=>{

        val candidateInspector = new CandidateTopicSurfaceForm(surfaceForm, topic, matchedTopics, spotlightModel)
        (candidateInspector.isCandidate, topic:String, surfaceForm:String)
      }

    }

    val only_positive_results = results.par.filter{
      case (candidateFlag:Boolean, topic:String, surfaceForms:String)=>
       candidateFlag }.toList


    only_positive_results


  }

  private def  parseLine(line:String):Option[(String, String, List[String])]={

    println(line)
    val splitLine = line.split("\t")
    if (splitLine.size==3){
       val topic = splitLine(0)
       val sf = splitLine(1)
       val matchedTopics = splitLine(2).split('|').toList.filter(!_.contains("disambiguation") ).map(_.replace(" ","_"))
       Some((topic, sf, matchedTopics))
    }else{

      None
    }
  }



}



object SurfaceFormPicker {


  def main(args: Array[String]){
    val pathToFile = args(0)
    val pathToModel = args(1)
    val outputFile = args(2)
    val spotlightModel  =  new CustomSpotlightModel(pathToModel)

    val surfaceFormPicker = new SurfaceFormPicker(spotlightModel, scala.io.Source.fromFile(pathToFile).getLines().toList)
    val listOfPositives = surfaceFormPicker.getPicks()

    val writer = new PrintWriter(new File(outputFile ))
    listOfPositives.foreach{

      case (flag:Boolean, topic:String, surfaceForms:String) =>
        writer.write(topic+"\t"+surfaceForms+"\n")
    }
  }



}



class WikipediaTopicMatcher(topic:String, surfaceForms:String){

  val firstSurfaceForm = surfaceForms.split('|')(0)
  val page = new URL("http://en.wikipedia.org/w/api.php?format=json&action=query&rvlimit=max&srsearch="+firstSurfaceForm+"&bllimit=max&list=search&srprop=timestamp&sroffset=0")

  val result = {

    val con:HttpURLConnection = page.openConnection().asInstanceOf[HttpURLConnection]
    con.setRequestMethod("GET")
    con.setConnectTimeout(1000)

    val response:StringBuffer = new StringBuffer()


    val in:BufferedReader = new BufferedReader(
      new InputStreamReader(con.getInputStream()));


    var inputLine:String =""
    inputLine = in.readLine()
    while(inputLine!=null ){
      response.append(inputLine)
      inputLine = in.readLine()

    }
    in.close()

    response.toString()
  }


  val jsonResult = scala.util.parsing.json.JSON.parseFull(result)

  val matchedTopics = {
  try{
            jsonResult match {
              case Some(m: Map[String, Any]) =>
                   m("query")  match {
                     case query:Map[String, Any] =>{

                        query("search") match{

                           case topicMatches:List[Map[String, Any]]  =>{

                              topicMatches.flatMap{

                                   topicMatch:Map[String, Any] =>
                                       topicMatch.get("title")
                              }.map{_.asInstanceOf[String].replace(" ","_")}

                           }
                        }

                     }


                   }
            }
  }catch{

    case e:Exception => List[String]()
  }

  }






}



object WikipediaTopicMatcher{

  def parseFile(lines: List[String]):List[(String, String, List[String])] = {

    val parsedLines = lines.map{
      line =>
        val splitLine = line.trim.split("\t")
        val topic = splitLine(0)
        val surfaceForms = splitLine(1)
        (topic, surfaceForms)
    }

    val results = parsedLines.par.flatMap{
      case (topic:String, surfaceForms:String) =>
        try{
            val matcher = new WikipediaTopicMatcher(topic,surfaceForms)
           println("sf:"+surfaceForms + "  "+ matcher.matchedTopics)
           Some(topic, surfaceForms, matcher.matchedTopics)
        } catch{
          case e:Exception => None
        }

    }.toList

    results


  }



  def main(args: Array[String]){

    val file = args(0)
    val outputFile = args(0)+"_withMatchedTopicsColumn"
    val results = parseFile(scala.io.Source.fromFile(file).getLines.toList)

    println("writting to file")
    val writer = new PrintWriter(new File(outputFile ))
    results.foreach{

      case (topic:String, surfaceForms:String, matchedTopics:List[String]) =>
        writer.write(topic+"\t"+surfaceForms+"\t"+matchedTopics.mkString("|")+"\n")
    }

    writer.close()


  }

}
