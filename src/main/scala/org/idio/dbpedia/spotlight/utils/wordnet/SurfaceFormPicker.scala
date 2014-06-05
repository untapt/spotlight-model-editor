package org.idio.dbpedia.spotlight.utils.wordnet

import org.idio.dbpedia.spotlight.CustomSpotlightModel
import org.dbpedia.spotlight.model.TokenType
import scala.collection.JavaConverters._
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



class CandidateTopicSurfaceForm(topicId:String, matchedTopics:List[String],  spotlightModel:CustomSpotlightModel){


  var isCandidate :Boolean= true

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

  }

  def getSimilarity(overlappedVector:scala.collection.mutable.HashMap[TokenType, Double], topicVector:java.util.Map[TokenType, Int]): Double ={


    if (overlapVectors().size<1){

      println("\t"+ topicId+ " .similarity:"+0.0)
        0.0
    }

    val allKeys = overlappedVector.keySet.union(topicVector.asScala.keySet)

    val listOfKeys =  allKeys.toList
    val overlappedVectorCalculation:Array[Double] =listOfKeys.map( overlappedVector.getOrElse(_, 0.0) ).toArray
    val overlappedVectortopic:Array[Double] = listOfKeys.map( topicVector.asScala.getOrElse(_,0) * 1.0 ).toArray



    val cosineSimilarity = CosineSimilarity.cosineSimilarity(overlappedVectorCalculation, overlappedVectortopic)
    println("\t"+ topicId+ " .similarity:"+cosineSimilarity)


    cosineSimilarity

  }

  def getValue():Boolean = {
    val overlappedVector = overlapVectors()
    val topicVectorCounts = spotlightModel.customContextStore.contextStore.getContextCounts(topicVector)
    getSimilarity(overlappedVector, topicVectorCounts )
    true
  }

}

class SurfaceFormPicker( val spotlightModel: CustomSpotlightModel, lines:List[String]){

  private def  parseFile():List[(String,String, List[String])]={
   lines.map(parseLine)
 }

  def getPicks(){

    val parsedLines = parseFile()

    parsedLines.par.foreach{

      case (topic:String,surfaceForm:String, matchedTopics:List[String])=>{

        val candidateInspector = new CandidateTopicSurfaceForm(topic, matchedTopics, spotlightModel)
        println(topic+" "+ surfaceForm)
        println("\t"+ candidateInspector.getValue )

      }

    }

  }

  private def  parseLine(line:String):(String, String, List[String])={
    val splitLine = line.split("\t")
    (splitLine(0),splitLine(1), splitLine(2).split('|').toList)
  }



}

object SurfaceFormPicker {


  def main(args: Array[String]){
    val pathToFile = args(0)
    val pathToModel = args(1)
    val spotlightModel  =  new CustomSpotlightModel(pathToModel)

    val surfaceFormPicker = new SurfaceFormPicker(spotlightModel, scala.io.Source.fromFile(pathToFile).getLines().toList)
    surfaceFormPicker.getPicks()
  }



}
