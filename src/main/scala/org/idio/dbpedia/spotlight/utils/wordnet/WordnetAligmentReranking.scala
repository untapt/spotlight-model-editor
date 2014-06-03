package org.idio.dbpedia.spotlight.utils.wordnet

import scala.collection.mutable
import org.idio.dbpedia.spotlight.CustomSpotlightModel
import org.dbpedia.spotlight.model.DBpediaResource
import org.idio.wordnet.WordnetInterface

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

//Wordnet aligment wikipediaId->WordnetId
// wordnetID -> offSet-POS i.e: 123-n
class WordnetAligmentReranking(pathToModel: String, wordnetAligment: mutable.HashMap[String, String]){


   val spotlightModel = new CustomSpotlightModel(pathToModel)

   private def findMatchingTopics(): List[DBpediaResource] = {

     val foundDbpediaResources = wordnetAligment.keySet.par.flatMap{
       wikipediaID: String =>
          try{
             Some(spotlightModel.customDbpediaResourceStore.resStore.getResourceByName(wikipediaID))
          }catch{
            case e:Exception =>
             println("WARNING WIKIPEDIA ID NOT FOUND " + wikipediaID )
             None
          }
     }

     foundDbpediaResources.toList
   }

  /*
  * Gets the lemmas for each topic
  * Returns a list of (dbpediaResource, lemmasOfDbpeidaResource)
  * */
  private def getWordnetLemmas(listOfTopics: List[DBpediaResource]): List[(DBpediaResource, List[String])] ={

    listOfTopics.flatMap{
      topic : DBpediaResource =>
        try{
           val synsetId = wordnetAligment.get(topic.uri).get

           val lemmas = WordnetInterface.getWordnetLemmas(synsetId)
           Some((topic, lemmas))
        }catch{

          case e:Exception => None

        }
    }

  }


 private def expandLemma(lemma:String): List[String] = {

    val expandedLemmas = scala.collection.immutable.HashSet( lemma,
                                                             lemma.toLowerCase,
                                                             lemma.capitalize,
                                                             lemma.split(" ").map(_.capitalize).mkString(" "))

    expandedLemmas.toList
  }


 private def updateSurfaceformsAndAssociations( listOfTopicLemmas: List[(DBpediaResource, List[String])]){

   listOfTopicLemmas.foreach{

     case(topic:DBpediaResource, lemmas:List[String]) =>{

       lemmas.foreach{
         lemma: String =>

           expandLemma(lemma).foreach{

             newLemma: String =>

             // attaching lemma-topic nad making lemma spottable
               spotlightModel.addNewSFDbpediaResource(lemma, topic.uri, Array[String]())
           }

       }
     }
   }

 }



  def updateModel(){
    //Get the topics described in the mapping which exists in the model
    val matchingTopics = findMatchingTopics()

    //Get the lemmas of the matching topics
    val listOfTopicLemmas = getWordnetLemmas(matchingTopics)

    updateSurfaceformsAndAssociations(listOfTopicLemmas)

  }



}
