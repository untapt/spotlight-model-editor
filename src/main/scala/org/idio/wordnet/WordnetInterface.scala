package org.idio.wordnet

import edu.mit.jwi.data.ILoadPolicy
import edu.mit.jwi.RAMDictionary
import edu.mit.jwi.item.{ISynset, SynsetID}
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
object WordnetInterface {

  val wordnetPath = System.getenv("WNHOME")// "/Users/dav009/Downloads/WordNet-3.0/dict"
  lazy val wordnetFile = new java.io.File(wordnetPath)
  lazy val wordnetDict = new RAMDictionary ( wordnetFile , ILoadPolicy.NO_LOAD ) ;


  def getSynsetFromSynsetIdentifier(synsetStrIdentifier: String): ISynset = {
    wordnetDict.open()
    val (offset, pos) = (synsetStrIdentifier.split("-")(0), synsetStrIdentifier.split("-")(1))
    wordnetDict.getSynset(new SynsetID(offset.toInt,  edu.mit.jwi.item.POS.NOUN))
  }


  def getWordnetLemmas(synsetId: String ): List[String] = {
    val synset = getSynsetFromSynsetIdentifier(synsetId)
    println(synset.getGloss)
    val listOfwords = synset.getWords().asScala.map(_.getLemma)

    // multiwords wornet lemmas are returned separated by _ i.e: hysterical_neurosis
    // Note that wordnet lemmas are not uppercase/lowercase consistent i.e: hysterical_neurosis vs Great_Depression vs depression
    listOfwords.map(_.replace("_", " ")).asInstanceOf[List[String]]
  }


}
