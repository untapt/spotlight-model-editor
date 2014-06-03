package org.idio.wordnet

import scala.collection.mutable

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
object WordnetMappingParser {


  private def cleanWikipediaId(wikipediaID: String): String = {
    wikipediaID.replace(" ","_")
  }


  private def parse(line: String): (String, String) ={
    val splitLine = line.trim.split("\t")


    val wikipediaId = cleanWikipediaId(splitLine(2))

    val wordnetPos = "n"//splitLine(0).split(".")(1)
    val wordnetOffset =  splitLine(1)
    val wordnetId = wordnetOffset + "-" + wordnetPos

    (wordnetId, cleanWikipediaId(wikipediaId))
  }

  def readMapping(pathToFile:String): mutable.HashMap[String, String] = {

    val mapping = new mutable.HashMap[String, String]()
    val lines = scala.io.Source.fromFile(pathToFile).getLines.toList
    lines.map{ line:String =>
         val (wordnetId, wikipediaId) = parse(line)
         mapping.put(wikipediaId, wordnetId)
    }

    mapping
  }

}
