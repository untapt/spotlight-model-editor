package org.idio.dbpedia.spotlight.stores

/**
 * Created by dav009 on 31/12/2013.
 */

import org.dbpedia.spotlight.db.memory.{MemoryStore,MemoryTokenTypeStore}
import java.io.{File, FileInputStream}
import org.dbpedia.spotlight.db.stem.SnowballStemmer
import scala.collection.mutable.HashMap

class IdioTokenResourceStore(val pathtoFolder:String, stemmerLanguage:String) {

  var tokenStore:MemoryTokenTypeStore= MemoryStore.loadTokenTypeStore(new FileInputStream(new File(pathtoFolder,"tokens.mem")))
  var stemmer:SnowballStemmer = new SnowballStemmer(stemmerLanguage)

  /*
  * Prints the Tokens in the Store
  * */
  def showTokens(){
    for (token:String<-this.tokenStore.tokenForId){
        val counts = this.tokenStore.counts(tokenStore.idFromToken.get(token))
        println(token + "--" + this.tokenStore.getTokenType(token) + "---" +  counts)
    }
  }

  /*
  * boost the counts of a tokenId
  * */
  def raiseCountsForToken(token:String, boost:Int){
      val tokenId:Int = tokenStore.idFromToken.get(token)
      this.tokenStore.counts(tokenId) = tokenStore.counts(tokenId) + boost
  }

  /*
  * Add a token to the store if it doesn't exist
  * */
  private def getOrCreateTokenNoMapUpdate(token:String):Int={
    if (this.tokenStore.idFromToken.containsKey(token)){
      return this.tokenStore.idFromToken.get(token)
    }

    this.tokenStore.tokenForId = this.tokenStore.tokenForId :+ token
    this.tokenStore.counts = this.tokenStore.counts :+ 1

    return this.tokenStore.tokenForId.size -1
  }

  /*
* Add a set of Tokens updating the internal Token store
* Updates the reverse lookup if necessary
* */
  def addSetOfTokens(stemmedTokens:scala.collection.Set[String]):scala.collection.Map[String,Int]={

    val mapOfIdentifiers = scala.collection.mutable.HashMap[String, Int]()
    val currentStoreSize = this.tokenStore.tokenForId.size

    stemmedTokens.foreach{ token:String =>
      val id = this.getOrCreateTokenNoMapUpdate(token)
      mapOfIdentifiers.put(token, id)
    }

    val newStoreSize = this.tokenStore.tokenForId.size

    if (newStoreSize != currentStoreSize)
      this.tokenStore.createReverseLookup()

    return mapOfIdentifiers
  }

  /*
  * Returns the Id of a token if it already exists.
  * Otherwise it adds the token to the store and returns its Id
  * */
  def getOrCreateToken(token:String):Int = {
    this.addSetOfTokens(scala.collection.immutable.Set[String](token))
    return this.tokenStore.idFromToken.get(token)
  }

  def export(pathToFolder:String){
    MemoryStore.dump(this.tokenStore, new File(pathToFolder,"sf.mem"))
  }

  /*
  * Returns a stemmed version of a Token.
  * */
  def stemToken(token:String):String = {
    var stemmedToken:String = stemmer.stem(token)
    return stemmedToken
  }

  /*
  * Transform ContextWords into ContextTokens(stemmed)
  * Returns a map from Token to Token Frequency
  * */
  def getContextTokens(contextWords:Array[String], contextCounts:Array[Int]):HashMap[String,Int] = {
    val contextTokenMap:HashMap[String,Int] = new HashMap[String, Int]()

    (contextWords, contextCounts).zipped foreach { (word, counts) =>
      {
        val stemmedWord:String = this.stemToken(word)
        var currentCount = 0
        if (contextTokenMap.contains(stemmedWord)){
          currentCount = contextTokenMap.get(stemmedWord).get
        }
        contextTokenMap.put(stemmedWord, currentCount + counts)

      }
    }
    return contextTokenMap
  }


}
