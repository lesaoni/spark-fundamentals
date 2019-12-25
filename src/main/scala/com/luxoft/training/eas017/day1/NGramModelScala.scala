package com.luxoft.training.eas017.day1

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
Original dataset - a subset of Helsinki Corpora (HC) English
https://www.kaggle.com/d2i2k2w/coursera-data-science-capstone-datasets
Tweets from Eng_US_Twitter.csv file have been splitted by sentences using nltk sentence tokenizer

Starting an interactive Spark Shell
 $ spark-shell --conf spark.dynamicAllocation.maxExecutors=4  --conf spark.executor.memory=1g
*/

object NGramModelScala extends App {

  val sc = new SparkContext("yarn", "N-gram model")

  val sentences = sc.textFile("hdfs:///user/centos/eas-017/n-gram/tokenized_twitter_sentences.txt",100)

  val emoticons = sc.textFile("hdfs:///user/centos/eas-017/n-gram/emoticons.txt")

  /* TODO: implement function cleanText
    Step 1: Remove all emoticons, contained in emoticons RDD
    Step 2: Keep only alpha-numeric characters and whitespaces (Hint: you might want to use regex [^a-zA-Z0-9\\s])
    Step 3: Convert to lower case
   */
  def cleanText(text : String) : String = ???

  val cleanedSentences : RDD[String] = ???

  // TODO: calculate how many words there are in the dataset

  val totalWordCount : Long = ???

  // TODO: Calculate unigrams (single word counts), print 10 most popular words
  val unigrams : RDD[( String, Int)] = ???

  /* TODO: Claculate bigrams, print 10 most popular bigrams
    Implement function splitIntoBigrams, which returns the sequence of all bigrams in the sentence with their counts
    Example: for the sentence "thank you for rt" the result should be ("thank", "you"), ("you", "for"), ("for, "rt")
    Hint: you might want to use "sliding" method of Scala collections
    Before applying the method ensure, that the input contains at least 2 words
  */
  def splitIntoBigrams(sentence : String) : Seq[(String, String)] =  ???

  val bigrams : RDD[((String, String), Int)] = ???


  // TODO: Claculate trigrams just like bigrams, print 10 most popular trigrams
  def splitIntoTrigrams(sentence : String) : Seq[(String, String, String)] =  ???

  val trigrams : RDD[((String, String, String), Int)] = ???

  // TODO: Demo - copy paste class code in the shell
  class SparkTrigramModel(val unigrams : RDD[( String, Int)],
                          val bigrams  : RDD[((String, String), Int)],
                          val trigrams : RDD[((String, String, String), Int)]) {
    lazy val v = unigrams.count()

    private def checkAlpha(alpha : Double) = require(alpha >= 0 && alpha <= 1)

    // Returns probability of one given word based on unigrams, alpha - smoothing parameter
    def p_w1(raw_w1: String, alpha: Double = 0.0) : Double = {
      checkAlpha(alpha)
      val w1 = cleanText(raw_w1)
      val f_w1 = this.unigrams.lookup(w1).headOption.getOrElse(0)
      (f_w1 + alpha) / this.v
    }

    def p_w2_given_w1(raw_w1 : String, raw_w2 : String, alpha : Double = 0.0) : Double = {
      checkAlpha(alpha)
      val w1 = cleanText(raw_w1)
      val w2 = cleanText(raw_w2)

      val f_w1w2 = bigrams.lookup((w1, w2)).headOption.getOrElse(0)
      val f_w1 = unigrams.lookup(w1).headOption.getOrElse(0)

      (f_w1w2 + alpha)/(f_w1 + this.v)
    }

    def p_w3_given_w1w2(raw_w1 : String, raw_w2 : String, raw_w3 : String, alpha : Double = 0.0) : Double = {
      checkAlpha(alpha)
      val w1 = cleanText(raw_w1)
      val w2 = cleanText(raw_w2)
      val w3 = cleanText(raw_w3)

      val f_w1w2w3 = trigrams.lookup((w1, w2, w3)).headOption.getOrElse(0)
      val f_w1w2 = bigrams.lookup((w1, w2)).headOption.getOrElse(0)

      (f_w1w2w3 + alpha)/(f_w1w2 + this.v)
    }

    def p_bigram(sentence : String, alpha : Double =0.0): Double = {
      checkAlpha(alpha)
      val words = cleanText(sentence).split(" ")
      val sentBigrams = splitIntoBigrams(sentence)
      val p_w1 = this.p_w1(words.head)

      sentBigrams.map {case (w1, w2) => p_w2_given_w1(w1, w2, alpha)}.foldLeft(p_w1)(_ * _)
    }

    def p_trigram(sentence : String, alpha : Double =0.0): Double = {
      checkAlpha(alpha)
      val words = cleanText(sentence).split(" ")
      val sentTrgrams = splitIntoTrigrams(sentence)
      val p_w1 = this.p_w1(words.head)

      sentTrgrams.map {case (w1, w2, w3) => p_w3_given_w1w2(w1, w2, w3, alpha)}.foldLeft(p_w1)(_ * _)
    }

    def predictNextMostProbableWord(rawInputW1 : String) : Option[String] = {
      val inputW1 = cleanText(rawInputW1)
      val bigramO = this.bigrams.filter { case ((w1, _), _) => w1 == inputW1 } take 1 headOption //assume bigrams are already sorted

      bigramO map { case ((_,w2) ,_) => w2 }
    }

    def predictNextMostProbableWord(rawInputW1 : String, rawInputW2 : String) : Option[String] = {
      val inputW1 = cleanText(rawInputW1)
      val inputW2 = cleanText(rawInputW2)
      val trigramO = this.trigrams.filter { case ((w1, w2, _), _) => w1 == inputW1 && w2 == inputW2} take 1 headOption //assume trigrams are already sorted

      trigramO map { case ((_, _,w3) ,_) => w3 }
    }
  }

  // TODO run examples
  val nGramModel = new SparkTrigramModel(unigrams, bigrams, trigrams)

  println(nGramModel.p_w1("Thank"))

  println(nGramModel.p_w2_given_w1("Thank", "you"))

  println(nGramModel.p_w3_given_w1w2("Thank", "you", "for"))

  println(nGramModel.predictNextMostProbableWord("How", "are"))

}