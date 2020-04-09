package wikipedia

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {
  /* By default Spark outputs a lot of logs, you can set the loggers to Level.WARN
   * to remove most of them.
   */
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.INFO)

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val spark: SparkSession = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  val wikiRdd: RDD[WikipediaArticle] = spark.sparkContext.parallelize(WikipediaData.articles)

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: should you count the "Java" language when you see "JavaScript"?
   *  Hint3: the only whitespaces are blanks " "
   *  Hint4: no need to search in the title :)
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(article => article.text.matches(lang + "[ .,:;!?]") || article.title.matches(lang + "[ .,:;!?]")).count().toInt
  }

  /** (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs.map(lang => (lang, occurrencesOfLang(lang, rdd))).sortBy(rank => rank._2)
  }

  /** Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd.flatMap(article => langs.map(l => (l, article)).filter(x => x._2.text.matches(x._1 + "[ .,:;!?]") || x._2.title.matches(x._1 + "[ .,:;!?]"))).groupByKey()
  }

  /** (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.map(x => (x._1, x._2.size)).sortBy(-_._2).collect().toList
  }

  /** (3) Use `reduceByKey` so that the computation of the index and the ranking is combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap(article => langs.map(l => (l, article)).filter(x => x._2.text.matches(x._1 + "[ .,:;!?]") || x._2.title.matches(x._1 + "[ .,:;!?]")).map(x => (x._1, 1)))
      .reduceByKey(_ + _).sortBy(-_._2).collect().toList
  }

  def main(args: Array[String]) {
    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))
    println(langsRanked)

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))
    println(langsRanked2)

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))
    println(langsRanked3)

    /* Output the speed of each ranking */
    println(timing)
    spark.close()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
