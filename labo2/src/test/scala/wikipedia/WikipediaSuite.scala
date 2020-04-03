package wikipedia

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import wikipedia.WikipediaRanking._

class WikipediaSuite extends FunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("'occurrencesOfLang' should work for (specific) RDD with one element") {
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Java Jakarta")))
    val res = occurrencesOfLang("Java", rdd)
    assert(res == 1, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
  }

  test("'occurrencesOfLang' should not count Java in Javascript") {
    val rdd = sc.parallelize(Seq(WikipediaArticle("title", "Jakarta Javascript")))
    val res = occurrencesOfLang("Java", rdd)
    assert(res == 0, "occurrencesOfLang given (specific) RDD with one element should equal to 1")
  }

  test("'rankLangs' should work for RDD with two elements") {
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler")))
    val ranked = rankLangs(langs, rdd)
    assert(ranked.head._1 == "Scala")
  }

  test("'rankLangs' should be sorted by descending order") {
    val langs = List("Scala", "Java")
    val rdd = sc.parallelize(List(WikipediaArticle("1", "Scala is great"), WikipediaArticle("2", "Java is OK, but Scala is cooler"),
      WikipediaArticle("3", "Java is not cool"), WikipediaArticle("4", "However, Java is more used")))
    val ranked = rankLangs(langs, rdd)
    assert(ranked.head._1 == "Java")
    assert(ranked.head._2 == 3)
  }

  test("'makeIndex' creates a simple index with two entries") {
    val langs = List("Scala", "Java")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    assert(index.count() == 2)
  }

  test("'rankLangsUsingIndex' should work for a simple RDD with three elements") {
    val langs = List("Scala", "Java")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional")
      )
    val rdd = sc.parallelize(articles)
    val index = makeIndex(langs, rdd)
    val ranked = rankLangsUsingIndex(index)
    assert(ranked.head._1 == "Scala")
  }

  test("'rankLangsReduceByKey' should work for a simple RDD with four elements") {
    val langs = List("Scala", "Java", "Groovy", "Haskell", "Erlang")
    val articles = List(
        WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
        WikipediaArticle("2","Scala and Java run on the JVM"),
        WikipediaArticle("3","Scala is not purely functional"),
        WikipediaArticle("4","The cool kids like Haskell more than Java"),
        WikipediaArticle("5","Java is for enterprise developers")
      )
    val rdd = sc.parallelize(articles)
    val ranked = rankLangsReduceByKey(langs, rdd)
    assert(ranked.head._1 == "Java")
  }


}

