package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, Attribute, ScalaUdf, Row}
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.FloatType
import org.scalatest.FunSuite

import scala.collection.mutable.ArraySeq
import scala.util.Random

case class Student(sid: Int, gpa: Float)
case class TestTable(field1: Int, field2: String, field3: Float)

class CS186UtilsSuite extends FunSuite {
  val numberGenerator: Random = new Random()

  val studentAttributes: Seq[Attribute] =  ScalaReflection.attributesFor[Student]
  val testAttributes: Seq[Attribute] =  ScalaReflection.attributesFor[TestTable]

  // TESTS FOR TASK #3
  /* NOTE: This test is not a guarantee that your caching iterator is completely correct.
     However, if your caching iterator is correct, then you should be passing this test. */
  test("caching iterator") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)

    for (i <- 0 to 999) {
      list(i) = (Row(numberGenerator.nextInt(10000), numberGenerator.nextFloat()))
    }


    val udf: ScalaUdf = new ScalaUdf((sid: Int) => sid + 1, IntegerType, Seq(studentAttributes(0)))

    val result: Iterator[Row] = CachingIteratorGenerator(studentAttributes, udf, Seq(studentAttributes(1)), Seq(), studentAttributes)(list.iterator)

    assert(result.hasNext)

    result.foreach((x: Row) => {
      val inputRow: Row = Row(x.getInt(1) - 1, x.getFloat(0))
      assert(list.contains(inputRow))
    })
  }

  test("caching iterator with testAttributes") {
    val list: ArraySeq[Row] = new ArraySeq[Row](1000)

    for (i <- 0 to 999) {
      list(i) = (Row(numberGenerator.nextInt(10000), numberGenerator.nextInt(10000).toString(), numberGenerator.nextFloat()))
    }


    val udf: ScalaUdf = new ScalaUdf((sid: Int) => sid + 1, IntegerType, Seq(testAttributes(0)))

    val result: Iterator[Row] = CachingIteratorGenerator(testAttributes, udf, Seq(testAttributes(1)), Seq(testAttributes(2)), testAttributes)(list.iterator)

    assert(result.hasNext)

    result.foreach((x: Row) => {
      val inputRow: Row = Row(x.getInt(1) - 1, x.getString(0), x.getFloat(2))
      assert(list.contains(inputRow))
    })
  }

  test("sequence with 1 UDF") {
    val udf: ScalaUdf = new ScalaUdf((i: Int) => i + 1, IntegerType, Seq(studentAttributes(0)))
    val attributes: Seq[Expression] = Seq() ++ studentAttributes ++ Seq(udf)

    assert(CS186Utils.getUdfFromExpressions(attributes) == udf)
  }

  test("sequence with multiple UDFs") {
    val udf1: ScalaUdf = new ScalaUdf((i: Int) => i + 1, IntegerType, Seq(testAttributes(0)))
    val udf2: ScalaUdf = new ScalaUdf((i: String) => i + "1", IntegerType, Seq(testAttributes(1)))
    val udf3: ScalaUdf = new ScalaUdf((i: Float) => i + 1, IntegerType, Seq(testAttributes(2)))
    val attributes: Seq[Expression] = Seq() ++ testAttributes ++ Seq(udf1) ++ Seq(udf2) ++ Seq(udf3)

    assert(CS186Utils.getUdfFromExpressions(attributes) == udf3)
  }
}