/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.api.java

import org.apache.spark.sql.catalyst.types.decimal.Decimal

import scala.beans.BeanProperty

import org.scalatest.FunSuite

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.TestSQLContext

// Implicits
import scala.collection.JavaConversions._

class PersonBean extends Serializable {
  @BeanProperty
  var name: String = _

  @BeanProperty
  var age: Int = _
}

class AllTypesBean extends Serializable {
  @BeanProperty var stringField: String = _
  @BeanProperty var intField: java.lang.Integer = _
  @BeanProperty var longField: java.lang.Long = _
  @BeanProperty var floatField: java.lang.Float = _
  @BeanProperty var doubleField: java.lang.Double = _
  @BeanProperty var shortField: java.lang.Short = _
  @BeanProperty var byteField: java.lang.Byte = _
  @BeanProperty var booleanField: java.lang.Boolean = _
  @BeanProperty var dateField: java.sql.Date = _
  @BeanProperty var timestampField: java.sql.Timestamp = _
  @BeanProperty var bigDecimalField: java.math.BigDecimal = _
}

class JavaSQLSuite extends FunSuite {
  val javaCtx = new JavaSparkContext(TestSQLContext.sparkContext)
  val javaSqlCtx = new JavaSQLContext(javaCtx)

  test("schema from JavaBeans") {
    val person = new PersonBean
    person.setName("Michael")
    person.setAge(29)

    val rdd = javaCtx.parallelize(person :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[PersonBean])

    schemaRDD.registerTempTable("people")
    javaSqlCtx.sql("SELECT * FROM people").collect()
  }

  test("schema with null from JavaBeans") {
    val person = new PersonBean
    person.setName("Michael")
    person.setAge(29)

    val rdd = javaCtx.parallelize(person :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[PersonBean])

    schemaRDD.registerTempTable("people")
    val nullRDD = javaSqlCtx.sql("SELECT null FROM people")
    val structFields = nullRDD.schema.getFields()
    assert(structFields.size == 1)
    assert(structFields(0).getDataType().isInstanceOf[NullType])
    assert(nullRDD.collect.head.row === Seq(null))
  }

  test("all types in JavaBeans") {
    val bean = new AllTypesBean
    bean.setStringField("")
    bean.setIntField(0)
    bean.setLongField(0)
    bean.setFloatField(0.0F)
    bean.setDoubleField(0.0)
    bean.setShortField(0.toShort)
    bean.setByteField(0.toByte)
    bean.setBooleanField(false)
    bean.setDateField(java.sql.Date.valueOf("2014-10-10"))
    bean.setTimestampField(java.sql.Timestamp.valueOf("2014-10-10 00:00:00.0"))
    bean.setBigDecimalField(new java.math.BigDecimal(0))

    val rdd = javaCtx.parallelize(bean :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[AllTypesBean])
    schemaRDD.registerTempTable("allTypes")

    assert(
      javaSqlCtx.sql(
        """
          |SELECT stringField, intField, longField, floatField, doubleField, shortField, byteField,
          |       booleanField, dateField, timestampField, bigDecimalField
          |FROM allTypes
        """.stripMargin).collect.head.row ===
      Seq("", 0, 0L, 0F, 0.0, 0.toShort, 0.toByte, false, java.sql.Date.valueOf("2014-10-10"),
        java.sql.Timestamp.valueOf("2014-10-10 00:00:00.0"), scala.math.BigDecimal(0)))
  }

  test("decimal types in JavaBeans") {
    val bean = new AllTypesBean
    bean.setStringField("")
    bean.setIntField(0)
    bean.setLongField(0)
    bean.setFloatField(0.0F)
    bean.setDoubleField(0.0)
    bean.setShortField(0.toShort)
    bean.setByteField(0.toByte)
    bean.setBooleanField(false)
    bean.setDateField(java.sql.Date.valueOf("2014-10-10"))
    bean.setTimestampField(java.sql.Timestamp.valueOf("2014-10-10 00:00:00.0"))
    bean.setBigDecimalField(new java.math.BigDecimal(0))

    val rdd = javaCtx.parallelize(bean :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[AllTypesBean])
    schemaRDD.registerTempTable("decimalTypes")

    assert(javaSqlCtx.sql(
      "select bigDecimalField + bigDecimalField from decimalTypes"
    ).collect.head.row === Seq(scala.math.BigDecimal(0)))
  }

  test("all types null in JavaBeans") {
    val bean = new AllTypesBean
    bean.setStringField(null)
    bean.setIntField(null)
    bean.setLongField(null)
    bean.setFloatField(null)
    bean.setDoubleField(null)
    bean.setShortField(null)
    bean.setByteField(null)
    bean.setBooleanField(null)
    bean.setDateField(null)
    bean.setTimestampField(null)
    bean.setBigDecimalField(null)

    val rdd = javaCtx.parallelize(bean :: Nil)
    val schemaRDD = javaSqlCtx.applySchema(rdd, classOf[AllTypesBean])
    schemaRDD.registerTempTable("allTypes")

    assert(
      javaSqlCtx.sql(
        """
          |SELECT stringField, intField, longField, floatField, doubleField, shortField, byteField,
          |       booleanField, dateField, timestampField, bigDecimalField
          |FROM allTypes
        """.stripMargin).collect.head.row ===
        Seq.fill(11)(null))
  }

  test("loads JSON datasets") {
    val jsonString =
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "bigInteger":92233720368547758070,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }""".replaceAll("\n", " ")
    val rdd = javaCtx.parallelize(jsonString :: Nil)

    var schemaRDD = javaSqlCtx.jsonRDD(rdd)

    schemaRDD.registerTempTable("jsonTable1")

    assert(
      javaSqlCtx.sql("select * from jsonTable1").collect.head.row ===
        Seq(BigDecimal("92233720368547758070"),
            true,
            1.7976931348623157E308,
            10,
            21474836470L,
            null,
            "this is a simple string."))

    val file = getTempFilePath("json")
    val path = file.toString
    rdd.saveAsTextFile(path)
    schemaRDD = javaSqlCtx.jsonFile(path)

    schemaRDD.registerTempTable("jsonTable2")

    assert(
      javaSqlCtx.sql("select * from jsonTable2").collect.head.row ===
        Seq(BigDecimal("92233720368547758070"),
            true,
            1.7976931348623157E308,
            10,
            21474836470L,
            null,
            "this is a simple string."))
  }
}
