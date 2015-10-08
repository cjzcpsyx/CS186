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

package org.apache.spark.sql.parquet

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.scalatest.FunSuite
import parquet.schema.MessageTypeParser

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.sql.test.TestSQLContext

class ParquetSchemaSuite extends FunSuite with ParquetTest {
  val sqlContext = TestSQLContext

  /**
   * Checks whether the reflected Parquet message type for product type `T` conforms `messageType`.
   */
  private def testSchema[T <: Product: ClassTag: TypeTag](
      testName: String, messageType: String): Unit = {
    test(testName) {
      val actual = ParquetTypesConverter.convertFromAttributes(ScalaReflection.attributesFor[T])
      val expected = MessageTypeParser.parseMessageType(messageType)
      actual.checkContains(expected)
      expected.checkContains(actual)
    }
  }

  testSchema[(Boolean, Int, Long, Float, Double, Array[Byte])](
    "basic types",
    """
      |message root {
      |  required boolean _1;
      |  required int32   _2;
      |  required int64   _3;
      |  required float   _4;
      |  required double  _5;
      |  optional binary  _6;
      |}
    """.stripMargin)

  testSchema[(Byte, Short, Int, Long)](
    "logical integral types",
    """
      |message root {
      |  required int32 _1 (INT_8);
      |  required int32 _2 (INT_16);
      |  required int32 _3 (INT_32);
      |  required int64 _4 (INT_64);
      |}
    """.stripMargin)

  // Currently String is the only supported logical binary type.
  testSchema[Tuple1[String]](
    "binary logical types",
    """
      |message root {
      |  optional binary _1 (UTF8);
      |}
    """.stripMargin)

  testSchema[Tuple1[Seq[Int]]](
    "array",
    """
      |message root {
      |  optional group _1 (LIST) {
      |    repeated int32 array;
      |  }
      |}
    """.stripMargin)

  testSchema[Tuple1[Map[Int, String]]](
    "map",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional binary value (UTF8);
      |    }
      |  }
      |}
    """.stripMargin)

  testSchema[Tuple1[Pair[Int, String]]](
    "struct",
    """
      |message root {
      |  optional group _1 {
      |    required int32 _1;
      |    optional binary _2 (UTF8);
      |  }
      |}
    """.stripMargin)

  testSchema[Tuple1[Map[Int, (String, Seq[(Int, Double)])]]](
    "deeply nested type",
    """
      |message root {
      |  optional group _1 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional group value {
      |        optional binary _1 (UTF8);
      |        optional group _2 (LIST) {
      |          repeated group bag {
      |            optional group array {
      |              required int32 _1;
      |              required double _2;
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin)

  testSchema[(Option[Int], Map[Int, Option[Double]])](
    "optional types",
    """
      |message root {
      |  optional int32 _1;
      |  optional group _2 (MAP) {
      |    repeated group map (MAP_KEY_VALUE) {
      |      required int32 key;
      |      optional double value;
      |    }
      |  }
      |}
    """.stripMargin)

  test("DataType string parser compatibility") {
    val schema = StructType(List(
      StructField("c1", IntegerType, false),
      StructField("c2", BinaryType, true)))

    val fromCaseClassString = ParquetTypesConverter.convertFromString(schema.toString)
    val fromJson = ParquetTypesConverter.convertFromString(schema.json)

    (fromCaseClassString, fromJson).zipped.foreach { (a, b) =>
      assert(a.name == b.name)
      assert(a.dataType === b.dataType)
      assert(a.nullable === b.nullable)
    }
  }
}
