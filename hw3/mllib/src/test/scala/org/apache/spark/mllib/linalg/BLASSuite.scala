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

package org.apache.spark.mllib.linalg

import org.scalatest.FunSuite

import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.mllib.linalg.BLAS._

class BLASSuite extends FunSuite {

  test("copy") {
    val sx = Vectors.sparse(4, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0, 0.0)
    val sy = Vectors.sparse(4, Array(0, 1, 3), Array(2.0, 1.0, 1.0))
    val dy = Array(2.0, 1.0, 0.0, 1.0)

    val dy1 = Vectors.dense(dy.clone())
    copy(sx, dy1)
    assert(dy1 ~== dx absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    copy(dx, dy2)
    assert(dy2 ~== dx absTol 1e-15)

    intercept[IllegalArgumentException] {
      copy(sx, sy)
    }

    intercept[IllegalArgumentException] {
      copy(dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        copy(sx, Vectors.dense(0.0, 1.0, 2.0))
      }
    }
  }

  test("scal") {
    val a = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)

    scal(a, sx)
    assert(sx ~== Vectors.sparse(3, Array(0, 2), Array(0.1, -0.2)) absTol 1e-15)

    scal(a, dx)
    assert(dx ~== Vectors.dense(0.1, 0.0, -0.2) absTol 1e-15)
  }

  test("axpy") {
    val alpha = 0.1
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val dy = Array(2.0, 1.0, 0.0)
    val expected = Vectors.dense(2.1, 1.0, -0.2)

    val dy1 = Vectors.dense(dy.clone())
    axpy(alpha, sx, dy1)
    assert(dy1 ~== expected absTol 1e-15)

    val dy2 = Vectors.dense(dy.clone())
    axpy(alpha, dx, dy2)
    assert(dy2 ~== expected absTol 1e-15)

    val sy = Vectors.sparse(4, Array(0, 1), Array(2.0, 1.0))

    intercept[IllegalArgumentException] {
      axpy(alpha, sx, sy)
    }

    intercept[IllegalArgumentException] {
      axpy(alpha, dx, sy)
    }

    withClue("vector sizes must match") {
      intercept[Exception] {
        axpy(alpha, sx, Vectors.dense(1.0, 2.0))
      }
    }
  }

  test("dot") {
    val sx = Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0))
    val dx = Vectors.dense(1.0, 0.0, -2.0)
    val sy = Vectors.sparse(3, Array(0, 1), Array(2.0, 1.0))
    val dy = Vectors.dense(2.0, 1.0, 0.0)

    assert(dot(sx, sy) ~== 2.0 absTol 1e-15)
    assert(dot(sy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(sx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, sx) ~== 2.0 absTol 1e-15)
    assert(dot(dx, dy) ~== 2.0 absTol 1e-15)
    assert(dot(dy, dx) ~== 2.0 absTol 1e-15)

    assert(dot(sx, sx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(sx, dx) ~== 5.0 absTol 1e-15)
    assert(dot(dx, sx) ~== 5.0 absTol 1e-15)

    val sx1 = Vectors.sparse(10, Array(0, 3, 5, 7, 8), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    val sx2 = Vectors.sparse(10, Array(1, 3, 6, 7, 9), Array(1.0, 2.0, 3.0, 4.0, 5.0))
    assert(dot(sx1, sx2) ~== 20.0 absTol 1e-15)
    assert(dot(sx2, sx1) ~== 20.0 absTol 1e-15)

    withClue("vector sizes must match") {
      intercept[Exception] {
        dot(sx, Vectors.dense(2.0, 1.0))
      }
    }
  }

  test("gemm") {

    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val B = new DenseMatrix(3, 2, Array(1.0, 0.0, 0.0, 0.0, 2.0, 1.0))
    val expected = new DenseMatrix(4, 2, Array(0.0, 1.0, 0.0, 0.0, 4.0, 0.0, 2.0, 3.0))

    assert(dA multiply B ~== expected absTol 1e-15)
    assert(sA multiply B ~== expected absTol 1e-15)

    val C1 = new DenseMatrix(4, 2, Array(1.0, 0.0, 2.0, 1.0, 0.0, 0.0, 1.0, 0.0))
    val C2 = C1.copy
    val C3 = C1.copy
    val C4 = C1.copy
    val C5 = C1.copy
    val C6 = C1.copy
    val C7 = C1.copy
    val C8 = C1.copy
    val expected2 = new DenseMatrix(4, 2, Array(2.0, 1.0, 4.0, 2.0, 4.0, 0.0, 4.0, 3.0))
    val expected3 = new DenseMatrix(4, 2, Array(2.0, 2.0, 4.0, 2.0, 8.0, 0.0, 6.0, 6.0))

    gemm(1.0, dA, B, 2.0, C1)
    gemm(1.0, sA, B, 2.0, C2)
    gemm(2.0, dA, B, 2.0, C3)
    gemm(2.0, sA, B, 2.0, C4)
    assert(C1 ~== expected2 absTol 1e-15)
    assert(C2 ~== expected2 absTol 1e-15)
    assert(C3 ~== expected3 absTol 1e-15)
    assert(C4 ~== expected3 absTol 1e-15)

    withClue("columns of A don't match the rows of B") {
      intercept[Exception] {
        gemm(true, false, 1.0, dA, B, 2.0, C1)
      }
    }

    val dAT =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sAT =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    assert(dAT transposeMultiply B ~== expected absTol 1e-15)
    assert(sAT transposeMultiply B ~== expected absTol 1e-15)

    gemm(true, false, 1.0, dAT, B, 2.0, C5)
    gemm(true, false, 1.0, sAT, B, 2.0, C6)
    gemm(true, false, 2.0, dAT, B, 2.0, C7)
    gemm(true, false, 2.0, sAT, B, 2.0, C8)
    assert(C5 ~== expected2 absTol 1e-15)
    assert(C6 ~== expected2 absTol 1e-15)
    assert(C7 ~== expected3 absTol 1e-15)
    assert(C8 ~== expected3 absTol 1e-15)
  }

  test("gemv") {

    val dA =
      new DenseMatrix(4, 3, Array(0.0, 1.0, 0.0, 0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 3.0))
    val sA = new SparseMatrix(4, 3, Array(0, 1, 3, 4), Array(1, 0, 2, 3), Array(1.0, 2.0, 1.0, 3.0))

    val x = new DenseVector(Array(1.0, 2.0, 3.0))
    val expected = new DenseVector(Array(4.0, 1.0, 2.0, 9.0))

    assert(dA multiply x ~== expected absTol 1e-15)
    assert(sA multiply x ~== expected absTol 1e-15)

    val y1 = new DenseVector(Array(1.0, 3.0, 1.0, 0.0))
    val y2 = y1.copy
    val y3 = y1.copy
    val y4 = y1.copy
    val y5 = y1.copy
    val y6 = y1.copy
    val y7 = y1.copy
    val y8 = y1.copy
    val expected2 = new DenseVector(Array(6.0, 7.0, 4.0, 9.0))
    val expected3 = new DenseVector(Array(10.0, 8.0, 6.0, 18.0))

    gemv(1.0, dA, x, 2.0, y1)
    gemv(1.0, sA, x, 2.0, y2)
    gemv(2.0, dA, x, 2.0, y3)
    gemv(2.0, sA, x, 2.0, y4)
    assert(y1 ~== expected2 absTol 1e-15)
    assert(y2 ~== expected2 absTol 1e-15)
    assert(y3 ~== expected3 absTol 1e-15)
    assert(y4 ~== expected3 absTol 1e-15)
    withClue("columns of A don't match the rows of B") {
      intercept[Exception] {
        gemv(true, 1.0, dA, x, 2.0, y1)
      }
    }

    val dAT =
      new DenseMatrix(3, 4, Array(0.0, 2.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 3.0))
    val sAT =
      new SparseMatrix(3, 4, Array(0, 1, 2, 3, 4), Array(1, 0, 1, 2), Array(2.0, 1.0, 1.0, 3.0))

    assert(dAT transposeMultiply x ~== expected absTol 1e-15)
    assert(sAT transposeMultiply x ~== expected absTol 1e-15)

    gemv(true, 1.0, dAT, x, 2.0, y5)
    gemv(true, 1.0, sAT, x, 2.0, y6)
    gemv(true, 2.0, dAT, x, 2.0, y7)
    gemv(true, 2.0, sAT, x, 2.0, y8)
    assert(y5 ~== expected2 absTol 1e-15)
    assert(y6 ~== expected2 absTol 1e-15)
    assert(y7 ~== expected3 absTol 1e-15)
    assert(y8 ~== expected3 absTol 1e-15)
  }
}
