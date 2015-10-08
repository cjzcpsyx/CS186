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

package org.apache.spark.mllib.recommendation

import java.lang.{Integer => JavaInteger}

import org.jblas.DoubleMatrix

import org.apache.spark.Logging
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Model representing the result of matrix factorization.
 *
 * Note: If you create the model directly using constructor, please be aware that fast prediction
 * requires cached user/product features and their associated partitioners.
 *
 * @param rank Rank for the features in this model.
 * @param userFeatures RDD of tuples where each tuple represents the userId and
 *                     the features computed for this user.
 * @param productFeatures RDD of tuples where each tuple represents the productId
 *                        and the features computed for this product.
 */
class MatrixFactorizationModel(
    val rank: Int,
    val userFeatures: RDD[(Int, Array[Double])],
    val productFeatures: RDD[(Int, Array[Double])]) extends Serializable with Logging {

  require(rank > 0)
  validateFeatures("User", userFeatures)
  validateFeatures("Product", productFeatures)

  /** Validates factors and warns users if there are performance concerns. */
  private def validateFeatures(name: String, features: RDD[(Int, Array[Double])]): Unit = {
    require(features.first()._2.size == rank,
      s"$name feature dimension does not match the rank $rank.")
    if (features.partitioner.isEmpty) {
      logWarning(s"$name factor does not have a partitioner. "
        + "Prediction on individual records could be slow.")
    }
    if (features.getStorageLevel == StorageLevel.NONE) {
      logWarning(s"$name factor is not cached. Prediction could be slow.")
    }
  }

  /** Predict the rating of one user for one product. */
  def predict(user: Int, product: Int): Double = {
    val userVector = new DoubleMatrix(userFeatures.lookup(user).head)
    val productVector = new DoubleMatrix(productFeatures.lookup(product).head)
    userVector.dot(productVector)
  }

  /**
    * Predict the rating of many users for many products.
    * The output RDD has an element per each element in the input RDD (including all duplicates)
    * unless a user or product is missing in the training set.
    *
    * @param usersProducts  RDD of (user, product) pairs.
    * @return RDD of Ratings.
    */
  def predict(usersProducts: RDD[(Int, Int)]): RDD[Rating] = {
    val users = userFeatures.join(usersProducts).map{
      case (user, (uFeatures, product)) => (product, (user, uFeatures))
    }
    users.join(productFeatures).map {
      case (product, ((user, uFeatures), pFeatures)) =>
        val userVector = new DoubleMatrix(uFeatures)
        val productVector = new DoubleMatrix(pFeatures)
        Rating(user, product, userVector.dot(productVector))
    }
  }

  /**
   * Java-friendly version of [[MatrixFactorizationModel.predict]].
   */
  def predict(usersProducts: JavaPairRDD[JavaInteger, JavaInteger]): JavaRDD[Rating] = {
    predict(usersProducts.rdd.asInstanceOf[RDD[(Int, Int)]]).toJavaRDD()
  }

  /**
   * Recommends products to a user.
   *
   * @param user the user to recommend products to
   * @param num how many products to return. The number returned may be less than this.
   * @return [[Rating]] objects, each of which contains the given user ID, a product ID, and a
   *  "score" in the rating field. Each represents one recommended product, and they are sorted
   *  by score, decreasing. The first returned is the one predicted to be most strongly
   *  recommended to the user. The score is an opaque value that indicates how strongly
   *  recommended the product is.
   */
  def recommendProducts(user: Int, num: Int): Array[Rating] =
    recommend(userFeatures.lookup(user).head, productFeatures, num)
      .map(t => Rating(user, t._1, t._2))

  /**
   * Recommends users to a product. That is, this returns users who are most likely to be
   * interested in a product.
   *
   * @param product the product to recommend users to
   * @param num how many users to return. The number returned may be less than this.
   * @return [[Rating]] objects, each of which contains a user ID, the given product ID, and a
   *  "score" in the rating field. Each represents one recommended user, and they are sorted
   *  by score, decreasing. The first returned is the one predicted to be most strongly
   *  recommended to the product. The score is an opaque value that indicates how strongly
   *  recommended the user is.
   */
  def recommendUsers(product: Int, num: Int): Array[Rating] =
    recommend(productFeatures.lookup(product).head, userFeatures, num)
      .map(t => Rating(t._1, product, t._2))

  private def recommend(
      recommendToFeatures: Array[Double],
      recommendableFeatures: RDD[(Int, Array[Double])],
      num: Int): Array[(Int, Double)] = {
    val recommendToVector = new DoubleMatrix(recommendToFeatures)
    val scored = recommendableFeatures.map { case (id,features) =>
      (id, recommendToVector.dot(new DoubleMatrix(features)))
    }
    scored.top(num)(Ordering.by(_._2))
  }
}
