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

package org.apache.spark.ml

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.sql.types.DataType

/**
 * :: DeveloperApi ::
 * A fitted model, i.e., a [[Transformer]] produced by an [[Estimator]].
 *
 * @tparam M model type
 */
@DeveloperApi
abstract class Model[M <: Model[M]] extends Transformer {
  /**
   * The parent estimator that produced this model.
   * Note: For ensembles' component Models, this value can be null.
   */
  @transient var parent: Estimator[M] = _

  /**
   * Sets the parent of this model (Java API).
   */
  def setParent(parent: Estimator[M]): M = {
    this.parent = parent
    this.asInstanceOf[M]
  }

  /** Indicates whether this [[Model]] has a corresponding parent. */
  def hasParent: Boolean = parent != null

  override def copy(extra: ParamMap): M
}

/**
  * :: DeveloperApi ::
  * A fitted model, i.e., a [[Transformer]] produced by an [[Estimator]]
  * with a single input and output column
  *
  * @tparam M model type
  */
@DeveloperApi
trait UnaryModel[IN, OUT, M <: Model[M] with UnaryModel[IN, OUT, M]] {

  def createSoftTransformFunc: IN => Option[OUT] = {
    val transformFunc = createTransformFunc
    val validInput = validInputFunc
    in => if (validInput(in)) Some(transformFunc(in)) else None
  }
  protected def validInputFunc: IN => Boolean = _ => true

  /**
    * Creates the transform function using the given param map. The input param map already takes
    * account of the embedded param map. So the param values should be determined solely by the input
    * param map.
    */
  protected def createTransformFunc: (IN) => OUT

  /**
    * Returns the data type of the output column.
    */
  protected def outputDataType: DataType

  val uid: String
}