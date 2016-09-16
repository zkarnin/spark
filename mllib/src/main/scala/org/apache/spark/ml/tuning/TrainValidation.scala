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

package org.apache.spark.ml.tuning

import java.util.{List => JList}

import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{ParamValidators, IntParam, Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._
import scala.language.existentials


/**
  * Params for [[TrainValidation]] and [[TrainValidationModel]].
  */
private[ml] trait TrainValidationParams extends ValidatorParams {
  /**
    * Budget of iterations. In each iteration a model (or several) is trained
    * Default: 30
    *
    * @group param
    */
  val seqIterNum: IntParam = new IntParam(this, "seqIterNum",
    "Budget of iterations. In each iteration a model (or several) is trained", ParamValidators.inRange(1, 10000))
  /** @group getParam */
  def getSeqIterNum: Int = $(seqIterNum)
  setDefault(seqIterNum -> 30)

  /**
    * Number of models trained in each iteration
    * Default: 1
    *
    * @group param
    */
  val modelsInOneIter: IntParam = new IntParam(this, "modelsInOneIter",
    "Number of models trained in each iteration", ParamValidators.inRange(1, 10000))
  /** @group getParam */
  def getModelsInOneIter: Int = $(modelsInOneIter)
  setDefault(modelsInOneIter -> 1)

  val paramMapAdaptiveExplore: Param[ParamMapAdaptiveExplore] =
    new Param[ParamMapAdaptiveExplore](this,
      "paramMapGPExplore",
      "object used for hyper-parameter choosing based on gaussian processes"
    )
  def getParamMapAdaptiveExplore: ParamMapAdaptiveExplore = $(paramMapAdaptiveExplore)


  /**
    * Name of column with data about original of instance, train or validation
    *
    * @group param
    */
  val trainIndicatorCol: Param[String] = new Param[String](this, "trainIndicatorCol",
    "name of column with boolean indicating whether the instance is a train instance (otherwise validation)")
  /** @group getParam */
  def getTrainIndicatorCol: String = $(trainIndicatorCol)

  val optimizationMethod: Param[String] = new Param[String](this,"optimizationMethod",
  "method for optimization. Either use a fixed array of parameters or explore adaptively",
    Array("fixedArray", "adaptiveGP").contains(_ : String))
  def getOptimizationMethod : String = $(optimizationMethod)
}

/**
 * Validation for hyper-parameter tuning.
 * Accepts a dataset with column containing for each item whether it is in hte train
 * or validation set. Uses evaluation metric on the validation set to select the best model.
 * Similar to [[org.apache.spark.ml.tuning.CrossValidator]], but only splits the set once.
 */
class TrainValidation @Since("1.5.0")(@Since("1.5.0") override val uid: String)
  extends Estimator[TrainValidationModel]
  with TrainValidationParams with MLWritable with Logging {

  def this() = this(Identifiable.randomUID("tvs"))

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  def setTrainIndicatorCol(value: String) : this.type = set(trainIndicatorCol, value)

  /** @group setParam */
  def setParamMapAdaptiveExplore(value: ParamMapAdaptiveExplore): this.type = set(paramMapAdaptiveExplore, value)

  /** @group setParam */
  def setModelsInOneIter(value: Int): this.type = set(modelsInOneIter, value)

  /** @group setParam */
  def setSeqIterNum(value: Int): this.type = set(seqIterNum, value)

  protected def fitWithAdaptiveExplore(trainingDataset: Dataset[_], validationDataset: Dataset[_],
                    schema: StructType): TrainValidationModel = {
    transformSchema(schema, logging = true)
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = $(seqIterNum) * $(modelsInOneIter)
    val metrics = new Array[Double](numModels)
    val evalMult = if (eval.isLargerBetter) -1 else 1
    var bestMetric: Double = Double.MaxValue
    var bestParamMap : ParamMap = null

    trainingDataset.cache()
    validationDataset.cache()

    for (j <- 0 until $(seqIterNum)) {
      val paramMaps: Array[ParamMap] = getParamMapAdaptiveExplore.getNextK(getModelsInOneIter)
      val startTime = System.nanoTime()
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      val logTimeForTraining = Math.log((System.nanoTime() - startTime) / 1000)
      for (i <- 0 until $(modelsInOneIter)) {
        val metric =
          eval.evaluate(models(i).transform(validationDataset, paramMaps(i))) * evalMult
        logDebug(s"Got metric $metric for model trained with ${paramMaps(i)}.")
        metrics(j * $(modelsInOneIter) + i) = metric
        getParamMapAdaptiveExplore.update(paramMaps(i), metric, logTimeForTraining)
        if (bestMetric > metric) {
          bestMetric=metric
          bestParamMap = paramMaps(i)
        }
      }
    }

    trainingDataset.unpersist()
    validationDataset.unpersist()

    logInfo(s"validation metrics: ${metrics.toSeq}")
    logInfo(s"Best set of parameters:\n$bestParamMap")
    logInfo(s"Best train validation metric: $bestMetric.")
    val bestModel = est.fit(trainingDataset, bestParamMap).asInstanceOf[Model[_]]
    copyValues(new TrainValidationModel(uid, bestModel, metrics).setParent(this))
  }

  protected def fitWithParamMapArray(trainingDataset: Dataset[_], validationDataset: Dataset[_],
                    schema: StructType): TrainValidationModel = {
    transformSchema(schema, logging = true)
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)

    trainingDataset.cache()
    validationDataset.cache()

    // multi-model training
    logDebug(s"User defined train and validation with multiple sets of parameters.")
    val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
    trainingDataset.unpersist()
    var i = 0
    while (i < numModels) {
      // TODO: duplicate evaluator to take extra params from input
      val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
      logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
      metrics(i) += metric
      i += 1
    }
    validationDataset.unpersist()

    logInfo(s"Train validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best train validation metric: $bestMetric.")
    val bestModel = est.fit(trainingDataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new TrainValidationModel(uid, bestModel, metrics).setParent(this))
  }

  override def fit(dataSet: Dataset[_]): TrainValidationModel = {
    val schema = dataSet.schema
    val trainingDataset = dataSet.where(s"$getTrainIndicatorCol = true")
    val validationDataset = dataSet.where(s"$getTrainIndicatorCol = false")
    fitWithParamMapArray(trainingDataset,validationDataset,schema)
  }

  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  override def copy(extra: ParamMap): TrainValidation = {
    val copied = defaultCopy(extra).asInstanceOf[TrainValidation]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  override def write: MLWriter = new TrainValidation.TrainValidationWriter(this)
}

object TrainValidation extends MLReadable[TrainValidation] {

  override def read: MLReader[TrainValidation] = new TrainValidationReader

  override def load(path: String): TrainValidation = super.load(path)

  private[TrainValidation] class TrainValidationWriter(instance: TrainValidation)
    extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class TrainValidationReader extends MLReader[TrainValidation] {

    /** Checked against metadata when loading model */
    private val className = classOf[TrainValidation].getName

    override def load(path: String): TrainValidation = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val trainIndicatorCol = (metadata.params \ "trainIndicatorCol").extract[String]
      val seed = (metadata.params \ "seed").extract[Long]
      new TrainValidation(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
        .setTrainIndicatorCol(trainIndicatorCol)
        .setSeed(seed)
    }
  }
}

/**
 * Model from train validation split.
 *
 * @param uid Id.
 * @param bestModel Estimator determined best model.
 * @param validationMetrics Evaluated validation metrics.
 */
class TrainValidationModel private[ml](
    override val uid: String,
    val bestModel: Model[_],
    val validationMetrics: Array[Double])
  extends Model[TrainValidationModel] with TrainValidationParams with MLWritable {

  /** A Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, bestModel: Model[_], validationMetrics: JList[Double]) = {
    this(uid, bestModel, validationMetrics.asScala.toArray)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  override def copy(extra: ParamMap): TrainValidationModel = {
    val copied = new TrainValidationModel (
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      validationMetrics.clone())
    copyValues(copied, extra)
  }

  override def write: MLWriter = new TrainValidationModel.TrainValidationModelWriter(this)
}

object TrainValidationModel extends MLReadable[TrainValidationModel] {

  override def read: MLReader[TrainValidationModel] = new TrainValidationModelReader

  override def load(path: String): TrainValidationModel = super.load(path)

  private[TrainValidationModel]
  class TrainValidationModelWriter(instance: TrainValidationModel) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "validationMetrics" -> instance.validationMetrics.toSeq
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class TrainValidationModelReader extends MLReader[TrainValidationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[TrainValidationModel].getName

    override def load(path: String): TrainValidationModel = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val trainIndicatorCol = (metadata.params \ "trainIndicatorCol").extract[String]
      val seed = (metadata.params \ "seed").extract[Long]
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val validationMetrics = (metadata.metadata \ "validationMetrics").extract[Seq[Double]].toArray
      val model = new TrainValidationModel(metadata.uid, bestModel, validationMetrics)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
        .set(model.trainIndicatorCol, trainIndicatorCol)
        .set(model.seed, seed)
    }
  }
}
