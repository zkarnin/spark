package org.apache.spark.ml.tuning

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.tuning.ParamMapAdaptiveExplore.SampleType
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.json4s._

/**
  * Created by zkarnin on 9/20/16.
  */
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
  setDefault(trainIndicatorCol -> "trainIndicator")


  val optimizationMethod: Param[String] = new Param[String](this,"optimizationMethod",
    "method for optimization. Either use a fixed array of parameters or explore adaptively",
    Array("fixedArray", "adaptiveGP").contains(_ : String))
  def getOptimizationMethod : String = $(optimizationMethod)
  setDefault(optimizationMethod -> "fixedArray")

}

private[ml] object TrainValidationParams {
  /**
    * implementation of save for [[TrainValidationParams]] .
    * This handles [[TrainValidationParams ]] fields and saves [[Param]] values, but the implementing
    * class needs to handle model data.
    */
  def saveImpl(
                path: String,
                instance: TrainValidationParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit = {
    ValidatorParams.saveImpl(path,instance,sc,extraMetadata)
    /*
      At this point we should save the parameter ParamMapAdaptiveExplorer in some way.
      However, therer is no real reason to save this parameter as it should always be
      created from anew in the code. For this reason nothing is saved an a dummy object
      will be loaded when loading
     */

  }

  /**
    * Generic implementation of load for [[ValidatorParams]] types.
    * This handles all [[ValidatorParams]] fields, but the implementing
    * class needs to handle model data and special [[Param]] values.
    */
  def loadImpl[M <: Model[M]](
     path: String,
     sc: SparkContext,
     expectedClassName: String): (Metadata, Estimator[M], Evaluator, Array[ParamMap], ParamMapAdaptiveExplore) = {
    val (metadata, estimator, evaluator, paramMaps) = ValidatorParams.loadImpl[M](path,sc,expectedClassName)
    /*
      A dummy paramMapAdaptiveExplorer is loaded here since there is no reason to actually store it. If the need
      comes up, some serealization should be implemented here and in saveImpl
    */
    val paramMapAdaptiveExploreBuilder= new ParamMapAdaptiveExploreBuilder
    paramMapAdaptiveExploreBuilder.addParam(new DoubleParam("dummyParent","dummyParam","dummy param"),(1.0,2.0),SampleType.Logscale)
    val paramMapAdaptiveExplore = paramMapAdaptiveExploreBuilder.build()
    (metadata, estimator, evaluator, paramMaps,paramMapAdaptiveExplore)
  }

}
