package org.apache.spark.ml.tuning

import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning.ParamMapAdaptiveExplore.{SampleType, ParamTransformer}
import scala.collection.mutable

/**
  * Created by zkarnin on 6/15/16.
  */
class ParamMapAdaptiveExploreBuilder extends Serializable {
  private val map = mutable.Map[Param[Any], ParamTransformer[Any]]()
  private val lossLengthScaleMap = mutable.Map[Param[Any],Double]()
  private val logCostLengthScaleMap = mutable.Map[Param[Any],Double]()
  private var lossPriorMean :Array[Double]=> Double = null
  private var logCostPriorMean :Array[Double]=> Double = null
  private var logCostLambda : Double = -1.0

  /*
      The kernel is not fixed but
      rather determined by the lengthScales: it is defined as
      K(x,y) = exp(-|transpose(x-y) * D_{lengthScales} * (x-y)|)
      with D being a diagonal matrix determined by the lengthScales values.
      Intuitively, the more sensitive a hyperparameter should be, the larger
      its lengthScale should be
   */
  private val defaultLossLengthScale = 1.0
  private val defaultLogCostLengthScale = 1.0

  /**
    * add categorical paramater for the exploration grid.
    * Note: Currently supports only two possible categories
    *
    * @param param the parameter to be explored
    * @param values possible values of the parameter
    */
  def addParam[T](param: Param[T],
                  values: Array[T]): Unit = {
    addParam(param,values,defaultLossLengthScale,defaultLogCostLengthScale)
  }

  /**
    * add categorical paramater for the exploration grid.
    * Note: Currently supports only two possible categories
    *
    * @param param the parameter to be explored
    * @param values possible values of the parameter
    * @param lossLengthScale Determine the strength of the prior and the previous
    *                        examples on the estimate for the loss.
    *                        large means strong, small means weak.
    * @param logCostLengthScale same as lossLengthScale but for the computation time
    */
  def addParam[T](param: Param[T],
                  values: Array[T],
                  lossLengthScale : Double,
                  logCostLengthScale : Double): Unit = {
    assert(values != null && values.nonEmpty, "illegal value set given: values cannot be empty")
    assert(values.length <= 2, "Currently, only two possible values are supported")
    val double2value = { (x: Double) =>
      assert(x >= 0 && x <= 1)
      if (Math.round(x) == 0) values(0) else values(1)
    }
    val value2double = { (x: T) =>
      assert(values.contains(x))
      if (x.equals(values(0))) 0.0 else 1.0
    }
    val transformer = ParamTransformer[T](value2double, double2value)
    put(param, transformer)

    lossLengthScaleMap.put(param.asInstanceOf[Param[Any]],lossLengthScale)
    logCostLengthScaleMap.put(param.asInstanceOf[Param[Any]],logCostLengthScale)
  }

  private def put(paramAndTransformer: (Param[_], ParamTransformer[_])): Unit = {
    map.put(paramAndTransformer._1.asInstanceOf[Param[Any]],
      paramAndTransformer._2.asInstanceOf[ParamTransformer[Any]])
  }

  /**
    * add an integer paramater for the exploration grid.
    *
    * @param param the parameter to be explored
    * @param intRange range for its possible values (both ends are included)
    * @param sampleType manner in which sample is made. Supported values are
    *                   Uniform and log-scale
    */
  def addParam(param: IntParam, intRange: (Int, Int),
               sampleType: SampleType.Value): Unit = {
    addParam(param,intRange,sampleType,defaultLossLengthScale,defaultLogCostLengthScale)
  }

  /**
    * add an integer paramater for the exploration grid.
    *
    * @param param the parameter to be explored
    * @param intRange range for its possible values (both ends are included)
    * @param sampleType manner in which sample is made. Supported values are
    *                   Uniform and log-scale
    * @param lossLengthScale Determine the strength of the prior and the previous
    *                        examples on the estimate for the loss.
    *                        large value means its a linear function (strong prior).
    *                        small value means its an unknown function (weak prior).
    * @param logCostLengthScale same as lossLengthScale but for the computation time
    */
  def addParam(param: IntParam, intRange: (Int, Int),
               sampleType: SampleType.Value,
               lossLengthScale : Double,
               logCostLengthScale : Double): Unit = {
    assert(intRange._2 > intRange._1,s"illegal range given: (${intRange._1},${intRange._2})")
    if (sampleType == SampleType.Logscale) {
      assert(intRange._1 > 0)
    }
    val range = (intRange._1-0.499,intRange._2+0.499)

    val logRange1 = Math.log(range._1)
    val logRange2 = Math.log(range._2)

    val double2value = if (sampleType == SampleType.Uniform) {
      (x: Double) => {
        assert(x >= 0 && x <= 1)
        val asDouble = x * (range._2 - range._1) + range._1
        asDouble.round.toInt
      }
    } else /* sample type is random */ {
      (x: Double) => {
        assert(x >= 0 && x <= 1)
        val asDouble = x * (logRange2 - logRange1) + logRange1
        Math.exp(asDouble).round.toInt
      }
    }
    val value2double = {
      if (sampleType == SampleType.Uniform) {
        (x: Int) => {
          assert(x >= range._1 && x <= range._2,s"expected value between ${range._1} and ${range._2}. Value given is $x")
          (x.toDouble - range._1) / (range._2 - range._1)
        }
      } else {
        (x: Int) => {
          assert(x >= range._1 && x <= range._2,s"expected value between ${range._1} and ${range._2}. Value given is $x")
          (Math.log(x) - logRange1) / (logRange2 - logRange1)
        }
      }
    }
    val transformer = ParamTransformer(value2double, double2value)
    put(param, transformer)

    lossLengthScaleMap.put(param.asInstanceOf[Param[Any]],lossLengthScale)
    logCostLengthScaleMap.put(param.asInstanceOf[Param[Any]],logCostLengthScale)
  }

  /**
    * add a double paramater for the exploration grid.
    *
    * @param param the parameter to be explored
    * @param range range for its possible values (both ends are included)
    * @param sampleType manner in which sample is made. Supported values are
    *                   Uniform and log-scale
    */
  def addParam(param: DoubleParam, range: (Double, Double),
               sampleType: SampleType.Value): Unit = {
    addParam(param,range,sampleType,defaultLossLengthScale,defaultLogCostLengthScale)
  }

  /**
    * add a Double paramater for the exploration grid.
    *
    * @param param the parameter to be explored
    * @param range range for its possible values (both ends are included)
    * @param sampleType manner in which sample is made. Supported values are
    *                   Uniform and log-scale
    * @param lossLengthScale Determine the strength of the prior and the previous
    *                        examples on the estimate for the loss.
    *                        large value means its a linear function (strong prior).
    *                        small value means its an unknown function (weak prior).
    * @param logCostLengthScale same as lossLengthScale but for the computation time
    */
  def addParam(param: DoubleParam, range: (Double, Double),
               sampleType: SampleType.Value,
               lossLengthScale : Double,
               logCostLengthScale : Double): Unit = {
    assert(range._2 > range._1)
    if (sampleType == SampleType.Logscale) {
      assert(range._1 > 0)
    }
    val logRange1 = Math.log(range._1)
    val logRange2 = Math.log(range._2)

    val double2value = if (sampleType == SampleType.Uniform) {
      (x: Double) => {
        assert(x >= 0 && x <= 1)
        x * (range._2 - range._1) + range._1
      }
    } else /* sample type is random */ {
      (x: Double) => {
        assert(x >= 0 && x <= 1)
        val asDouble = x * (logRange2 - logRange1) + logRange1
        Math.exp(asDouble)
      }
    }
    val value2double = {
      if (sampleType == SampleType.Uniform) {
        (x: Double) => {
          // due to numerical errors there could be minor overflow
          assert(x >= range._1*0.99 && x <= range._2*1.01,s"expected value between ${range._1} and ${range._2}. Value given is $x")
          val fixed = if (x > range._2) range._2 else if (x < range._1) range._1 else x
          (fixed - range._1) / (range._2 - range._1)
        }
      } else {
        (x: Double) => {
          // due to numerical errors there could be minor overflow
          assert(x >= range._1*0.99 && x <= range._2*1.01,s"expected value between ${range._1} and ${range._2}. Value given is $x")
          val fixed = if (x > range._2) range._2 else if (x < range._1) range._1 else x
          (Math.log(fixed) - logRange1) / (logRange2 - logRange1)
        }
      }
    }
    val transformer = ParamTransformer(value2double, double2value)
    put(param, transformer)

    lossLengthScaleMap.put(param.asInstanceOf[Param[Any]],lossLengthScale)
    logCostLengthScaleMap.put(param.asInstanceOf[Param[Any]],logCostLengthScale)
  }

  /**
    * add an integer (Long) paramater for the exploration grid.
    *
    * @param param the parameter to be explored
    * @param range range for its possible values (both ends are included)
    * @param sampleType manner in which sample is made. Supported values are
    *                   Uniform and log-scale
    */
  def addParam(param: LongParam, range: (Long, Long),
               sampleType: SampleType.Value): Unit = {
    addParam(param,range,sampleType,defaultLossLengthScale,defaultLogCostLengthScale)
  }

  /**
    * add an integer (Long) paramater for the exploration grid.
    *
    * @param param the parameter to be explored
    * @param range range for its possible values (both ends are included)
    * @param sampleType manner in which sample is made. Supported values are
    *                   Uniform and log-scale
    * @param lossLengthScale Determine the strength of the prior and the previous
    *                        examples on the estimate for the loss.
    *                        large value means its a linear function (strong prior).
    *                        small value means its an unknown function (weak prior).
    * @param logCostLengthScale same as lossLengthScale but for the computation time
    */
  def addParam(param: LongParam, range: (Long, Long),
               sampleType: SampleType.Value,
               lossLengthScale : Double,
               logCostLengthScale : Double): Unit = {
    assert(range._2 > range._1)
    if (sampleType == SampleType.Logscale) {
      assert(range._1 > 0)
    }
    val logRange1 = Math.log(range._1)
    val logRange2 = Math.log(range._2)

    val double2value = if (sampleType == SampleType.Uniform) {
      (x: Double) => {
        assert(x >= 0 && x <= 1)
        val asDouble = x * (range._2 - range._1) + range._1
        asDouble.round
      }
    } else /* sample type is random */ {
      (x: Double) => {
        assert(x >= 0 && x <= 1)
        val asDouble = x * (logRange2 - logRange1) + logRange1
        Math.exp(asDouble).round
      }
    }
    val value2double = {
      if (sampleType == SampleType.Uniform) {
        (x: Long) => {
          assert(x >= range._1 && x <= range._2, s"expected value between ${range._1} and ${range._2}. Value given is $x")
          (x.toDouble - range._1) / (range._2 - range._1)
        }
      } else {
        (x: Long) => {
          assert(x >= range._1 && x <= range._2, s"expected value between ${range._1} and ${range._2}. Value given is $x")
          (Math.log(x) - logRange1) / (logRange2 - logRange1)
        }
      }
    }
    val transformer = ParamTransformer(value2double, double2value)
    put(param, transformer)

    lossLengthScaleMap.put(param.asInstanceOf[Param[Any]],lossLengthScale)
    logCostLengthScaleMap.put(param.asInstanceOf[Param[Any]],logCostLengthScale)
  }

  /**
    * Adds parameter ranges from another builder.
    * Useful for adding default parameters from existing Evaluators
    */
  def addParamsFromOther(other:ParamMapAdaptiveExploreBuilder) = {
    other.map.foreach{case (param, transformer) =>
      put(param,transformer)
      lossLengthScaleMap.put(param,other.lossLengthScaleMap.get(param).get)
      logCostLengthScaleMap.put(param,other.logCostLengthScaleMap.get(param).get)
    }
  }

  /**
    * Set a prior for the loss of the training given the
    * hyperparameters since we are working with a distance based kernel,
    * the prior's values should be given as a function (mu) from a parameter
    * map to the loss values.
    *
    * @param mu           function reflecting the prior values
    */
  def setPrior(mu: ParamMap => Double = (pm:ParamMap) => 0.0
              ): Unit = {
    lossPriorMean = {doubles : Array[Double]=> mu(doubleArray2paramMap(doubles))}
  }

  /**
    * Same as setPrior, but the value is not the loss but log of the computation time
    */
  def setLogComputationPrior(mu: ParamMap => Double = (pm:ParamMap) => 0.0,
                             lengthScales: Array[Double] = (0 until map.size).map(x => 0.1).toArray,
                             lambda: Double = logCostLambda
                            ): Unit = {
    logCostPriorMean = {doubles : Array[Double]=> mu(doubleArray2paramMap(doubles))}
    logCostLambda = lambda
  }


  private def doubleArray2paramMap(doubles: Array[Double]): ParamMap = {
    assert(doubles.length == map.size)
    val ret = new ParamMap()
    map.iterator.zip(doubles.iterator)
      .foreach { case ((param, paramFunc), dValue) =>
        val value = paramFunc.doubleToValue(dValue)
        ret.put(param -> value)
      }
    ret
  }


  private def paramDoubleMap2DoubleArray(paramDoubleMap: mutable.Map[Param[Any],Double],
                                         defaultValue: Double
                                        ):Array[Double] = {
    map.map{case (param,transformer) =>
      paramDoubleMap.getOrElse(param,defaultValue)
    }.toArray
  }

  /**
    * Build the ParamMapAdaptiveExplore object
    */
  def build() : ParamMapAdaptiveExplore = {
    val gpOptimizer: BayesOptimize = new BayesOptimize(map.size)
    gpOptimizer.setPrior(
      if (lossPriorMean!= null) lossPriorMean else gpOptimizer.lossPriorMean,
      paramDoubleMap2DoubleArray(lossLengthScaleMap,1.0))
    gpOptimizer.setLogComputationPrior(
      if (logCostPriorMean!= null) logCostPriorMean else gpOptimizer.logCostPriorMean,
      paramDoubleMap2DoubleArray(logCostLengthScaleMap,0.1),
      if (logCostLambda > 0.0) logCostLambda else gpOptimizer.logCostPriorLambda)
    new ParamMapAdaptiveExplore(map.toMap,gpOptimizer)
  }
}


class ParamMapAdaptiveExplore(val map: Map[Param[Any],ParamTransformer[Any]],
                              val gpOptimizer: BayesOptimize
                             ) extends Serializable {


  /**
    * Get the next k parameter maps to use.
    * These are chosen in order to maximize the expected gain per computation.
    * Practically, if we have the ability to train k models in parallel this
    * gives k different parameter maps to train with.
    *
    * @param k number of parameter maps to use
    * @return
    */
  def getNextK(k: Int): Array[ParamMap] = {
    val nextKAsDoubles: Array[Array[Double]] = gpOptimizer.getNextN(k)
    nextKAsDoubles.map { curDoubleParams =>
      doubleArray2paramMap(curDoubleParams)
    }
  }

  private def doubleArray2paramMap(doubles: Array[Double]): ParamMap = {
    assert(doubles.length == map.size)
    val ret = new ParamMap()
    map.iterator.zip(doubles.iterator)
      .foreach { case ((param, paramFunc), dValue) =>
        val value = paramFunc.doubleToValue(dValue)
        ret.put(param -> value)
      }
    ret
  }

  /**
    * Update the GP about a query to the model training with a certain
    * parameter map.
    *
    * @param paramMap the parameter map used
    * @param loss     the loss associated with it
    * @param logTime  log of the running time (in seconds)
    */
  def update(paramMap: ParamMap, loss: Double, logTime: Double): Unit = {
    val dataPoint = paramMap2doubleArray(paramMap)
    println("\n\n---------------------")
    val paramDescription = map.keys.map{param=>
      param.name+"="+paramMap.get(param).toString
    }.mkString(",")
    println(s"got feedback for "+paramDescription)
    println(s"loss=$loss, logTime=$logTime")
    println("---------------------\n")
    gpOptimizer.update(dataPoint,loss,logTime)
    // call masrour's update function with datapoint = asDoubles and loss / logTime
  }

  private def paramMap2doubleArray(paramMap: ParamMap): Array[Double] = {
    map.iterator.map { case (param, paramFunc) =>
      val value = paramMap(param)
      paramFunc.valueToDouble(value)
    }.toArray
  }
}


object ParamMapAdaptiveExplore {

  object SampleType extends Enumeration {
    val Uniform, Logscale = Value
    // Uniform for (a,b): uniform between (a,b)
    // Logscale for (a,b): sample x ~_U(log(a),log(b)) then give exp(x)
  }

  case class ParamTransformer[T](val valueToDouble: T => Double,
                                 val doubleToValue: Double => T) extends Serializable{
  }

}
