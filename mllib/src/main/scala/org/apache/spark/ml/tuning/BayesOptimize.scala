package org.apache.spark.ml.tuning

import java.io.Serializable

import breeze.linalg.{DenseMatrix, DenseVector, argmax, argmin, argsort, det, inv, sum}
import breeze.numerics.erf
import breeze.stats.distributions.{RandBasis, ThreadLocalRandomGenerator}
import org.apache.commons.math3.random.MersenneTwister

import scala.collection.mutable.ArrayBuffer
import scala.math.{Pi, abs, exp, log, max, min, pow, sqrt}
import scala.util.Random


/**
  * Created by zkarnin on 7/12/16.
  */
class BayesOptimize(val Dim : Int)
  extends Serializable {


  var lossPriorMean = (x: Array[Double]) => 0.0
  var logCostPriorMean = (x: Array[Double]) => 0.0
  var kernelTuning = "randOpt"
  var considerCost = false
  var tuningBudget = 5
  var optimizationBudget = 1000
  var lossObsNoise = 0.001
  var logCostObsNoise = 0.01
  var lossLengthScales = Array.fill(Dim)(1.0)
  var logCostLengthScales = Array.fill(Dim)(1.0)
  var pastSamples = new ArrayBuffer[Array[Double]]()
  var pastLosses = new ArrayBuffer[Double]()
  var pastLogCosts = new ArrayBuffer[Double]()
  var bestPastSurrogateSamples = new ArrayBuffer[Array[Double]]()
  val defaultMinTuningSamples = 3*Dim // TODO - experiment with this
  val seed = 42
  val rand = new Random(seed = seed)
  var lscaleUpdateRate = 5
  var logCostPriorLambda = 1.0
  implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))


  def setPrior(mu : Array[Double] => Double,
               lengthScales : Array[Double]
              ) : Unit = {
    lossPriorMean = mu
    lossLengthScales = lengthScales.clone()
  }

  /**
    * Same as setPrior, but the value is not the loss but log of the computation time
    */
  def setLogComputationPrior(mu : Array[Double] => Double,
                             lengthScales : Array[Double],
                             lambda: Double
                            ) : Unit = {
    logCostPriorMean = mu
    logCostLengthScales = lengthScales.clone()
    logCostPriorLambda=lambda
  }

  def getNextN(N: Int): Array[Array[Double]] = {
    val tune_lscales = rand.nextInt(lscaleUpdateRate)==0
    getNextN(N,tune_lscales)
  }

  def getNextN(N: Int, tune_lscales: Boolean): Array[Array[Double]] = {
    var samples = new ArrayBuffer[Array[Double]]
    val T0 = System.currentTimeMillis
    if (tune_lscales) {
//      println("tune_lscales: "+tune_lscales.toString)
      val lScales_oNoise = tuneHyperparams(pastSamples.toArray,
        pastLosses.toArray,
        SEKernel,
        tuningBudget * 10,
        lossLengthScales,
        lossObsNoise)
      lossLengthScales = lScales_oNoise._1
      lossObsNoise = lScales_oNoise._2
      if (considerCost) {
        val lScales_oNoise = tuneHyperparams(pastSamples.toArray,
          pastLogCosts.toArray,
          SEKernel,
          tuningBudget * 10,
          logCostLengthScales,
          logCostObsNoise)
        logCostLengthScales = lScales_oNoise._1
        logCostObsNoise = lScales_oNoise._2
      }
    }
    val tT = System.currentTimeMillis
    samples += MaximizeSurrogate(optimizationBudget,
      lossLengthScales,lossObsNoise,
      logCostLengthScales,logCostObsNoise,
      SEKernel, updatePastSurrogateSamples = true)
    val tO = System.currentTimeMillis
//    println("Tuning time="+(tT-T0).toString+"ms,   Optimization time="+(tO-tT).toString+"ms")
    for (ind <- 2 to N) {
      val losslScales_oNoise = tuneHyperparams(pastSamples.toArray,
        pastLosses.toArray,
        SEKernel,
        tuningBudget,
        lossLengthScales,
        lossObsNoise,
        nPointsToSubsample = defaultMinTuningSamples,
        checkOnAllSamples = false)
      var costlScales_oNoise = (logCostLengthScales.clone,logCostObsNoise)
      if (considerCost) {
        costlScales_oNoise = tuneHyperparams(pastSamples.toArray,
          pastLogCosts.toArray,
          SEKernel,
          tuningBudget,
          logCostLengthScales,
          logCostObsNoise,
          nPointsToSubsample = defaultMinTuningSamples,
          checkOnAllSamples = true)
      }
      val sample = MaximizeSurrogate(optimizationBudget,
        losslScales_oNoise._1, losslScales_oNoise._2,
        costlScales_oNoise._1, costlScales_oNoise._2,
        SEKernel, updatePastSurrogateSamples = false)
      samples += sample
    }
    samples.toArray
  }

  def bool2int(b:Boolean) = if (b) 1 else 0

  def update(sample: Array[Double], loss: Double, cost: Double): Unit = {
    pastSamples += sample
    pastLosses += loss
    pastLogCosts += log(cost)
  }

  def getMinLoss = {
    pastLosses.min
  }

  def getMinTime = {
    argmin(DenseVector(pastLosses.toArray))
  }

  private def MaximizeSurrogate(optBudget: Int,
                                in_lossLengthScales: Array[Double],
                                loc_lossObsNoise: Double,
                                in_logCostLengthScales: Array[Double],
                                loc_logCostObsNoise: Double,
                                kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                                updatePastSurrogateSamples: Boolean): Array[Double] = {
    val loc_lossLengthScales = in_lossLengthScales.clone
    val loc_logCostLengthScales = in_logCostLengthScales.clone
    if (pastSamples.isEmpty) {
      val nextNormalizedRandSample: Array[Double] = DenseVector.rand(Dim,rand=randBasis.uniform).toArray
      return nextNormalizedRandSample
    }
    var newValue = 0.0
    var newSample: Array[Double] = pastSamples(0)
    var samples = new ArrayBuffer[Array[Double]]()
    var values = new ArrayBuffer[Double]()

    // Get smoothed cov matrices
    val K = getCovMtx(pastSamples.toArray,kernel, loc_lossLengthScales, loc_lossObsNoise)

    // Inverse of the cov mtx: SVD tends to be more stable, but VERY SLOW!
    //val svd.SVD(ru,rs,rv) = svd(K)
    // val Kinv = rv.t * diag(DenseVector.ones[Double](rs.length)/rs) * ru.t
    val Kinv = inv(K)

    var cKinv = DenseMatrix.zeros[Double](pastSamples.length,pastSamples.length)
    if (considerCost) {
      val cK = getCovMtx(pastSamples.toArray,kernel, loc_logCostLengthScales, loc_logCostObsNoise)
      cKinv = inv(cK)
    }

    // Look around the old points maximizing the surrogate function:
    if (bestPastSurrogateSamples.nonEmpty) {
      for ( ind <- bestPastSurrogateSamples.indices ) {
        val pastSample = bestPastSurrogateSamples(ind)
        val pastValue = EvaluateSurrogate(pastSample, SEKernel, Kinv, EI)
        for ( jnd <- 1 to optBudget/(4*bestPastSurrogateSamples.length) ) {
          newSample = clipped_randn(pastSample, loc_lossLengthScales)
          newValue = EvaluateSurrogate(newSample, SEKernel, Kinv, EI)
          val sample_value_to_keep = PointsToKeep(pastSample, pastValue, newSample, newValue,
            SEKernel, lossPriorMean, Kinv, EI)
          samples ++= sample_value_to_keep._1
          values ++= sample_value_to_keep._2
        }
      }
    }

    // Look at random samples too
    for( ind <- 1 to optBudget/2 ){
      newSample = DenseVector.rand(Dim,rand=randBasis.uniform).toArray
      newValue = EvaluateSurrogate(newSample,SEKernel,Kinv,EI)
      samples += newSample
      values += newValue
    }

    if (updatePastSurrogateSamples)
      updateBestPastSurrogateSamples(samples,values,Kinv)

    // Return the point with the max surrogate value
    val dvValues = DenseVector(values.toArray)
    val best_ind = argmax(dvValues)
    val m = values.min
    val M = values.max
//    println("EI(x)="+values(best_ind).toString+"  min/max EI=("+f"$m%1.5f"+","+f"$M%1.5f"+")")
    samples(best_ind)
  }

  private def updateBestPastSurrogateSamples(samples: ArrayBuffer[Array[Double]], values: ArrayBuffer[Double],
                                             Kinv: DenseMatrix[Double]): Unit = {
    // Keep the most promising points
    val dvValues = DenseVector(values.toArray)
    val Inds = DenseVector(argsort(dvValues).toArray)
    var newBestSurrogateSamples = ArrayBuffer(samples(Inds(-1)))
    var newBestSurrogateValues = ArrayBuffer(values(Inds(-1)))
    var i = 2
    while (i < min(50, Inds.length) & newBestSurrogateSamples.length < 10) {
      val ind = Inds(-i)

      // Start out assuming that samples(ind) is going to be added to newBestSurrogateSamples
      var addSample = true
      var j = 0
      while (j < newBestSurrogateSamples.length) {
        val sample_value_to_keep = PointsToKeep(newBestSurrogateSamples(j), newBestSurrogateValues(j),
          samples(ind), values(ind),
          SEKernel, lossPriorMean, Kinv, EI)
        val s = sample_value_to_keep._1
        val v = sample_value_to_keep._2
        val o = sample_value_to_keep._3

        // If sample(ind) or the intermediate sample is better than newBestSurrogateSamples(j), replace the latter.
        if (o == "other" || o == "2") {
          newBestSurrogateSamples(j) = s(0)
          newBestSurrogateValues(j) = v(0)
        }

        // If there is another sample that is better than sample(ind), then don't add it to newBestSurrogateSamples
        if (o == "other" || o == "1") addSample = false
        j += 1
      }

      if (addSample) {
        newBestSurrogateSamples += samples(ind)
        newBestSurrogateValues += values(ind)
      }

      i += 1
    }

    bestPastSurrogateSamples = newBestSurrogateSamples.clone
//        println("bestPastSurrogateSamples.length="+bestPastSurrogateSamples.length.toString)
    //    println("values.length="+values.length.toString)
  }


  private def PointsToKeep(x1: Array[Double], y1: Double, x2: Array[Double], y2: Double,
                           kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                           priorMean: Array[Double] => Double,
                           Kinv: DenseMatrix[Double],
                           surrogate: (Double,Double,Double) => Double): (Array[Array[Double]],Array[Double], String) = {
    var Xs = new ArrayBuffer[Array[Double]]()
    var Ys = new ArrayBuffer[Double]()
    val lambda = 1.0/3+rand.nextDouble()/3
    val ax1 = new DenseVector(x1)
    val ax2 = new DenseVector(x2)
    val x12 = (lambda * ax1 + (1-lambda) * ax2).toArray
    val y12 = EvaluateSurrogate(x12,kernel,Kinv,surrogate)
    var outcome = ""
    if (y12 < min(y1,y2)) {
      Xs += x1
      Xs += x2
      Ys += y1
      Ys += y2
      outcome = "both"
    } else {
      val allXs = Array(x12, x1, x2)
      val allYs = new DenseVector(Array(y1, y12, y2))
      val max_i = argmin(allYs)
      Xs += allXs(max_i)
      Ys += allYs(max_i)
      if (max_i == 0) {
        outcome = "other"
      } else {
        outcome = max_i.toString
      }
    }
    (Xs.toArray, Ys.toArray, outcome)
  }

  private def clipped_randn(x: Array[Double],lScale: Array[Double]): Array[Double] = {
    val x2 = x
    for (ind <- x.indices) {
      x2(ind) += rand.nextGaussian() * min(lScale(ind),1)/4
    }
    clip01(x2)
  }

  private def clip01(x: Array[Double]): Array[Double] = {
    val x01 = x
    for (ind <- x01.indices) {
      x01(ind) = min(x01(ind),1)
      x01(ind) = max(x01(ind),0)
    }
    x01
  }

  private def tuneHyperparams(pastSamples: Array[Array[Double]],
                              pastValues: Array[Double],
                              kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                              optBudget: Int,
                              in_currentLengthScales: Array[Double],
                              currentObsNoise: Double,
                              nPointsToSubsample: Int = 0,
                              checkOnAllSamples: Boolean = false): (Array[Double],Double) = {
    val currentLengthScales = in_currentLengthScales.clone
    val N = pastSamples.length
    // If there are fewer samples than the dimension, don't tune anything
    var n = N
    if (n > nPointsToSubsample && nPointsToSubsample >= defaultMinTuningSamples) {
      n = nPointsToSubsample
    }
    if (n < defaultMinTuningSamples) {
      return (currentLengthScales.toArray,currentObsNoise)
    }
    var lScales_oNoise = (currentLengthScales.toArray,currentObsNoise)
    if ( kernelTuning == "randOpt" ) {
      for( i <- 1 to optBudget ){
        lScales_oNoise = tryNewLengthScales(pastSamples,pastValues,n,lScales_oNoise._1,lScales_oNoise._2,kernel)
      }
    }
    // Verify that things have not got worse on the whole data:
    if (checkOnAllSamples){
      compareLengthScales(pastSamples,pastValues,kernel,in_nTrials=1,nPointsToSubsample=pastSamples.length,
        in_currentLengthScales,currentObsNoise,lScales_oNoise._1,lScales_oNoise._2)
    } else {
      lScales_oNoise
    }
  }

  private def tryNewLengthScales(pastSamples: Array[Array[Double]],
                                 pastValues: Array[Double],
                                 nPointsToSubsample: Int,
                                 input_oldLScales: Array[Double],
                                 oldObsNoise: Double,
                                 kernel: (Array[Double],Array[Double],Array[Double]) => Double): (Array[Double],Double) = {
    val oldLScales = input_oldLScales.clone
    val newLScales_Noise = sampleNearbyLengthScales(oldLScales, oldObsNoise)
    val newLScales = newLScales_Noise._1
    // The observation noise should not become too small for numerical reasons:
    val newObsNoise = max(newLScales_Noise._2, 0.001)
    val nTrials = 3
    compareLengthScales(pastSamples,pastValues,kernel,nTrials,nPointsToSubsample,
      oldLScales,oldObsNoise,newLScales,newObsNoise)
  }

  private def compareLengthScales(pastSamples: Array[Array[Double]],
                                  pastValues: Array[Double],
                                  kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                                  in_nTrials: Int,
                                  nPointsToSubsample: Int,
                                  input_oldLScales: Array[Double],
                                  oldObsNoise: Double,
                                  input_newLScales: Array[Double],
                                  newObsNoise: Double): (Array[Double],Double) = {
    val oldLScales = input_oldLScales.clone
    val newLScales = input_newLScales.clone
    var oldLLs = new ArrayBuffer[Double]()
    var newLLs = new ArrayBuffer[Double]()
    var nTrials = in_nTrials
    // If we're using all of the samples, do the following for-loop only once:
    if (pastSamples.length == nPointsToSubsample) nTrials = 1
    val winsForNew = DenseVector.zeros[Double](nTrials)
    for (ind <- 0 until nTrials) {
      val samples_losses = subSampleArrays(pastSamples, pastValues, nPointsToSubsample)
      val samples = samples_losses._1
      val losses = samples_losses._2
      val f = getF(losses)
      val oldK = getCovMtx(samples, kernel, oldLScales, oldObsNoise)
      val oldLL = getLL(oldK, f)
      oldLLs += oldLL
      val newK = getCovMtx(samples, kernel, newLScales, newObsNoise)
      val newLL = getLL(newK, f)
      newLLs += newLL

      if (newLL > oldLL) {
        winsForNew(ind) = 1.0
      }
      else {
        winsForNew(ind) = -1.0
      }
    }
    if (sum(winsForNew) >= nTrials) {
      if (nTrials == 1) {
//        println("Updated the length scales: *******************")
      } else {
//        println("Updated the length scales:")
      }
//      println("old hyperparams:" + BayesOptimize.Array2String(oldLScales))
//      println("new hyperparams:" + BayesOptimize.Array2String(newLScales))
//      println("old LLs:" + BayesOptimize.Array2String(oldLLs.toArray))
//      println("new LLs:" + BayesOptimize.Array2String(newLLs.toArray))
    }
    if (sum(winsForNew) >= nTrials) (newLScales, newObsNoise) else (oldLScales, oldObsNoise)
  }


  private def sampleNearbyLengthScales(lScales: Array[Double],oNoise: Double): (Array[Double],Double) = {
    val newLScales = lScales.clone
    var newLogLS = 0.0
    // Lesson learned: Do NOT tune oNoise together with lScales because the tuning becomes very unstable!
    // val newLogObsNoise = log(oNoise) + rand.nextGaussian()/2
    val newObsNoise = oNoise //exp(newLogObsNoise)
    val ind = abs(rand.nextInt) % lScales.length
    newLogLS = log(lScales(ind)) + rand.nextGaussian()/10
    newLScales(ind) = exp(newLogLS)
    (newLScales,newObsNoise)
  }

  private def getRandnMtx(R: Int, C: Int): DenseMatrix[Double] = {
    val normal01 = breeze.stats.distributions.Gaussian(0, 1)
    val samples = normal01.sample(R*C)
    val X = new DenseMatrix[Double](R, C, samples.toArray)
    X
  }


  private def subSampleArrays(A: Array[Array[Double]], b: Array[Double], n: Int): (Array[Array[Double]],Array[Double]) = {
    val N = A.length
    var tempA = new ArrayBuffer[Array[Double]]()
    var newA = new ArrayBuffer[Array[Double]]()
    var tempb = new ArrayBuffer[Double]()
    var newb = new ArrayBuffer[Double]()
    if (n > 0 & n < N){
      var Inds = new ArrayBuffer[Int]()
      var j = 0
      while (j < n){
        val ind = rand.nextInt(n)
        if (!Inds.contains(ind)){
          Inds += ind
          tempA += A(ind)
          tempb += b(ind)
          j += 1
        }
      }
      newA = tempA
      newb = tempb
    } else{
      newA = A.to[ArrayBuffer]
      newb = b.to[ArrayBuffer]
    }
    (newA.toArray,newb.toArray)
  }

  private def getLL(K: DenseMatrix[Double],
                    f: DenseMatrix[Double]): Double = {

    val fKinvf: Double = sum(f.t * inv(K) * f)
    -0.5 * log(det(K)) - fKinvf
  }

  private def EvaluateSurrogate(x: Array[Double],
                                kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                                Kinv: DenseMatrix[Double],
                                surrogate: (Double,Double,Double) => Double): Double = {
    val loss_mu_sigma_x = mu_sigma(x,pastSamples.toArray,pastLosses.toArray,Kinv,kernel,lossPriorMean)
    var s = surrogate(pastLosses.min, loss_mu_sigma_x._1, loss_mu_sigma_x._2)
    if (considerCost) {
      val logCost_mu_x = mu_sigma(x,pastSamples.toArray,pastLogCosts.toArray,Kinv,kernel,lossPriorMean)._1
      s /= exp(logCost_mu_x)
    }
    s
  }

  private def mu_sigma(x: Array[Double],
                       pastSamples: Array[Array[Double]],
                       pastValues: Array[Double],
                       Kinv: DenseMatrix[Double],
                       kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                       priorMean: Array[Double] => Double): (Double,Double) = {
    // Get smoothed cov matrices
    val k = getCovVec(x, pastSamples, kernel, lossLengthScales)
    val f = getF(pastValues)
    val m = DenseMatrix.zeros[Double](f.rows,1)
    for (ind <- 0 until f.rows)
      m(ind,0) = priorMean(pastSamples(ind))

    val Kinvk = Kinv * k

    // posterior mean and std deviation
    val mu_x = sum((f.t-m.t) * Kinvk)+priorMean(x)
    val sigma_x = sqrt(kernel(x,x,lossLengthScales)) - sum(k.t * Kinvk)

    (mu_x,sigma_x)
  }

  private def getCovVec(x: Array[Double],
                        pastSamples: Array[Array[Double]],
                        kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                        lScale: Array[Double]): DenseMatrix[Double] = {
    val n = pastSamples.length
    // Covariance vector
    val k = DenseMatrix.zeros[Double](n,1)
    for(i <- 0 until n) {
      k(i,0) = kernel(x, pastSamples(i), lScale)
    }
    k
  }

  private def getCovMtx(pastSamples: Array[Array[Double]],
                        kernel: (Array[Double],Array[Double],Array[Double]) => Double,
                        lScale: Array[Double],
                        oNoise: Double): DenseMatrix[Double] = {
    val n = pastSamples.length
    // Covariance matrix
    val K = DenseMatrix.zeros[Double](n,n)
    // Fill the values
    for(i <- 0 until n) {
      for (j <- 0 until n) {
        K(i, j) = kernel(pastSamples(i), pastSamples(j), lScale)
      }
    }
    // Smooth the covariance matrix
    K+oNoise*DenseMatrix.eye[Double](n)
  }

  def getF(pastValues: Array[Double]): DenseMatrix[Double] = {
    val n = pastValues.length
    // Vector of function values
    val f = DenseMatrix.zeros[Double](n,1)
    for(i <- 0 until n) {
      f(i,0) = pastValues(i)
    }
    f
  }

  def EI(best_value: Double, mu_x: Double, sigma_x: Double): Double = {
    val gamma_x = my_gamma(best_value, mu_x, sigma_x)
    sigma_x * (gamma_x * Phi(gamma_x) + phi(gamma_x))
  }

  def my_gamma(best_value: Double, mu_x: Double, sigma_x: Double): Double = (best_value - mu_x) / sigma_x

  def phi(x: Double): Double = exp(-pow(x,2)/2.0) / sqrt(2.0*Pi)

  def Phi(x: Double): Double = .5 * (1.0 + erf( x / sqrt(2.0) ))

  def SEKernel(x1: Array[Double],x2: Array[Double],l: Array[Double]): Double = {
    var sum = 0.0
    for (ind <- x1.indices){
      sum += pow( (x1(ind)-x2(ind)) / l(ind),2)
    }
    exp( -sum )
  }

}

object BayesOptimize {
  def Array2String(A: Array[Double]): String = {
    var tbpA = "("
    var a = 0.0
    for (ind <- A.indices) {
      a = A(ind)
      tbpA += f"$a%1.4f"
      if (ind < A.length - 1) tbpA += ", "
    }
    tbpA + ")"
  }
}