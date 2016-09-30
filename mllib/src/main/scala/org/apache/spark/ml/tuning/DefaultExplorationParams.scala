package org.apache.spark.ml.tuning

import org.apache.spark.ml.classification._
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.regression._
import org.apache.spark.ml.tuning.ParamMapAdaptiveExplore.SampleType.{Logscale,Uniform}


object DefaultExplorationParams {

  def gbdtClassifier(gbdt: GBTClassifier) : ParamMapAdaptiveExploreBuilder = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(gbdt.maxIter,(10,100),Logscale,0.5,20)
    explorer.addParam(gbdt.stepSize,(0.005,0.8),Uniform)
    explorer.addParam(gbdt.impurity,Array("gini","impurity"))
    explorer.addParam(gbdt.maxDepth,(4,18),Logscale,0.5,20)
    explorer.addParam(gbdt.minInstancesPerNode,(20,200),Logscale,1.0,10)
    explorer
  }

  def randomForestClassifier(rf: RandomForestClassifier) : ParamMapAdaptiveExploreBuilder = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(rf.numTrees,(10,200),Logscale,0.5,20)
    explorer.addParam(rf.impurity,Array("gini","impurity"))
    explorer.addParam(rf.maxDepth,(4,18),Logscale,0.5,20)
    explorer
  }

  def logisticRegressionClassifier(lr: LogisticRegression) = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(lr.elasticNetParam,(0.0,1.0),Uniform)
    explorer.addParam(lr.regParam,(1e-10,1e2),Logscale)
    explorer
  }

  def multilayerPerceptron(mp: MultilayerPerceptronClassifier) = {
    // TODO(zkarnin) - find reasonable classifier params
    new ParamMapAdaptiveExploreBuilder
  }

  def naiveBayes(nb: NaiveBayes) = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(nb.smoothing,(0.3,3.0),Logscale)
    explorer.addParam(nb.modelType,Array("multinomial","bernoulli"))
    explorer
  }

  def alternatingLeastSquares(als: ALS) = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(als.regParam,(1e-10,1e2),Logscale)
    explorer
  }

  def aftSurvivalRegression(asr: AFTSurvivalRegression) = {
    new ParamMapAdaptiveExploreBuilder
  }

  def gbtRegressor(gbt: GBTRegressor) = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(gbt.maxIter,(10,100),Logscale,0.5,20)
    explorer.addParam(gbt.stepSize,(0.005,0.8),Uniform)
    explorer.addParam(gbt.maxDepth,(4,18),Logscale,0.5,20)
    explorer
  }

  def generalizedLinearRegression(lr: GeneralizedLinearRegression) = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(lr.regParam,(1e-10,1e2),Logscale)
    explorer
  }

  def linearRegression(lr: LinearRegression) = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(lr.regParam,(1e-10,1e2),Logscale)
    explorer.addParam(lr.elasticNetParam,(0.0,1.0),Uniform)
    explorer
  }

  def randomForestRegressor(rf: RandomForestRegressor) : ParamMapAdaptiveExploreBuilder = {
    val explorer = new ParamMapAdaptiveExploreBuilder
    explorer.addParam(rf.numTrees,(10,200),Logscale,0.5,20)
    explorer.addParam(rf.maxDepth,(4,18),Logscale,0.5,20)
    explorer
  }


}
