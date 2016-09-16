package org.apache.spark.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.Matchers.{any, eq => meq}
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar.mock



/**
  * Created by zkarnin on 9/15/16.
  */
class MapPipelineModelSuite  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest{
  abstract class MyModel extends Model[MyModel]  with MapRowTransformer
  abstract class MyTransformer extends Transformer  with MapRowTransformer

  test("map pipeline construction") {
    val estimator0 = mock[Estimator[MyModel]]
    val model0 = mock[MyModel]
    val transformer1 = mock[MyTransformer]
    val estimator2 = mock[Estimator[MyModel]]
    val model2 = mock[MyModel]
    val transformer3 = mock[MyTransformer]
    val dataset0 = mock[DataFrame]
    val dataset1 = mock[DataFrame]
    val dataset2 = mock[DataFrame]
    val dataset3 = mock[DataFrame]
    val dataset4 = mock[DataFrame]

    when(dataset0.toDF).thenReturn(dataset0)
    when(dataset1.toDF).thenReturn(dataset1)
    when(dataset2.toDF).thenReturn(dataset2)
    when(dataset3.toDF).thenReturn(dataset3)
    when(dataset4.toDF).thenReturn(dataset4)

    when(estimator0.copy(any[ParamMap])).thenReturn(estimator0)
    when(model0.copy(any[ParamMap])).thenReturn(model0)
    when(transformer1.copy(any[ParamMap])).thenReturn(transformer1)
    when(estimator2.copy(any[ParamMap])).thenReturn(estimator2)
    when(model2.copy(any[ParamMap])).thenReturn(model2)
    when(transformer3.copy(any[ParamMap])).thenReturn(transformer3)

    when(estimator0.fit(meq(dataset0))).thenReturn(model0)
    when(model0.transform(meq(dataset0))).thenReturn(dataset1)
    when(model0.parent).thenReturn(estimator0)
    when(transformer1.transform(meq(dataset1))).thenReturn(dataset2)
    when(estimator2.fit(meq(dataset2))).thenReturn(model2)
    when(model2.transform(meq(dataset2))).thenReturn(dataset3)
    when(model2.parent).thenReturn(estimator2)
    when(transformer3.transform(meq(dataset3))).thenReturn(dataset4)



    val pipeline = new Pipeline()
      .setStages(Array(estimator0, transformer1, estimator2, transformer3))
    val pipelineModel = pipeline.fit(dataset0)
    val mapPipelineModel =
      new MapPipelineModel[Row](pipelineModel)

    assert(mapPipelineModel!=null)
  }
}
