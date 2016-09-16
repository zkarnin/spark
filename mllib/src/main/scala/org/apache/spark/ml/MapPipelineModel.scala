package org.apache.spark.ml

import org.apache.spark.sql.Row

class MapPipelineModel[T](private val pipelineModel: PipelineModel) {
  assert(pipelineModel.stages.length>=1,"cannot instantiate MapPipelineModel with no stages")
  assert(classOf[Transformer].isAssignableFrom(pipelineModel.stages(0).getClass) &&
    classOf[MapTransformer[T]].isAssignableFrom(pipelineModel.stages(0).getClass),
  s"first transformer of model must extend both ${classOf[MapTransformer[T]]} and Transformer")
  val firstStage = pipelineModel.stages(0).asInstanceOf[Transformer with MapTransformer[T]]
  val otherStages = {
    val asTransformers = pipelineModel.stages.slice(1,pipelineModel.stages.length)
    assert(asTransformers.forall{stage =>
      classOf[Transformer].isAssignableFrom(stage.getClass) &&
        classOf[MapRowTransformer].isAssignableFrom(stage.getClass)
    }, "all but first stage must extend MapRowTransformer")
    asTransformers.map(_.asInstanceOf[Transformer with MapRowTransformer])
  }

  private def rowPermutation(inputs : Array[String],outputs : Array[String])
  : Row => Row = {
    val inputIndices = outputs.map{output =>
      val inputIndex = inputs.indexOf(output)
      assert(inputIndex>=0)
      inputIndex
    }
    row=> {
      Row(inputIndices.map(row.get))
    }
  }
  /**
    * Transforms a single object into a row. Returns Option
    * to handle invalid inputs
    * (this can be used in flatMap to convert the dataset)
    */
  def transformRow(row: T): Option[Row] = {
    //TODO(zkarnin) - make this more efficient.
    // Option 1) Have a init method
    //  that should be used before transformRow to set up all these row
    //  permutation functions.
    // Option 2) allow modifying the output columns so that no permutation
    //  functions are needed
    var output = firstStage.transformRow(row)
    var i=0
    var prevOutputCols = firstStage.getOutputCols
    while (output.isDefined && i<otherStages.length) {
      val curInputCols = otherStages(i).getInputCols
      val rowTransformFunc = rowPermutation(prevOutputCols,curInputCols)
      val input = rowTransformFunc (output.get)
      output = otherStages(i).transformRow(input)
      prevOutputCols = otherStages(i).getOutputCols
      i+=1
    }
    output
  }
}

//
//@Since("2.1.0")
///**
//  * Created by zkarnin on 9/14/16.
//  */
//class MapPipelineModel[T](
//                           override val uid: String,
//                           val firstStage : Transformer with MapTransformer[T],
//                           val otherStages: Array[Transformer with MapRowTransformer])
//  extends PipelineModel(uid, Array(firstStage.asInstanceOf[Transformer])++otherStages) with MapTransformer[T]{
//
//  private def rowPermutation(inputs : Array[String],outputs : Array[String])
//  : Row => Row = {
//    val inputIndices = outputs.map{output =>
//      val inputIndex = inputs.indexOf(output)
//      assert(inputIndex>=0)
//      inputIndex
//    }
//    row=> {
//      Row(inputIndices.map(row.get))
//    }
//  }
//  /**
//    * Transforms a single object into a row. Returns Option
//    * to handle invalid inputs
//    * (this can be used in flatMap to convert the dataset)
//    */
//  override def transformRow(row: T): Option[Row] = {
//    //TODO(zkarnin) - make this more efficient.
//    // Option 1) Have a init method
//    //  that should be used before transformRow to set up all these row
//    //  permutation functions.
//    // Option 2) allow modifying the output columns so that no permutation
//    //  functions are needed
//    var output = firstStage.transformRow(row)
//    var i=0
//    var prevOutputCols = firstStage.getOutputCols
//    while (output.isDefined && i<otherStages.length) {
//      val curInputCols = otherStages(i).getInputCols
//      val rowTransformFunc = rowPermutation(prevOutputCols,curInputCols)
//      val input = rowTransformFunc (output.get)
//      output = otherStages(i).transformRow(input)
//      prevOutputCols = otherStages(i).getOutputCols
//      i+=1
//    }
//    output
//  }
//}

//object MapPipelineModel {
//
//  def fromPipelineModel[T](uid: String, pipelineModel: PipelineModel): MapPipelineModel[T] = {
//    assert(!pipelineModel.stages.isEmpty)
//    val firstStage = pipelineModel.stages(0).asInstanceOf[Transformer with MapTransformer[T]]
//    val otherStages = pipelineModel.stages.slice(1,pipelineModel.stages.length)
//      .map(_.asInstanceOf[Transformer with MapRowTransformer])
//
//    new MapPipelineModel(uid, firstStage,otherStages)
//  }
//
//
//}


