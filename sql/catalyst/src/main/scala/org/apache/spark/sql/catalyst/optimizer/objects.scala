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

package org.apache.spark.sql.catalyst.optimizer

<<<<<<< HEAD
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
=======
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types.StructType
>>>>>>> tuning_adaptive

/*
 * This file defines optimization rules related to object manipulation (for the Dataset API).
 */

<<<<<<< HEAD
=======

>>>>>>> tuning_adaptive
/**
 * Removes cases where we are unnecessarily going between the object and serialized (InternalRow)
 * representation of data item.  For example back to back map operations.
 */
object EliminateSerialization extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case d @ DeserializeToObject(_, _, s: SerializeFromObject)
<<<<<<< HEAD
      if d.outputObjAttr.dataType == s.inputObjAttr.dataType =>
      // Adds an extra Project here, to preserve the output expr id of `DeserializeToObject`.
      // We will remove it later in RemoveAliasOnlyProject rule.
      val objAttr = Alias(s.inputObjAttr, s.inputObjAttr.name)(exprId = d.outputObjAttr.exprId)
      Project(objAttr :: Nil, s.child)

    case a @ AppendColumns(_, _, _, _, _, s: SerializeFromObject)
      if a.deserializer.dataType == s.inputObjAttr.dataType =>
      AppendColumnsWithObject(a.func, s.serializer, a.serializer, s.child)

    // If there is a `SerializeFromObject` under typed filter and its input object type is same with
    // the typed filter's deserializer, we can convert typed filter to normal filter without
    // deserialization in condition, and push it down through `SerializeFromObject`.
    // e.g. `ds.map(...).filter(...)` can be optimized by this rule to save extra deserialization,
    // but `ds.map(...).as[AnotherType].filter(...)` can not be optimized.
    case f @ TypedFilter(_, _, _, _, s: SerializeFromObject)
      if f.deserializer.dataType == s.inputObjAttr.dataType =>
      s.copy(child = f.withObjectProducerChild(s.child))

    // If there is a `DeserializeToObject` upon typed filter and its output object type is same with
    // the typed filter's deserializer, we can convert typed filter to normal filter without
    // deserialization in condition, and pull it up through `DeserializeToObject`.
    // e.g. `ds.filter(...).map(...)` can be optimized by this rule to save extra deserialization,
    // but `ds.filter(...).as[AnotherType].map(...)` can not be optimized.
    case d @ DeserializeToObject(_, _, f: TypedFilter)
      if d.outputObjAttr.dataType == f.deserializer.dataType =>
      f.withObjectProducerChild(d.copy(child = f.child))
  }
}

/**
 * Combines two adjacent [[TypedFilter]]s, which operate on same type object in condition, into one,
 * mering the filter functions into one conjunctive function.
 */
object CombineTypedFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case t1 @ TypedFilter(_, _, _, _, t2 @ TypedFilter(_, _, _, _, child))
        if t1.deserializer.dataType == t2.deserializer.dataType =>
      TypedFilter(
        combineFilterFunction(t2.func, t1.func),
        t1.argumentClass,
        t1.argumentSchema,
        t1.deserializer,
        child)
  }

  private def combineFilterFunction(func1: AnyRef, func2: AnyRef): Any => Boolean = {
    (func1, func2) match {
      case (f1: FilterFunction[_], f2: FilterFunction[_]) =>
        input => f1.asInstanceOf[FilterFunction[Any]].call(input) &&
          f2.asInstanceOf[FilterFunction[Any]].call(input)
      case (f1: FilterFunction[_], f2) =>
        input => f1.asInstanceOf[FilterFunction[Any]].call(input) &&
          f2.asInstanceOf[Any => Boolean](input)
      case (f1, f2: FilterFunction[_]) =>
        input => f1.asInstanceOf[Any => Boolean].apply(input) &&
          f2.asInstanceOf[FilterFunction[Any]].call(input)
      case (f1, f2) =>
        input => f1.asInstanceOf[Any => Boolean].apply(input) &&
          f2.asInstanceOf[Any => Boolean].apply(input)
=======
        if d.outputObjectType == s.inputObjectType =>
      // Adds an extra Project here, to preserve the output expr id of `DeserializeToObject`.
      // We will remove it later in RemoveAliasOnlyProject rule.
      val objAttr =
        Alias(s.child.output.head, s.child.output.head.name)(exprId = d.output.head.exprId)
      Project(objAttr :: Nil, s.child)
    case a @ AppendColumns(_, _, _, s: SerializeFromObject)
        if a.deserializer.dataType == s.inputObjectType =>
      AppendColumnsWithObject(a.func, s.serializer, a.serializer, s.child)
  }
}


/**
 * Typed [[Filter]] is by default surrounded by a [[DeserializeToObject]] beneath it and a
 * [[SerializeFromObject]] above it.  If these serializations can't be eliminated, we should embed
 * the deserializer in filter condition to save the extra serialization at last.
 */
object EmbedSerializerInFilter extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s @ SerializeFromObject(_, Filter(condition, d: DeserializeToObject))
      // SPARK-15632: Conceptually, filter operator should never introduce schema change. This
      // optimization rule also relies on this assumption. However, Dataset typed filter operator
      // does introduce schema changes in some cases. Thus, we only enable this optimization when
      //
      //  1. either input and output schemata are exactly the same, or
      //  2. both input and output schemata are single-field schema and share the same type.
      //
      // The 2nd case is included because encoders for primitive types always have only a single
      // field with hard-coded field name "value".
      // TODO Cleans this up after fixing SPARK-15632.
      if s.schema == d.child.schema || samePrimitiveType(s.schema, d.child.schema) =>

      val numObjects = condition.collect {
        case a: Attribute if a == d.output.head => a
      }.length

      if (numObjects > 1) {
        // If the filter condition references the object more than one times, we should not embed
        // deserializer in it as the deserialization will happen many times and slow down the
        // execution.
        // TODO: we can still embed it if we can make sure subexpression elimination works here.
        s
      } else {
        val newCondition = condition transform {
          case a: Attribute if a == d.output.head => d.deserializer
        }
        val filter = Filter(newCondition, d.child)

        // Adds an extra Project here, to preserve the output expr id of `SerializeFromObject`.
        // We will remove it later in RemoveAliasOnlyProject rule.
        val objAttrs = filter.output.zip(s.output).map { case (fout, sout) =>
          Alias(fout, fout.name)(exprId = sout.exprId)
        }
        Project(objAttrs, filter)
      }
  }

  def samePrimitiveType(lhs: StructType, rhs: StructType): Boolean = {
    (lhs, rhs) match {
      case (StructType(Array(f1)), StructType(Array(f2))) => f1.dataType == f2.dataType
      case _ => false
>>>>>>> tuning_adaptive
    }
  }
}
