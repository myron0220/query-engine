// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.optimizer

import java.lang.IllegalStateException
import io.andygrove.kquery.logical.*

class ProjectionPushDownRule : OptimizerRule {

  override fun optimize(plan: LogicalPlan): LogicalPlan {
    return pushDown(plan, mutableSetOf())
  }

  // Note: `plan` has two attributes: expr (tree of current expression) and input (tree of children of the current plan)

  // -----------------------------------------------------------------------------------------------
  // recursively push down Projection, Selection, and Aggregate into deeper child plan (plan.input)
  // INPUT:
  // DO:
  // OUTPUT:
  private fun pushDown(plan: LogicalPlan, columnNames: MutableSet<String>): LogicalPlan {
    return when (plan) {
      // Projection: schema (changed),  (not changed)
      is Projection -> {
        extractColumns(plan.expr, plan.input, columnNames) // return all used columes into columnNames
        val input = pushDown(plan.input, columnNames) // recursively push down in the child plan
        Projection(input, plan.expr) // create new Projection using the current expr and input
      }
      is Selection -> {
        extractColumns(plan.expr, plan.input, columnNames)
        val input = pushDown(plan.input, columnNames)
        Selection(input, plan.expr)
      }
      is Aggregate -> {
        extractColumns(plan.groupExpr, plan.input, columnNames)
        extractColumns(plan.aggregateExpr.map { it.expr }, plan.input, columnNames)
        val input = pushDown(plan.input, columnNames)
        Aggregate(input, plan.groupExpr, plan.aggregateExpr)
      }
      is Scan -> {
        val validFieldNames = plan.dataSource.schema().fields.map { it.name }.toSet()
        val pushDown = validFieldNames.filter { columnNames.contains(it) }.toSet().sorted()
        Scan(plan.path, plan.dataSource, pushDown)
      }
      else -> throw IllegalStateException("ProjectionPushDownRule does not support plan: $plan")
    }
  }
}
