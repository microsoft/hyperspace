package com.microsoft.hyperspace.goldstandard

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.index.LogicalPlanSignatureProvider

class MockSignatureProvider extends LogicalPlanSignatureProvider {
  override def signature(logicalPlan: LogicalPlan): Option[String] = {
    val leafPlans = logicalPlan.collectLeaves()
    if (leafPlans.size != 1) return None

    leafPlans.head match {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        Some(location.rootPaths.head.getName)
    }
  }
}
