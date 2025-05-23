/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistribution;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;

import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.DIFFERENT_FROM_ALL_CHILDREN;
import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.NO_CHILD;
import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.SAME_WITH_ALL_CHILDREN;
import static org.apache.iotdb.db.queryengine.plan.planner.distribution.NodeDistributionType.SAME_WITH_SOME_CHILD;

public class AddExchangeNodes
    extends PlanVisitor<PlanNode, TableDistributedPlanGenerator.PlanContext> {

  private final MPPQueryContext queryContext;

  public AddExchangeNodes(MPPQueryContext queryContext) {
    this.queryContext = queryContext;
  }

  public PlanNode addExchangeNodes(
      PlanNode node, TableDistributedPlanGenerator.PlanContext context) {
    return node.accept(this, context);
  }

  @Override
  public PlanNode visitPlan(PlanNode node, TableDistributedPlanGenerator.PlanContext context) {
    if (node instanceof WritePlanNode) {
      return node;
    }

    PlanNode newNode = node.clone();
    if (node.getChildren().size() == 1) {
      newNode.addChild(node.getChildren().get(0).accept(this, context));
      context.nodeDistributionMap.put(
          node.getPlanNodeId(),
          new NodeDistribution(
              SAME_WITH_ALL_CHILDREN,
              context
                  .nodeDistributionMap
                  .get(node.getChildren().get(0).getPlanNodeId())
                  .getRegion()));
      return newNode;
    }

    for (PlanNode child : node.getChildren()) {
      PlanNode rewriteNode = child.accept(this, context);
      ExchangeNode exchangeNode = new ExchangeNode(queryContext.getQueryId().genPlanNodeId());
      exchangeNode.addChild(rewriteNode);
      exchangeNode.setOutputSymbols(rewriteNode.getOutputSymbols());
      newNode.addChild(exchangeNode);
      context.hasExchangeNode = true;
      context.nodeDistributionMap.put(
          exchangeNode.getPlanNodeId(),
          new NodeDistribution(SAME_WITH_SOME_CHILD, context.mostUsedRegion));
    }
    context.nodeDistributionMap.put(
        node.getPlanNodeId(), new NodeDistribution(SAME_WITH_SOME_CHILD, context.mostUsedRegion));

    return newNode;
  }

  @Override
  public PlanNode visitTableScan(
      TableScanNode node, TableDistributedPlanGenerator.PlanContext context) {
    context.nodeDistributionMap.put(
        node.getPlanNodeId(),
        new NodeDistribution(SAME_WITH_ALL_CHILDREN, node.getRegionReplicaSet()));
    return node;
  }

  @Override
  public PlanNode visitTableDeviceFetch(
      final TableDeviceFetchNode node, final TableDistributedPlanGenerator.PlanContext context) {
    return processTableDeviceSourceNode(node, context);
  }

  @Override
  public PlanNode visitTableDeviceQueryScan(
      final TableDeviceQueryScanNode node,
      final TableDistributedPlanGenerator.PlanContext context) {
    return processTableDeviceSourceNode(node, context);
  }

  @Override
  public PlanNode visitTableDeviceQueryCount(
      final TableDeviceQueryCountNode node,
      final TableDistributedPlanGenerator.PlanContext context) {
    return processTableDeviceSourceNode(node, context);
  }

  @Override
  public PlanNode visitExplainAnalyze(
      ExplainAnalyzeNode node, TableDistributedPlanGenerator.PlanContext context) {
    ExplainAnalyzeNode newNode = (ExplainAnalyzeNode) node.clone();

    PlanNode child = newNode.getChild();
    child = child.accept(this, context);

    ExchangeNode exchangeNode = new ExchangeNode(queryContext.getQueryId().genPlanNodeId());
    exchangeNode.setChild(child);
    exchangeNode.setOutputSymbols(child.getOutputSymbols());
    newNode.setChild(exchangeNode);

    context.nodeDistributionMap.put(
        newNode.getPlanNodeId(),
        new NodeDistribution(DIFFERENT_FROM_ALL_CHILDREN, DataPartition.NOT_ASSIGNED));
    context.hasExchangeNode = true;
    return newNode;
  }

  @Override
  public PlanNode visitTableFunctionProcessor(
      TableFunctionProcessorNode node, TableDistributedPlanGenerator.PlanContext context) {
    if (node.getChildren().isEmpty()) {
      context.nodeDistributionMap.put(
          node.getPlanNodeId(), new NodeDistribution(NO_CHILD, DataPartition.NOT_ASSIGNED));
      return node;
    } else {
      return visitPlan(node, context);
    }
  }

  private PlanNode processTableDeviceSourceNode(
      final TableDeviceSourceNode node, final TableDistributedPlanGenerator.PlanContext context) {
    context.nodeDistributionMap.put(
        node.getPlanNodeId(),
        new NodeDistribution(SAME_WITH_ALL_CHILDREN, node.getRegionReplicaSet()));
    return node;
  }
}
