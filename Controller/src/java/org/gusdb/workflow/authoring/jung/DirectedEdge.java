package org.gusdb.workflow.authoring.jung;

import java.util.concurrent.atomic.AtomicInteger;

import org.gusdb.workflow.WorkflowNode;

public class DirectedEdge {

  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger(0);
  public Integer _id = ID_SEQUENCE.incrementAndGet();

  private WorkflowNode _from;
  private WorkflowNode _to;
  
  public DirectedEdge(WorkflowNode from, WorkflowNode to) {
    _from = from;
    _to = to;
  }

  public WorkflowNode getFromVertex() {
    return _from;
  }

  public WorkflowNode getToVertex() {
    return _to;
  }
}