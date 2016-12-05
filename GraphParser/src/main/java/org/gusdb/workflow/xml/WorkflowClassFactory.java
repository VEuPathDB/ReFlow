package org.gusdb.workflow.xml;

public interface WorkflowClassFactory<S extends WorkflowNode, T extends WorkflowXmlContainer<S>> {

  public Class<T> getContainerClass();
  public Class<S> getStepClass();

}
