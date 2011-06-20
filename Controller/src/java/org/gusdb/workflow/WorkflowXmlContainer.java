package org.gusdb.workflow;

import java.io.IOException;


public abstract class WorkflowXmlContainer <T extends WorkflowNode> {

  public abstract void setXmlFileName(String xmlFileName);
  public abstract void addParamDeclaration(Name name);
  public abstract void addConstant(NamedValue namedValue);
  public abstract void addGlobalConstant(NamedValue namedValue);
  public abstract void addStep(T step) throws IOException;
  public abstract void addGlobalStep(T step) throws IOException;
  
}
