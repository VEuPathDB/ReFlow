package org.gusdb.workflow.xml;

import java.io.IOException;

import org.gusdb.fgputil.xml.NamedValue;

public abstract class WorkflowXmlContainer <T extends WorkflowNode> {

  public abstract void setXmlFileName(String xmlFileName);
  public abstract void addParamDeclaration(ParamDeclaration paramDeclaration);
  public abstract void addConstant(NamedValue namedValue);
  public abstract void addGlobalConstant(NamedValue namedValue);
  public abstract void addStep(T step) throws IOException;
  public abstract void addGlobalStep(T step) throws IOException;
  
}
