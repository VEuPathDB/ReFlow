package org.gusdb.workflow.xml;

import java.io.IOException;

import org.gusdb.fgputil.xml.NamedValue;

public interface WorkflowXmlContainer<T extends WorkflowNode> {

  public void setXmlFileName(String xmlFileName);
  public void addParamDeclaration(ParamDeclaration paramDeclaration);
  public void addConstant(NamedValue namedValue);
  public void addGlobalConstant(NamedValue namedValue);
  public void addStep(T step) throws IOException;
  public void addGlobalStep(T step) throws IOException;
  
}
