package org.gusdb.workflow;

import java.util.List;

public interface WorkflowNode {
  
  public String getBaseName();
  public void setName(String baseName);

  public String getSourceXmlFileName();
  public void setSourceXmlFileName(String fileName);
  
  public String getSubgraphXmlFileName();
  public void setXmlFile(String fileName);
  
  public List<Name> getDependsNames();
  
  public void addParamValue(NamedValue namedValue);
  public void addDependsName(Name dependsName);
  public void addDependsGlobalName(Name dependsName);

}
