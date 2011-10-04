package org.gusdb.workflow.xml;

import java.util.ArrayList;
import java.util.List;

import org.gusdb.fgputil.xml.Name;
import org.gusdb.fgputil.xml.NamedValue;

public class SimpleXmlNode implements WorkflowNode {

  private String _baseName;
  private String _sourceXmlFileName;
  private String _subgraphXmlFileName;
  private List<NamedValue> _paramValues = new ArrayList<NamedValue>();
  private List<Name> _dependsNames = new ArrayList<Name>();
  private String _groupName;
  
  //TODO Java6 @Override
  public String getBaseName() {
    return _baseName;
  }
  
  //TODO Java6 @Override
  public void setName(String baseName) {
    _baseName = baseName;
  }
  
  //TODO Java6 @Override
  public String getSourceXmlFileName() {
    return _sourceXmlFileName;
  }
  
  //TODO Java6 @Override
  public void setSourceXmlFileName(String sourceXmlFileName) {
    _sourceXmlFileName = sourceXmlFileName;
  }
  
  //TODO Java6 @Override
  public String getSubgraphXmlFileName() {
    return _subgraphXmlFileName;
  }
  
  //TODO Java6 @Override
  public void setXmlFile(String subgraphXmlFileName) {
    _subgraphXmlFileName = subgraphXmlFileName;
  }
  
  //TODO Java6 @Override
  public List<Name> getDependsNames() {
    return _dependsNames;
  }
  
  //TODO Java6 @Override
  public void addParamValue(NamedValue paramValue) {
    _paramValues.add(paramValue);
  }
  
  //TODO Java6 @Override
  public void addDependsName(Name dependsName) {
    _dependsNames.add(dependsName);
  }
  
  //TODO Java6 @Override
  public void addDependsGlobalName(Name dependsName) {
    // don't differentiate between global, local, and external steps
    // TODO is this correct?
    addDependsName(dependsName);
  }
  
  //TODO Java6 @Override
  public void addDependsExternalName(Name dependsName) {
    // don't differentiate between global and local steps
    // TODO is this correct?
    addDependsName(dependsName);
  }
  
  //TODO Java6 @Override
  public String getGroupName() {
    return _groupName;
  }

  //TODO Java6 @Override
  public void setGroupName(String groupName) {
    _groupName = groupName;
  }

//TODO Java6 @Override
  public String toString() {
    return _baseName + " " + _dependsNames;
  }

}
