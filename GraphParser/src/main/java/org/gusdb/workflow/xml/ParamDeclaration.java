package org.gusdb.workflow.xml;

public class ParamDeclaration {
  String name;
  String deflt = null;
  public void setName(String n) { name = n; }
  public void setDefault(String d) { deflt = d; }
  public String getName() { return name; }
  public String getDefault() { return deflt; }
}
