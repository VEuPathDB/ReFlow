package org.gusdb.workflow.xml;

public class ParamDeclaration {

  private String _name;
  private String _default = null;

  public void setName(String name) { _name = name; }
  public void setDefault(String defaultValue) { _default = defaultValue; }
  public String getName() { return _name; }
  public String getDefault() { return _default; }

}
