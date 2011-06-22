package org.gusdb.workflow;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;

import org.gusdb.workflow.xml.WorkflowNode;
import org.gusdb.workflow.xml.WorkflowXmlContainer;

public class Utilities {

  private final static String NL = System.getProperty("line.separator");

  static void runCmd(String cmd) throws IOException, InterruptedException {
    Process process = Runtime.getRuntime().exec(cmd);
    process.waitFor();
    if (process.exitValue() != 0)
      error("Failed with status $status running: " + NL + cmd);
    process.destroy();
  }

  static void error(String msg) {
    System.err.println(msg);
    System.exit(1);
  }

  public static String substituteVariablesIntoString(String string,
      Map<String, String> variables) {
    if (string.indexOf("$$") == -1)
      return string;
    String newString = string;
    for (String variableName : variables.keySet()) {
      String variableValue = variables.get(variableName);
      newString = newString.replaceAll("\\$\\$" + variableName + "\\$\\$",
          Matcher.quoteReplacement(variableValue));
    }
    return newString;
  }

  public static String substituteMacrosIntoString(String string,
      Map<String, String> macros) {
    if (string.indexOf("@@") == -1)
      return string;
    String newString = string;
    for (String variableName : macros.keySet()) {
      String variableValue = macros.get(variableName);
      newString = newString.replaceAll("\\@\\@" + variableName + "\\@\\@",
          Matcher.quoteReplacement(variableValue));
    }
    return newString;
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends WorkflowNode, S extends WorkflowXmlContainer<T>>
  Class<S> getXmlContainerClass(Class<T> nodeClass, Class<S> containerClass)
      throws InstantiationException, IllegalAccessException {
    S instance = containerClass.newInstance();
    return (Class<S>)instance.getClass();
  }

}
