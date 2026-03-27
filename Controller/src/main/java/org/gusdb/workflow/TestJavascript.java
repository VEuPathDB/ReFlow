package org.gusdb.workflow;

import org.gusdb.fgputil.script.JavaScript;

public class TestJavascript {

  public static void main(String[] args) throws Exception {
    JavaScript js = new JavaScript();
    boolean math = js.evaluateBooleanExpression("1 + 1 == 2");
    System.out.println("Does 1 + 1 = 2? " + math);
  }

}
