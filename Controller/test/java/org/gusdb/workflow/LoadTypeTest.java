package org.gusdb.workflow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class LoadTypeTest {

  private static final String DEFAULT_LOAD_TYPE = "total";

  private WorkflowGraph<WorkflowStep> graph;
  private WorkflowStep emptyStep, labelStep, emptySub, labelSub;

  @BeforeEach
  public void createGraph() throws IOException {
    // create a workFlowGraph
    graph = new WorkflowGraph<>();

    // create a step that doesn't have load types
    emptyStep = new WorkflowStep();
    emptyStep.setName("empty-step");
    graph.addStep(emptyStep);

    // create a step that will be assigned load types
    labelStep = new WorkflowStep();
    labelStep.setName("label-step");
    graph.addStep(labelStep);

    // create a sub graph that doesn't have load types
    emptySub = new WorkflowStep();
    emptySub.setName("empty-sub");
    emptySub.setXmlFile(""); // trick the step to become a calling step
    graph.addStep(emptySub);

    // create a sub graph that will be assigned load types
    labelSub = new WorkflowStep();
    labelSub.setName("label-sub");
    labelSub.setXmlFile(""); // trink the step tp become a calling step
    graph.addStep(labelSub);

  }

  @Test
  public void testConditionalLoadTypes() {
    var type1 = "Type1";
    var type2 = "extra:Type2";

    // create a calling step for the graph, and declare labels
    var callingStep = new WorkflowStep();
    callingStep.setName("calling-step");
    callingStep.setXmlFile("");
    callingStep.setStepLoadTypes(
      "label-step: " + type1 + ",label-sub." + type2);

    graph.setCallingStep(callingStep);

    // verify the labels
    var loadTypes = emptyStep.getLoadTypes();
    Assertions.assertEquals(1, loadTypes.length);
    Assertions.assertEquals(DEFAULT_LOAD_TYPE, loadTypes[0]);

    loadTypes = labelStep.getLoadTypes();
    Assertions.assertEquals(2, loadTypes.length);
    Assertions.assertEquals(DEFAULT_LOAD_TYPE, loadTypes[0]);
    Assertions.assertEquals(type1, loadTypes[1]);

    loadTypes = emptySub.getLoadTypes();
    Assertions.assertEquals(1, loadTypes.length);
    Assertions.assertEquals(DEFAULT_LOAD_TYPE, loadTypes[0]);

    loadTypes = labelSub.getLoadTypes();
    Assertions.assertEquals(2, loadTypes.length);
    Assertions.assertEquals(DEFAULT_LOAD_TYPE, loadTypes[0]);
    Assertions.assertEquals(type2, loadTypes[1]);
  }

  @Test
  public void testInvalidLoadTypeWithInvalidPath() {
    // create a calling step for the graph, and declare labels
    Assertions.assertThrows(RuntimeException.class, () -> {
      var callingStep = new WorkflowStep();
      callingStep.setName("calling-step");
      callingStep.setXmlFile("");
      callingStep.setStepLoadTypes("label-step.label-step:Type");

      graph.setCallingStep(callingStep);
    });
  }

  @Test
  public void testInvalidLoadTypeWithoutPath() {
    Assertions.assertThrows(RuntimeException.class, () -> {
      // create a calling step for the graph, and declare labels
      var callingStep = new WorkflowStep();
      callingStep.setName("calling-step");
      callingStep.setXmlFile("");
      callingStep.setStepLoadTypes("label-sub:Type");

      graph.setCallingStep(callingStep);
    });
  }
}
