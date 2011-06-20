package org.gusdb.workflow.authoring.jung;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.gusdb.workflow.Name;
import org.gusdb.workflow.NamedValue;
import org.gusdb.workflow.Utilities;
import org.gusdb.workflow.Workflow;
import org.gusdb.workflow.WorkflowGraph;
import org.gusdb.workflow.WorkflowGraphUtil;
import org.gusdb.workflow.WorkflowNode;
import org.gusdb.workflow.WorkflowStep;

import edu.uci.ics.jung.algorithms.layout.BalloonLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateTree;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Forest;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.visualization.VisualizationViewer;

public class GraphDataBuilder {
  
  public static void main(String[] args) throws Exception  {
    try {
      // process args
      Options options = new Options();
      Utilities.addOption(options, "h", "", true);
      String cmdlineSyntax = GraphDataBuilder.class.getName() + " -h workflowDir";
      String cmdDescrip = "Parse and print out a workflow xml file.";
      CommandLine cmdLine =
          Utilities.parseOptions(cmdlineSyntax, cmdDescrip, "", options, args);
      String homeDirName = cmdLine.getOptionValue("h");
      File configDir = new File(homeDirName);
      if (!configDir.isDirectory() || !configDir.canRead()) {
        System.err.println("Error: workflowDir [" + homeDirName + "] is not a readable directory.");
        System.exit(1);
      }
  
      // create a parser, and parse the model file
      Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(configDir.getAbsolutePath());
      Class<WorkflowStep> stepClass = WorkflowStep.class;
      @SuppressWarnings("unchecked")
      Class<WorkflowGraph<WorkflowStep>> containerClass =
        Utilities.getXmlContainerClass(WorkflowStep.class, WorkflowGraph.class);
      WorkflowGraph<WorkflowStep> rootGraph = 
          WorkflowGraphUtil.constructFullGraph(stepClass, containerClass, workflow);
      workflow.setWorkflowGraph(rootGraph);
  
      // get all steps and sort by file, enable access by name
      List<? extends WorkflowNode> stepList = rootGraph.getSortedSteps();
      Map<String, WorkflowNode> stepMap = new HashMap<String, WorkflowNode>();
      for (WorkflowNode v : stepList) {
        stepMap.put(v.getBaseName(), v);
      }
      showGraph(stepMap);

      
      /*
      System.exit(0);
      
      // parse XML files specified and build graph data
      GraphDataBuilder graphData = new GraphDataBuilder(configDir);
      
      Graph<GraphVertex, DirectedEdge> entireGraph = graphData.getEntireGraph();
      */
      /*
      AssistedTreeLayout<GraphVertex, DirectedEdge> layout = new AssistedTreeLayout<GraphVertex, DirectedEdge>(
          entireGraph, 175, 75);
  
      // sets the initial size of the layout space
      // The VisualizationViewer is parameterized by the vertex and edge types
  
      VisualizationViewer<WorkflowStep, Integer> vv = new VisualizationViewer<WorkflowStep, Integer>(layout);
      vv.getRenderContext().setVertexShapeTransformer(shaper);
      vv.getRenderContext().setVertexLabelTransformer(labeler);
      vv.getRenderContext().setVertexFontTransformer(fontStyler);
      vv.getRenderContext().setVertexFillPaintTransformer(painter);
      vv.getRenderContext().setVertexStrokeTransformer(outliner);
  
      vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);
      */
      /*
      // render graph
      JFrame frame = new JFrame("Renderer");
      frame.setBounds(20, 20, 1000, 800);
      frame.setBackground(Color.BLACK);
      JPanel panel = new JPanel();
      panel.setBackground(Color.GREEN);
      frame.add(panel);
      
      Layout<GraphVertex, DirectedEdge> layout = new FRLayout<GraphVertex, DirectedEdge>(entireGraph);
      //Renderer r = new PluggableRenderer();
      VisualizationViewer<GraphVertex, DirectedEdge> viewer = new VisualizationViewer<GraphVertex, DirectedEdge>(layout);
      viewer.setBackground(Color.RED);
      panel.add(viewer, BorderLayout.CENTER);
      frame.validate();
      frame.repaint();
  
      frame.setVisible(true);
      */
    }
    catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
  
  /*********************** BEGIN INSTANCE DATA/METHODS ***********************/
  
  private Map<String, WorkflowNode> _allStepsMap;
  private Map<String, HashMap<String, WorkflowNode>> _stepMap;
  private Graph<WorkflowNode, DirectedEdge> _entireGraph;
  
  public GraphDataBuilder(File configDir) throws Exception {
    // create a parser, and parse the model file
    Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(configDir.getAbsolutePath());
    Class<WorkflowStep> stepClass = WorkflowStep.class;
    @SuppressWarnings("unchecked")
    Class<WorkflowGraph<WorkflowStep>> containerClass =
      Utilities.getXmlContainerClass(WorkflowStep.class, WorkflowGraph.class);
    WorkflowGraph<WorkflowStep> rootGraph = 
        WorkflowGraphUtil.constructFullGraph(stepClass, containerClass, workflow);
    workflow.setWorkflowGraph(rootGraph);

    // get all steps and sort by file, enable access by name
    init(rootGraph.getSortedSteps());
  }
  
  public GraphDataBuilder(List<? extends WorkflowNode> vertices) {
    init(vertices);
  }
  
  private void init(List<? extends WorkflowNode> vertices) {
    System.out.println("Initializing GraphDataBuilder with " + vertices.size() + " vertices:");
    System.out.println(vertices);
    _allStepsMap = new HashMap<String, WorkflowNode>();
    _stepMap = new HashMap<String, HashMap<String, WorkflowNode>>();
    for (WorkflowNode step : _allStepsMap.values()) {
      mapWorkflowStep(step);
    }
  }
  
  private void mapWorkflowStep(WorkflowNode step) {
    HashMap<String, WorkflowNode> nameMap = _stepMap.get(step.getSourceXmlFileName());
    if (nameMap == null) {
      nameMap = new HashMap<String, WorkflowNode>();
      _stepMap.put(step.getSourceXmlFileName(), nameMap);
    }
    nameMap.put(step.getBaseName(), step);
    _allStepsMap.put(step.getBaseName(), step);
  }
    
  // try to build a whole graph from all the steps
  public Graph<WorkflowNode, DirectedEdge> getEntireGraph() {
    if (_entireGraph == null) {
      synchronized(this) {
        if (_entireGraph == null) {
          _entireGraph = generateGraph(_allStepsMap);
        }
      }
    }
    return _entireGraph;
  }
  
  public Graph<WorkflowNode, DirectedEdge> getGraphForFile(String xmlFileName) {
    Map<String, WorkflowNode> vertices = _stepMap.get(xmlFileName);
    if (vertices == null) {
      return null;
    }
    return generateGraph(vertices);
  }
  
  private Graph<WorkflowNode, DirectedEdge> generateGraph(Map<String, WorkflowNode> vertices) {
    Collection<WorkflowNode> values = vertices.values();
    Graph<WorkflowNode, DirectedEdge> graph = new DirectedSparseGraph<WorkflowNode, DirectedEdge>();
    for (WorkflowNode vertex : values) {
      graph.addVertex(vertex);
    }
    for (WorkflowNode vertex : values) {
      for (Name dependencyName : vertex.getDependsNames()) {
        WorkflowNode dependency = vertices.get(dependencyName.toString());
        // TODO: decide if this is desired and/or necessary
        //if (dependency == null) {
        //  throw new IllegalStateException("Dependency name could not be found.  This should already have been checked.");
        //}
        graph.addEdge(new DirectedEdge(dependency, vertex), dependency, vertex, EdgeType.DIRECTED);
      }
    }
    return graph;
  }
  
  
  


  private static final int NUM_VERTICES = 10;

  public static class Vertex implements WorkflowNode {
    public String _name;
    public Vertex(String name) { _name = name; }
    //TODO: Java6 @Override
    public String getBaseName() { return _name; }
    //TODO: Java6 @Override
    public String getSourceXmlFileName() { return _name; }
    //TODO: Java6 @Override
    public String getSubgraphXmlFileName() { return _name; }
    //TODO: Java6 @Override
    public List<Name> getDependsNames() { return new ArrayList<Name>(); }

    //TODO: Java6 @Override
    public void setSourceXmlFileName(String fileName) {
      // TODO Auto-generated method stub 
    }
    //TODO: Java6 @Override
    public void addParamValue(NamedValue namedValue) {
      // TODO Auto-generated method stub
      
    }
    //TODO: Java6 @Override
    public void addDependsName(Name dependsName) {
      // TODO Auto-generated method stub
      
    }
    //TODO: Java6 @Override
    public void setName(String baseName) {
      // TODO Auto-generated method stub
      
    }
    //TODO: Java6 @Override
    public void setXmlFile(String fileName) {
      // TODO Auto-generated method stub
      
    }
    //TODO: Java6 @Override
    public void addDependsGlobalName(Name dependsName) {
      // TODO Auto-generated method stub
      
    }
  }
  
  public static void main2(String args[]) {
    /*
    if (args.length != 1) {
      System.out.println("USAGE: " + GraphDataBuilder.class.getName() + " <workflowXmlDir>");
      System.exit(1);
    }
    File xmlDir = new File(args[0]);
    if (!xmlDir.isDirectory() || !xmlDir.canRead()) {
      System.out.println("ERROR: " + xmlDir.getAbsolutePath() + " is not a readable directory.");
      System.exit(2);
    }
    File[] xmlFiles = xmlDir.listFiles(new FilenameFilter() {
      //TODO: Java6 @Override
      public boolean accept(File dir, String name) {
        return name.toLowerCase().endsWith(".xml");
      }});
    
    //.........was going to do more here.......
    */
    Map<String, WorkflowNode> vertices = new HashMap<String, WorkflowNode>();
    for (char c = 'a'; c < 'k'; c++) {
      WorkflowNode vertex = new Vertex(String.valueOf(c));
      vertices.put(String.valueOf(c), vertex);
    }
    
    //showGraph(vertices);
  }
    
  public static void showGraph(Map<String, ? extends WorkflowNode> vertices) {
    System.out.println("Generating Graph...");
    DirectedGraph<WorkflowNode, DirectedEdge> graph = new DirectedSparseGraph<WorkflowNode, DirectedEdge>();

    for (WorkflowNode vertex : vertices.values()) {
      graph.addVertex(vertex);
    }

    // create edges
    for (WorkflowNode vertex : vertices.values()) {
      for (Name dependencyName : vertex.getDependsNames()) {
        WorkflowNode dependency = vertices.get(dependencyName.toString());
        // TODO: decide if this is desired and/or necessary
        //if (dependency == null) {
        //  throw new IllegalStateException("Dependency name could not be found.  This should already have been checked.");
        //}
        graph.addEdge(new DirectedEdge(dependency, vertex), dependency, vertex, EdgeType.DIRECTED);
      }
    }
/*
    for (int i = 1; i < vertices.size(); i++) {
      DirectedEdge edge = new DirectedEdge(vertices.get(i-1), vertices.get(i));
      graph.addEdge(edge, vertices.get(i-1), vertices.get(i), EdgeType.DIRECTED);
    }
*/    
    // render graph
    JFrame frame = new JFrame("Renderer");
    frame.setBounds(20, 20, 1000, 800);
    JPanel panel = new JPanel();
    frame.add(panel);
    
    Forest<WorkflowNode, DirectedEdge> forest = new DelegateTree<WorkflowNode, DirectedEdge>(graph);
    Layout<WorkflowNode, DirectedEdge> layout = new BalloonLayout<WorkflowNode, DirectedEdge>(forest);
    //Layout<GraphVertex, DirectedEdge> layout = new FRLayout<GraphVertex, DirectedEdge>(graph);
    //Renderer r = new PluggableRenderer();
    VisualizationViewer<WorkflowNode, DirectedEdge> viewer = new VisualizationViewer<WorkflowNode, DirectedEdge>(layout);
    panel.add(viewer);

    frame.setVisible(true);
    
  }


}