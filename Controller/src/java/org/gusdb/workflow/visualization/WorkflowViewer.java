package org.gusdb.workflow.visualization;

import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.GraphZoomScrollPane;
import edu.uci.ics.jung.visualization.control.CrossoverScalingControl;
import edu.uci.ics.jung.visualization.control.PluggableGraphMouse;
import edu.uci.ics.jung.visualization.control.PickingGraphMousePlugin;
import edu.uci.ics.jung.visualization.control.ScalingGraphMousePlugin;
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;

import java.io.IOException;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Paint;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.collections15.Transformer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import org.gusdb.workflow.Utilities;
import org.gusdb.workflow.Workflow;
import org.gusdb.workflow.WorkflowGraph;
import org.gusdb.workflow.WorkflowStep;
import org.gusdb.workflow.WorkflowXmlParser;

import org.gusdb.workflow.visualization.layout.AssistedTreeLayout;
import org.gusdb.workflow.visualization.mouse.PopupVertexEdgeMenuMousePlugin;
import org.gusdb.workflow.visualization.mouse.WorkflowViewerMousePlugin;

public class WorkflowViewer extends JFrame implements ActionListener {
    final static String nl = System.getProperty("line.separator");
    private static final String TITLE = "Workflow Viewer";
    private Workflow workflow;
    private GraphZoomScrollPane currentView;
    private Map<String, DirectedGraph<WorkflowStep, Integer>> viewGraphs;
    private Map<String, GraphZoomScrollPane> viewInstances;
    private Stack<String> history;
    private JLabel current;
    private JButton back;
    private JPanel applicationPane;
    private JPanel graphPane;
    private JFrame menuFrame;
    private Transformer<WorkflowStep,Shape> shaper;
    private Transformer<WorkflowStep,String> labeler;
    private Transformer<WorkflowStep,Font> fontStyler;
    private Transformer<WorkflowStep,Paint> painter;
    private Transformer<WorkflowStep,Stroke> outliner;

    public WorkflowViewer(String workflowDir) throws IOException {
	super(TITLE);
	history = new Stack<String>();
	viewGraphs = new HashMap<String, DirectedGraph<WorkflowStep,Integer>>();
	viewInstances = new HashMap<String, GraphZoomScrollPane>();
	workflow = new Workflow<WorkflowStep>(workflowDir);
	initApplicationPane();
	initTransformers();
	createViewFromWorkflow();
    }

    public void actionPerformed(ActionEvent e) {
	loadPreviousGraphView();
    }
    
    public void displayGraph(String key) {
	if (currentView != null) {
	    history.add(current.getText());
	}

	updateGraphView(key);
    }

    private void createViewFromWorkflow() {
	try {
	    // create root graph for this workflow
	    Class<WorkflowStep> stepClass = WorkflowStep.class;
	    WorkflowGraph<WorkflowStep> rootGraph = WorkflowGraph.constructFullGraph(stepClass, workflow);

	    createViewGraph(workflow.getWorkflowXmlFileName(), rootGraph.getRootSteps());

	    displayGraph(workflow.getWorkflowXmlFileName());
	}
	catch (Exception ex) {
	    //TODO:  Don't do this.
	    ex.printStackTrace();
	    System.exit(1);
	}
    }

    private void createViewGraph(String key, List<WorkflowStep> rootSteps) {
	DirectedGraph<WorkflowStep, Integer> graph = new DirectedSparseGraph<WorkflowStep, Integer>();
	int i = 0;

	for (WorkflowStep step : rootSteps) {
	    i = addEdgeToGraph(graph, i, null, step);
	}

	viewGraphs.put(key, graph);
	createViewInstance(key, graph);
    }

    private int addEdgeToGraph(DirectedGraph<WorkflowStep, Integer> graph, int edgeId, WorkflowStep parent, WorkflowStep child) {
	if (!child.getIsSubgraphReturn()) {
	    if (parent == null) {
		// Add this step
		graph.addVertex(child);
	    }
	    else {
		// Add an edge between this step and its parent
		graph.addEdge(new Integer(edgeId), parent, child);
	    }
	    
	    WorkflowStep nextParent = child;

	    if (nextParent.getIsSubgraphCall()) {
		int graphDepth = 1;
		// Create the subgraph
		createViewGraph(WorkflowViewer.getSubgraphKey(nextParent), nextParent.getChildren());
		// In this graph, skip ahead to the appropriate subgraph return
		while (graphDepth > 0) {
		    if (nextParent.getChildren().size() > 0) {
			nextParent = nextParent.getChildren().get(0);
			// TODO:  Is there a one-to-one call to return relationship?  If so this shouldn't be needed (but it is)
		    }

		    if (nextParent.getIsSubgraphCall()) {
			graphDepth++;
		    }
		    else if (nextParent.getIsSubgraphReturn()) {
			graphDepth--;
		    }
		}
	    }
	    
	    for (WorkflowStep grandchild : nextParent.getChildren()) {
		edgeId = addEdgeToGraph(graph, ++edgeId, child, grandchild);
	    }
	}

	return edgeId;
    }

    private void updateGraphView(String key) {
	current.setText(key);
	System.err.println("Loading graph: " + key);

	currentView = viewInstances.get(key);

	graphPane.removeAll();
	graphPane.add(currentView, BorderLayout.CENTER);
	this.validate();
	this.repaint();
    }

    private void createViewInstance(String key, DirectedGraph<WorkflowStep,Integer> graph) {
	AssistedTreeLayout<WorkflowStep,Integer> layout = new AssistedTreeLayout<WorkflowStep,Integer>(graph, 175, 75);

	// sets the initial size of the layout space
	// The VisualizationViewer is parameterized by the vertex and edge types
	
	VisualizationViewer vv = new VisualizationViewer(layout);
	vv.getRenderContext().setVertexShapeTransformer(shaper);
	vv.getRenderContext().setVertexLabelTransformer(labeler);
	vv.getRenderContext().setVertexFontTransformer(fontStyler);
	vv.getRenderContext().setVertexFillPaintTransformer(painter);
	vv.getRenderContext().setVertexStrokeTransformer(outliner);
	
	vv.getRenderer().getVertexLabelRenderer().setPosition(Position.CNTR);
	
	// Step details popup is disabled for now.
	//PopupVertexEdgeMenuMousePlugin mousePlugin = new PopupVertexEdgeMenuMousePlugin();
	//mousePlugin.setVertexPopup(new MyMouseMenus.WorkflowStepMenu(menuFrame));
	
	PluggableGraphMouse gm = new PluggableGraphMouse();
	gm.add(new ScalingGraphMousePlugin(new CrossoverScalingControl(), 0, 1.1f, 0.9f));
	gm.add(new WorkflowViewerMousePlugin(this));
	//gm.add(mousePlugin);
	gm.add(new PickingGraphMousePlugin());
	
	vv.setGraphMouse(gm);
	
	viewInstances.put(key, new GraphZoomScrollPane(vv));
    }
    
    private void loadPreviousGraphView() {
	if (history.size() > 0) {
	    updateGraphView(history.pop());
	}
	else {
	    JOptionPane.showMessageDialog(this, "You are already looking at the first workflow graph.");
	}
    }

    private void initTransformers() {
	shaper = new Transformer<WorkflowStep,Shape>() {
	    public Shape transform(WorkflowStep vertex) {
		return new Rectangle(150,50);
	    }
	};

	labeler = new Transformer<WorkflowStep,String>() {
	    public String transform(WorkflowStep vertex) {
		return vertex.getBaseName();
	    }
	};
	
	fontStyler = new Transformer<WorkflowStep,Font>() {
	    public Font transform(WorkflowStep vertex) {
		return new Font("Lucida Sans Regular", Font.PLAIN, 10);
	    }
	};
	
	painter = new Transformer<WorkflowStep,Paint>() {
	    public Paint transform(WorkflowStep vertex) {
		return Color.WHITE;
	    }
	};
	
	outliner = new Transformer<WorkflowStep,Stroke>() {
	    public Stroke transform(WorkflowStep vertex) {
		if (vertex.getIsSubgraphCall()) {
		    return new BasicStroke(3f);
		}
		return new BasicStroke(1f);
	    }
	};
    }

    private void initApplicationPane() {
	applicationPane = new JPanel(new GridBagLayout());
	GridBagConstraints c = new GridBagConstraints();

	back = new JButton("Back");
	back.addActionListener(this);
	c.fill = GridBagConstraints.HORIZONTAL;
	c.gridx = 0;
	c.gridy = 0;
	c.weighty = 0;
	applicationPane.add(back, c);

	current = new JLabel();
	c.fill = GridBagConstraints.HORIZONTAL;
	c.anchor = GridBagConstraints.CENTER;
	c.gridx = 1;
	c.gridy = 0;
	c.weightx = 0.6;
	applicationPane.add(current, c);

	graphPane = new JPanel(new BorderLayout());
	c.fill = GridBagConstraints.BOTH;
	c.anchor = GridBagConstraints.SOUTHEAST;
	c.gridx = 0;
	c.gridy = 1;
	c.weighty = 1;
	c.gridwidth = 2;
	applicationPane.add(graphPane, c);

	menuFrame = new JFrame();

	this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	this.add(applicationPane);
	this.setSize(new Dimension(1024,768));
	this.setVisible(true);
	this.validate();
    }

    public static void main(String[] args) {
	String cmdName = System.getProperty("cmdName");

	// parse command line
	Options options = declareOptions();
	String cmdlineSyntax = cmdName + " -h workflow_home_dir";
	String cmdDescrip = "View a workflow graph.";
	CommandLine cmdLine =
	    Utilities.parseOptions(cmdlineSyntax, cmdDescrip, getUsageNotes(), options, args);
	 
	String homeDirName = cmdLine.getOptionValue("h");
	try {
	    new WorkflowViewer(homeDirName);
	}
	catch (Exception ex) {
	    Utilities.usage(cmdlineSyntax, cmdDescrip, getUsageNotes(), options);
	    System.exit(1);
	}
    }

    public static String getSubgraphKey(WorkflowStep step) {
	return step.getBaseName() + ": " + step.getSubgraphXmlFileName();
    }

    private static Options declareOptions() {
	Options options = new Options();

	Utilities.addOption(options, "h", "Workflow homedir (see below)", true);      

	return options;
    }

    private static String getUsageNotes() {
	return

	    nl 
	    + "Home dir must contain the following:" + nl
	    + "   config/" + nl
	    + "     initOfflineSteps   (steps to take offline at startup)" + nl
	    + "     loadBalance.prop   (configure load balancing)" + nl
	    + "     rootParams.prop    (root parameter values)" + nl
	    + "     stepsGlobal.prop   (global steps config)" + nl
	    + "     steps.prop         (steps config)" + nl
	    + "     workflow.prop      (meta config)" + nl
	    + nl + nl   
	    + nl + nl                        
	    + "Examples:" + nl
	    + nl     
	    + "  view a workflow:" + nl
	    + "    % workflowViewer -h workflow_dir" + nl;
    }
} 
