package org.gusdb.workflow;

import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.gusdb.fgputil.CliUtil;

public class IllegalGraphReport {

    private static Options declareOptions() {
        Options options = new Options();

        CliUtil.addOption(options, "h", "Workflow homedir (see below)", true);
        return options;
    }


    // //////////////////////////////////////////////////////////////////////
    // Static methods
    // //////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws Exception {
        String cmdName = System.getProperty("cmdName");

        // parse command line
        Options options = declareOptions();
        String cmdlineSyntax = cmdName
	    + " -h workflow_home_dir";
        String cmdDescrip = "Get a report of illegal graph changes.  This is useful when you update the graph in an existing workflow.";
	String usageNotes = "";
        CommandLine cmdLine = CliUtil.parseOptions(cmdlineSyntax, cmdDescrip,
						   usageNotes, options, args);

        String homeDirName = cmdLine.getOptionValue("h");

	System.err.println("Initializing...");
	RunnableWorkflow runnableWorkflow = new RunnableWorkflow(homeDirName);

	Class<RunnableWorkflowStep> stepClass = RunnableWorkflowStep.class;
	Class<WorkflowGraph<RunnableWorkflowStep>> containerClass = 
	    Utilities.getXmlContainerClass(RunnableWorkflowStep.class, WorkflowGraph.class);

	WorkflowGraph<RunnableWorkflowStep> rootGraph = 
	    WorkflowGraphUtil.constructFullGraph(stepClass, containerClass, runnableWorkflow);
	runnableWorkflow.setWorkflowGraph(rootGraph);

	if (!runnableWorkflow.workflowTableInitialized()) {
	    System.out.println("Workflow not in database yet.  Are you sure you are running the right report?");
	} else {
	    
	}
    }

}
