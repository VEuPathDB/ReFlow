package org.gusdb.workflow;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.gusdb.fgputil.CliUtil;
import org.gusdb.workflow.RunnableWorkflow.RunnableWorkflowGraphClassFactory;

public class IllegalGraphReport {

  private static Options declareOptions() {
    Options options = new Options();
    CliUtil.addOption(options, "h", "Workflow homedir", true);
    return options;
  }

  // //////////////////////////////////////////////////////////////////////
  // Static methods
  // //////////////////////////////////////////////////////////////////////

  public static void main(String[] args) throws Exception {
    String cmdName = System.getProperty("cmdName");

    // parse command line
    Options options = declareOptions();
    String cmdlineSyntax = cmdName + " -h workflow_home_dir";
    String cmdDescrip = "Get a report of illegal graph changes.  This is useful when you update the graph in an existing workflow.";
    String usageNotes = "To use this command, set up a new $GUS_HOME which has the version of the graph you want to check.  (Don't forget to generate from dataset classes.)  In that $GUS_HOME, set the gus.config to point to the database that the previous workflow was run on.  Run this command from the new $GUS_HOME.";
    CommandLine cmdLine = CliUtil.parseOptions(cmdlineSyntax, cmdDescrip, usageNotes, options, args);

    String homeDirName = cmdLine.getOptionValue("h");

    System.err.println("Initializing...");
    RunnableWorkflow runnableWorkflow = new RunnableWorkflow(homeDirName);
    WorkflowGraph<RunnableWorkflowStep> rootGraph = WorkflowGraphUtil.constructFullGraph(
        new RunnableWorkflowGraphClassFactory(), runnableWorkflow);
    runnableWorkflow.setWorkflowGraph(rootGraph);

    if (!runnableWorkflow.workflowTableInitialized()) {
      System.out.println("Workflow not in database yet.  Are you sure you are running the right report?");
    }
    else {
      rootGraph.inDbExactly(false);
      System.out.println("The graph has no illegal changes.  Woohoo.");
    }
  }
}
