package org.gusdb.workflow;

import java.sql.Connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.gusdb.fgputil.CliUtil;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.workflow.Workflow.WorkflowGraphClassFactory;

public class WorkflowParser {

    public static void main(String[] args) throws Exception {
        String cmdName = System.getProperty("cmdName");

        // process args
        Options options = declareOptions();
        String cmdlineSyntax = cmdName + " -h workflowDir";
        String cmdDescrip = "Parse and print out a workflow xml file.";
        CommandLine cmdLine = CliUtil.parseOptions(cmdlineSyntax, cmdDescrip,
                "", options, args);
        String homeDirName = cmdLine.getOptionValue("h");

        // create a parser, and parse the model file
        try (DatabaseInstance db = Workflow.getDb();
             Connection conn = db.getDataSource().getConnection()) {
          Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(homeDirName, conn, db.getPlatform());
          WorkflowGraph<WorkflowStep> rootGraph = WorkflowGraphUtil.constructFullGraph(
              new WorkflowGraphClassFactory(), workflow);
          workflow.setWorkflowGraph(rootGraph);

          // print out the model content
          System.out.println(rootGraph.toString());
        }
    }

    private static Options declareOptions() {
        Options options = new Options();

        CliUtil.addOption(options, "h", "", true);

        return options;
    }

}
