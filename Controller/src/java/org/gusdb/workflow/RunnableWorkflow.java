package org.gusdb.workflow;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.FieldPosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.io.File;

/*
   to do
   - integrate resource pipeline
   - possibly support taking DONE steps offline.  this will require recursion.
   - generate step documentation
   - whole system documentation
   - get manual confirm on -reset

  
   Workflow object that runs in two contexts:
     - step reporter
     - controller
  
   Controller theoretical issues
  
   Race conditions
   the controller gets a snapshot of the db each cycle. one class of possible
   race conditions is if there are external changes since the snapshot was
   taken. the controller only updates the state in the database if the state
   in the db is the same as in memory, ie, the same as in the snapshot.  this
   preserves any external change to be seen by the next cycle.
  
   this leaves the problem of (a) more than one external change happening
   within a cycle.  but, there are no transitory states that matter. The only
   consequence is that the intermediate state won't be logged.  the only
   one of these which could realistically happen in the time frame of a cycle
   is the transition from RUNNING to DONE, if a step executes quickly.  In this
   case, the log will show only DONE, without ever showing RUNNING, which is ok.
  
   and also the problem of (b) that the controller could not write the step's
   state because the state had changed since the snapshot.  the controller
   only writes the ON_DECK and FAILED states.  the next cycle will handle
   these correctly.
*/

public class RunnableWorkflow extends Workflow<RunnableWorkflowStep>{
    private int runningCount;

    final static String nl = System.getProperty("line.separator");

    public RunnableWorkflow(String homeDir) throws FileNotFoundException, IOException {
        super(homeDir);
        initHomeDir(); // initialize workflow home directory, if needed
    }

    // run the controller
    void run(boolean testOnly) throws Exception {

        initDb(true, testOnly);    // write workflow to db, if not already there

        getDbSnapshot();           // read state of Workflow and WorkflowSteps

	readOfflineFile();         // read start-up offline requests
	
        readStopAfterFile();       // read start-up offline requests
        
	setRunningState(testOnly); // set db state. fail if already running

        initializeUndo(testOnly);  // unless undoStepName is null

	// start polling
	while (true) {
	    getDbSnapshot();
	    if (handleStepChanges(testOnly)) break;  // return true if all steps done
	    findOndeckSteps();
	    fillOpenSlots(testOnly);
	    Thread.sleep(2000);
	    cleanProcesses();
	}
    }
    
    // write the workflow and steps to the db
    protected void initDb(boolean updateXmlFileDigest, boolean testmode) throws SQLException, IOException, Exception, NoSuchAlgorithmException {

        boolean stepTableEmpty = initWorkflowTable(updateXmlFileDigest, testmode);
        initWorkflowStepTable(stepTableEmpty);
    }

    private boolean initWorkflowTable(boolean updateXmlFileDigest, boolean testmode) throws SQLException, IOException, Exception, NoSuchAlgorithmException {
        
        boolean uninitialized = !workflowTableInitialized();
        if (uninitialized) {
        
            name = getWorkflowConfig("name");
            version = getWorkflowConfig("version");
            test_mode = new Boolean(testmode);

            log("Initializing workflow "
                    + "'" + name + " " + version + "' in database");
            
            if (!checkNewWorkflowHomeDir()) {
                error(nl + "Error: The data/ and steps/ directories are not empty, but your workflow does not exist in the database.  This suggests you have mistakenly changed databases.  Check your gus.config file." + nl);
            }
                
            // write row to Workflow table
            String sql = "select apidb.Workflow_sq.nextval from dual";
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = getDbConnection().createStatement();
                rs = stmt.executeQuery(sql);
                rs.next();
                workflow_id = rs.getInt(1);
            } finally {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
            }
            int testint = test_mode? 1 : 0;
            sql = "INSERT INTO apidb.workflow (workflow_id, name, version, test_mode)"  
                + " VALUES (" + workflow_id + ", '" + name + "', '" + version + "', " + testint + ")";
            executeSqlUpdate(sql);
        }
        
        return uninitialized;
    }
    
    private void setInitializingStepTableFlag(boolean initializing) throws FileNotFoundException, SQLException, IOException {
        int i = initializing? 1 : 0;
        String sql = "UPDATE apidb.workflow" +
        " SET initializing_step_table = " + i +
        " WHERE workflow_id = " + workflow_id;
        executeSqlUpdate(sql);
    }
    
    private void  initWorkflowStepTable(boolean stepTableEmpty) throws SQLException, IOException, Exception, NoSuchAlgorithmException {

        setInitializingStepTableFlag(true);
 
        // see if our current graph is already in db (exactly)
        // if so, no need to update db. otherwise, log the differences.
        // throw an error if any DONE or FAILED steps are changed
        if (workflowGraph.inDbExactly(stepTableEmpty)) 
            log("Graph in XML matches graph in database.  No need to update database.");

        else {
            if (stepTableEmpty) {
                if (undoStepName != null) error("Workflow has never run.  Undo not allowed.");
            } else {
                // can't allow changes to graph if already in undo mode, because it
                // is just too confusing.  they are allowed if the user is asking to
                // start an undo mode run, but the flow has not yet been converted
                // (ie, we are about to set undo_step_id in the db, but its not set yet)
                if (undo_step_id != null) error("Workflow graph in XML has changed.  Not allowed while in UNDO mode.");

                if (checkForRunningOrFailedSteps()) 
                    error("Workflow graph in XML has changed while there are steps in state RUNNING or FAILED." + nl
                            + "Please use workflowstep -l to find these states." + nl
                            + "Please wait until all RUNNING states are complete." + nl
                            + "For FAILED states, correct their problems and use workflowstep to set them to 'ready'.");

                log("Workflow graph in XML has changed.  Updating DB with new graph.");

                // if graph has changed, remove all READY/ON_DECK steps from db
                workflowGraph.removeReadyStepsFromDb();   
            }
 
            // write all steps to WorkflowStep table
            // for steps that are already there, update the depthFirstOrder
            Set<String> stepNamesInDb = workflowGraph.getStepNamesInDb();
            PreparedStatement insertStepPstmt = WorkflowStep.getPreparedInsertStmt(getDbConnection(), workflow_id);
            PreparedStatement updateStepPstmt = WorkflowStep.getPreparedUpdateStmt(getDbConnection(), workflow_id);
            try {
                for (WorkflowStep step : workflowGraph.getSortedSteps()) {
                    step.initializeStepTable(stepNamesInDb, insertStepPstmt, updateStepPstmt);
                }
            } finally {
                updateStepPstmt.close();
                insertStepPstmt.close();
            }

            // update steps in memory, to get their new IDs
            getStepsDbState();
        }
        setInitializingStepTableFlag(false);        
    }
    
    private void initializeUndo(boolean testOnly) throws SQLException, IOException, InterruptedException {
        
        if (undo_step_id == null && undoStepName == null) return; 
         
        if (undoStepName == null) error("An undo is in progress.  Cannot run the workflow in regular mode.");

	log("Running UNDO of step " + undoStepName);

        // if not already running undo
        if (undo_step_id == null) {
            
            // confirm that no steps are running   
            handleStepChanges(testOnly);
            List<RunnableWorkflowStep> runningSteps = new ArrayList<RunnableWorkflowStep>();
            for (RunnableWorkflowStep step : workflowGraph.getSteps()) {
                if (step.getState() != null && step.getState().equals(RUNNING))
                    runningSteps.add(step);
            }
            if (runningSteps.size() != 0) {
                String errStr = null;
                for (RunnableWorkflowStep step: runningSteps) {
                    errStr += step.getFullName() + nl;
                }
                if (errStr != null)
                    error("The following steps are running.  Can't start an undo while steps are running.  Wait for all steps to complete (or kill them), and try to run undo again" + nl +errStr);
            }
            
            // find the step based on its name, and set undo_step_id       
            for (RunnableWorkflowStep step : workflowGraph.getSteps()) {
                if (step.getFullName().equals(undoStepName)) {
                    undo_step_id = step.getId();
                    if (step.getUndoRoot() != null) 
                        error("This step may not be the root of an undo. Instead use " + step.getUndoRoot());
                    break;
                }
            }
            if (undo_step_id == null) error("Step name '" + undoStepName + "' is not found");
            
            // set undo_step_id in workflow table         
            String sql = "UPDATE apidb.Workflow" + nl
            + "SET undo_step_id = '" + undo_step_id + "'" + nl
            + "WHERE workflow_id = " + workflow_id;
            executeSqlUpdate(sql);
        } 
        
        // if already running undo
        else {
            // confirm that step name does not conflict with current undo step
            for (RunnableWorkflowStep step : workflowGraph.getSteps()) {
                if (undo_step_id == step.getId() && !step.getFullName().equals(undoStepName))
                    error("Step '" + undoStepName + "' does not match '" + step.getFullName()
                            + "' which is currently the step being undone");
            }
        }
            
        // invert and trim graph
        workflowGraph.convertToUndo();
	log("vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv");
	log("Steps in the Undo Graph:");
	log(workflowGraph.getStepsAsString());
	log("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    }

    // iterate through steps, checking on changes since last snapshot
    // while we're passing through, count how many steps are running
    private boolean handleStepChanges(boolean testOnly) throws SQLException, IOException, InterruptedException {

	runningCount = 0;
	boolean notDone = false;
	for (RunnableWorkflowStep step : workflowGraph.getSteps()) {
	    runningCount += step.handleChangesSinceLastSnapshot(this);
	    notDone |= !step.getOperativeState().equals(DONE);
	}
	if (!notDone) setDoneState(testOnly);
	return !notDone;
    }

    private void findOndeckSteps() throws SQLException, IOException {
	for (RunnableWorkflowStep step : workflowGraph.getSteps()) {
	    step.maybeGoToOnDeck();
	}
    }

    private void fillOpenSlots(boolean testOnly) throws IOException, SQLException {
	for (RunnableWorkflowStep step : workflowGraph.getSortedSteps()) {
	    String[] loadTypes = step.getLoadTypes();
	    boolean okToRun = true;
	    for (String loadType : loadTypes) {
	        if (runningLoadTypes.get(loadType) != null && runningLoadTypes.get(loadType) >= getLoadBalancingConfig(loadType)) {
	            okToRun = false;
	            break;
	        }
	    }
	    if (okToRun) {
	        int slotsUsed = step.runOnDeckStep(this, testOnly);	  
	        for (String loadType : loadTypes) {
	            Integer f = runningLoadTypes.get(loadType);
	            f = f == null? 0 : f;
	            runningLoadTypes.put(loadType, f + slotsUsed);
	        }
	    }
	}
    }

    private void readOfflineFile() throws IOException, java.lang.InterruptedException {
        readStepStateFile("initOfflineSteps", "offline");
    }

    private void readStopAfterFile() throws IOException, java.lang.InterruptedException {
        readStepStateFile("initStopAfterSteps", "stopafter");
    }

    private void readStepStateFile(String file, String state) throws IOException, java.lang.InterruptedException {
        String filename = getHomeDir() + "/config/" + file;
        File f = new File(filename);
        if (!f.exists()) error("Required config file " + filename + " does not exist");
        String[] cmd = {"workflowstep", "-h", getHomeDir(),
                        "-f", filename, state};
        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
	process.destroy();
    }


    private void setRunningState(boolean testOnly) throws SQLException, IOException, java.lang.InterruptedException {

        if (!test_mode && testOnly) error("Cannot run with '-t'.  Already running with '-r'");
        if (test_mode && !testOnly) error("Cannot run with '-r'.  Already running with '-t'");
        
        String hostname = java.net.InetAddress.getLocalHost().getHostName();

        if (host_machine != null && !host_machine.equals(hostname)) {
            error(nl + "Error: You are running on " + hostname + " but the workflow was last run on " 
                    + host_machine + "." + nl + nl
                    + "Please go to " + host_machine + 
                    " and run the ps -u command to confirm that no workflow process are running there.  It is CRITICAL that there be none." + nl + nl
                    + "If the controller is running on " + host_machine
                    + ", you must kill it to run here.  If workflowstepwrap processes are running, then you must either wait until they complete or kill them." + nl + nl
                    + "If there are NO PROCESSES RUNNING on " + host_machine + " then it is safe to run here.  Use the workflow -m option to let the controller know you are changing machines." + nl);
        }
        
        if (state != null && process_id != null && state.equals(RUNNING)) {
	    String[] cmd = {"ps", "-p", process_id};
	    Process process = Runtime.getRuntime().exec(cmd);
	    process.waitFor();
	    if (process.exitValue() == 0)
	        error("workflow already running (" + process_id + ")");
	    process.destroy();
	}

	String processId = getProcessId(); 

	if (testOnly) log("TESTING workflow....");

	String msg = "Setting workflow state to " + RUNNING
	    + " (host machine = " + hostname + " process id = " + processId + ")";
	log(msg);
	System.err.println(msg);
	
	String sql = "UPDATE apidb.Workflow" + nl
	    + "SET state = '" + RUNNING + "', process_id = " + processId
	    + ", host_machine = '" + hostname + "'" +  nl
	    + "WHERE workflow_id = " + workflow_id;
	executeSqlUpdate(sql);
    }

    private void setDoneState(boolean testOnly) throws SQLException, IOException {

        String doneFlag = "state = '" + DONE + "'";
        if (undo_step_id != null) doneFlag = "undo_step_id = NULL";

        String sql = "UPDATE apidb.Workflow "  
            + "SET " + doneFlag + ", process_id = NULL"  
            + " WHERE workflow_id = " + getId();
	executeSqlUpdate(sql);
	
	sql = "UPDATE apidb.WorkflowStep "
	    + "SET undo_state = NULL, undo_state_handled = 1 " 
	    + "WHERE workflow_id = " + workflow_id;
	executeSqlUpdate(sql); 
	
	String what = "Workflow";
	if (undo_step_id != null) what = "Undo of " + undoStepName;
	log(what + " " + (testOnly? "TEST " : "") + DONE);
    }

    /*
     * from http://blog.igorminar.com/2007/03/one-more-way-how-to-get-current-pid-in.html
     */
    private String getProcessId() throws IOException, InterruptedException {
        byte[] bo = new byte[100];
        String[] cmd = {"bash", "-c", "echo $PPID"};
        Process p = Runtime.getRuntime().exec(cmd);
	p.waitFor();
        p.getInputStream().read(bo);	
	p.destroy();
        return new String(bo).trim();
    }
    
    private boolean checkForRunningOrFailedSteps() throws SQLException, IOException {
        Statement stmt = null;
        ResultSet rs = null;
        String sql = "select count(*) from apidb.WorkflowStep where workflow_id = "
            + workflow_id
            + " and state in ('RUNNING', 'FAILED')";
            
        try {
            stmt = getDbConnection().createStatement();
            rs = stmt.executeQuery(sql);
            rs.next();
            Integer count = rs.getInt(1);
            return count != 0;
        } finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }
    }
    
    private boolean checkNewWorkflowHomeDir() {
        File stepDir = new File(getHomeDir() + "/steps");
        File dataDir = new File(getHomeDir() + "/data");
        return stepDir.list().length == 0 && dataDir.list().length == 0;
    }
    
    void log(String msg) throws IOException {
        String logFileName = getHomeDir() + "/logs/controller.log";
        PrintWriter writer = new PrintWriter(new FileWriter(logFileName, true));
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
        StringBuffer buf = sdf.format(new java.util.Date(), new StringBuffer(),
                                      new FieldPosition(0));

        writer.println(buf + "  " + msg + nl);
        writer.close();
    }
}
