package org.gusdb.workflow;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.gusdb.fgputil.CliUtil;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;

/**
 * 
 * @param <T>
 *            The type of step contained by this workflow, ie, runnable or not.
 * 
 *            This is the Workflow base class. It knows about the workflow home
 *            dir, configuration, graph, steps and all the state in the
 *            database. Its only actions are reporting. It cannot run (subclass
 *            does that).
 */
public class Workflow<T extends WorkflowStep> {

    // static
    // my parents are not done yet -- default state
    public static final String READY = "READY";
    // my parents are done, but there is no slot for me
    public static final String ON_DECK = "ON_DECK";
    public static final String FAILED = "FAILED";
    public static final String DONE = "DONE";
    public static final String RUNNING = "RUNNING";
    public static final String ALL = "ALL";
    final static String nl = System.getProperty("line.separator");
    
    public static final String LOAD_THROTTLE_FILE = "loadThrottle.prop";
    public static final String FAIL_THROTTLE_FILE = "failThrottle.prop";

    // configuration
    private Connection dbConnection;
    private String homeDir;
    private Properties workflowProps; // from workflow config file
    protected Properties loadThrottleConfig = new Properties();
    protected Properties failThrottleConfig = new Properties();
    private String[] homeDirSubDirs = { "logs", "steps", "data", "backups" };
    protected String name;
    protected String version;
    protected String workflowTable;
    protected String workflowStepTable;
    protected String workflowStepParamValTable;
    protected String workflowStepTrackingTable;
    protected int maxRunningPerStepClass;
    protected int maxFailedPerStepClass;

    // persistent state
    protected Integer workflow_id;
    protected String state;
    protected Integer undo_step_id;
    protected String process_id;
    protected String host_machine;
    protected Boolean test_mode;

    // derived from persistent state
    protected Map<String, Integer> runningLoadTypeCounts; // running steps, by type tag
    protected Map<String, Integer> runningStepClassCounts; // running steps, by step class
    protected Map<String, Integer> failedFailTypeCounts; // failed steps, by type tag
    protected Map<String, Integer> failedStepClassCounts; // failed steps, by step class

    // input
    protected WorkflowGraph<T> workflowGraph; // the graph
    protected String undoStepName; // iff we are running undo

    // list of processes to clean
    private List<Process> bgdProcesses = new ArrayList<Process>();

    public Workflow(String homeDir) throws FileNotFoundException, IOException {
        this.homeDir = homeDir.replaceAll("/$", "");
        name = getWorkflowConfig("name");
        version = getWorkflowConfig("version");
        workflowTable = getWorkflowConfig("workflowTable");
        workflowStepTable = getWorkflowConfig("workflowStepTable");
        workflowStepParamValTable = getWorkflowConfig("workflowStepParamValueTable");
        workflowStepTrackingTable = getWorkflowConfig("workflowStepTrackingTable");
        maxRunningPerStepClass = Integer.parseInt(getWorkflowConfig("maxRunningPerStepClass")); 
        maxFailedPerStepClass = Integer.parseInt(getWorkflowConfig("maxFailedPerStepClass")); 
    }

    // ///////////////////////////////////////////////////////////////////////
    // Properties
    // ///////////////////////////////////////////////////////////////////////
    public void setWorkflowGraph(WorkflowGraph<T> workflowGraph) {
        this.workflowGraph = workflowGraph;
    }

    String getHomeDir() {
        return homeDir;
    }

    Integer getUndoStepId() {
        return undo_step_id;
    }

    String getUndoStepName() {
        return undoStepName;
    }

    String getWorkflowTable() {
        return workflowTable;
    }

    String getWorkflowStepTable() {
        return workflowStepTable;
    }

    String getWorkflowStepParamValTable() {
        return workflowStepParamValTable;
    }

    String getWorkflowStepTrackingTable() {
        return workflowStepTrackingTable;
    }

    String getName() {
        return name;
    }

    String getVersion() {
        return version;
    }

    // ////////////////////////////////////////////////////////////////////////
    // Initialization
    // ////////////////////////////////////////////////////////////////////////

    protected void initHomeDir() throws IOException {
        for (String dirName : homeDirSubDirs) {
            File dir = new File(getHomeDir() + "/" + dirName);
            if (!dir.exists()) dir.mkdir();
        }
        log("");
        log("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
        log("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
        log("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
        log("");
    }

    // /////////////////////////////////////////////////////////////////////////
    // Read from DB
    // /////////////////////////////////////////////////////////////////////////

    Integer getId() throws SQLException, FileNotFoundException, IOException {
        getDbState();
        return workflow_id;
    }

    protected void getDbSnapshot() throws SQLException, IOException {
        getDbState();
        getStepsDbState();
    }

    protected void getDbState() throws SQLException, FileNotFoundException,
            IOException {
        if (workflow_id == null) {
            String sql = "select workflow_id, state, undo_step_id, process_id, host_machine, test_mode"
                    + " from "
                    + workflowTable
                    + " where name = '"
                    + name
                    + "'"
                    + " and version = '" + version + "'";

            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = getDbConnection().createStatement();
                rs = stmt.executeQuery(sql);
                if (!rs.next())
                    error("workflow '" + name + "' version '" + version
                            + "' not in database");
                workflow_id = rs.getInt(1);
                state = rs.getString(2);
                undo_step_id = (rs.getObject(3) == null) ? null : rs.getInt(3);
                process_id = rs.getString(4);
                host_machine = rs.getString(5);
                test_mode = rs.getBoolean(6);
            }
            finally {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
            }
        }
    }

    // read all WorkflowStep rows into memory (and remember the prev snapshot)
    protected void getStepsDbState() throws SQLException,
            FileNotFoundException, IOException {
        String sql = WorkflowStep.getBulkSnapshotSql(workflow_id,
                workflowStepTable);

        // run query to get all rows from WorkflowStep for this workflow
        // stuff each row into the snapshot, keyed on step name
        Statement stmt = null;
        ResultSet rs = null;
        
        resetStepCounts();  // used for throttling
        
        try {
            stmt = getDbConnection().createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String stepName = rs.getString("NAME");
                WorkflowStep step = workflowGraph.getStepsByName().get(stepName);
                if (step == null) {
                    if (undoStepName == null) {
                        (new Throwable()).printStackTrace();
                        error("Engine can't find step with name '" + stepName
                                + "'");
                    } else {
                        continue;
                    }
                }
                step.setFromDbSnapshot(rs);
                if (step.getOperativeState() != null) {
                   if (step.getOperativeState().equals(RUNNING)) updateRunningStepCounts(step, 1);
                   else if (step.getOperativeState().equals(FAILED)) updateFailedStepCounts(step, 1);
                }
            }
        }
        finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }
    }

    private void resetStepCounts() {
      runningLoadTypeCounts = new HashMap<String, Integer>();
      runningStepClassCounts = new HashMap<String, Integer>(); 
      failedFailTypeCounts = new HashMap<String, Integer>();
      failedStepClassCounts = new HashMap<String, Integer>();
    }
    
    protected void updateRunningStepCounts(WorkflowStep step, int increment) {
      updateStepCounts(runningStepClassCounts, runningLoadTypeCounts, step.getLoadTypes(), step, increment);     
    }
    
    private void updateFailedStepCounts(WorkflowStep step, int increment) {
      updateStepCounts(failedStepClassCounts, failedFailTypeCounts, step.getFailTypes(), step, increment);     
    }
    
    private void updateStepCounts(Map<String, Integer> stepClassCounts, Map<String, Integer> typeCounts, String[] types, WorkflowStep step, int increment) {
      // update total
      Integer s = typeCounts.get(WorkflowStep.totalLoadType);
      s = s == null? 0 : s;
      typeCounts.put(WorkflowStep.totalLoadType, s + increment);

      // update step class count
      s = stepClassCounts.get(step.getStepClassName());
      s = s == null? 0 : s;
      stepClassCounts.put(step.getStepClassName(), s + increment);

      // and each tag
      for (String type : types) {
        Integer f = typeCounts.get(type);
        f = f == null ? 0 : f;
        typeCounts.put(type, f + increment);
      }      
    }
    
    protected boolean workflowTableInitialized() throws FileNotFoundException,
            IOException, SQLException {

        // don't bother if already in db
        String sql = "select workflow_id" + " from " + workflowTable
                + " where name = " + "'" + name + "'" + " and version = '"
                + version + "'";

        log("Checking if database already intialized");

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = getDbConnection().createStatement();
            rs = stmt.executeQuery(sql);
            if (rs.next()) return true;
        }
        finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }
        return false;
    }

    // //////////////////////////////////////////////////////////////////////
    // Utilities
    // //////////////////////////////////////////////////////////////////////

    /**
     * RunnableWorkflow overrides this to log to a real log
     * 
	 * @throws IOException if error occurs while logging
	 */
    void log(String msg) throws IOException {
        System.err.println(msg);
    }

    Connection getDbConnection() throws SQLException, FileNotFoundException,
            IOException {
        if (dbConnection == null) {
            String dsn = Utilities.getGusConfig("jdbcDsn");
            String login = Utilities.getGusConfig("databaseLogin");
            log("Connecting to " + dsn + " (" + login + ")");
            DatabaseInstance db = new DatabaseInstance(
                SimpleDbConfig.create(SupportedPlatform.ORACLE, dsn, login,
                    Utilities.getGusConfig("databasePassword")));
            dbConnection = db.getDataSource().getConnection();
            log("Connected");
        }
        return dbConnection;
    }

    void executeSqlUpdate(String sql) throws SQLException,
            FileNotFoundException, IOException {
        Statement stmt = getDbConnection().createStatement();
        try {
            stmt.executeUpdate(sql);
        }
        finally {
            stmt.close();
        }
    }

    String getWorkflowConfig(String key) throws FileNotFoundException,
            IOException {
        String configFileName = getHomeDir() + "/config/workflow.prop";
        if (workflowProps == null) {
            workflowProps = new Properties();
            workflowProps.load(new FileInputStream(configFileName));
        }
        String value = workflowProps.getProperty(key);
        if (value == null)
            error("Required property " + key
                    + " not found in workflow properties file: "
                    + configFileName);
        return value;
    }

  Integer getLoadThrottleConfig(String key) throws FileNotFoundException, IOException {
    return getThrottleConfig(key, loadThrottleConfig, LOAD_THROTTLE_FILE);
  }

  Integer getFailThrottleConfig(String key) throws FileNotFoundException, IOException {
    return getThrottleConfig(key, failThrottleConfig, FAIL_THROTTLE_FILE);
  }

  Integer getThrottleConfig(String key, Properties config, String file)
      throws FileNotFoundException, IOException {
    if (config.size() == 0) {
      FileInputStream f = new FileInputStream(getHomeDir() + "/config/" + file);
      config = new Properties();
      config.load(f);
      f.close();
      if (config.getProperty(WorkflowStep.totalLoadType) == null)
        error("File " + file + " must contain a property for " + WorkflowStep.totalLoadType);
    }
    String value = config.getProperty(key);
    if (value == null)
      return null;
    return new Integer(config.getProperty(key));
  }

    void error(String msg) {
        Utilities.error(msg);
    }

    public String getWorkflowXmlFileName() throws FileNotFoundException,
            IOException {
        Properties workflowProps = new Properties();
        workflowProps.load(new FileInputStream(getHomeDir()
                + "/config/workflow.prop"));
        return workflowProps.getProperty("workflowXmlFile");
    }

    void addBgdProcess(Process p) {
        bgdProcesses.add(p);
    }

    void cleanProcesses() {
        List<Process> clone = new ArrayList<Process>(bgdProcesses);
        for (Process p : clone) {
            boolean stillRunning = false;
            try {
                p.exitValue();
            }
            catch (IllegalThreadStateException e) {
                stillRunning = true;
            }
            if (!stillRunning) {
                p.destroy();
                bgdProcesses.remove(p);
            }
        }
    }

    // ////////////////////////////////////////////////////////////////
    // Actions
    // ////////////////////////////////////////////////////////////////

    // very light reporting of state of workflow (no steps)
    void quickReportWorkflow() throws SQLException, FileNotFoundException,
            IOException {
        getDbState();

        System.out.println("Workflow '" + name + " " + version + "'" + nl
                + "workflow_id:           " + workflow_id + nl
                + "state:                 " + state + nl
                + "undo_step:             " + undoStepName + nl
                + "process_id:            " + process_id + nl
                + "host_machine:          " + host_machine + nl);
    }

    // light reporting of state of workflow with steps
    void quickReportSteps(String[] desiredStates, boolean oneColumnOutput)
            throws SQLException, FileNotFoundException, IOException {
        getDbState();
        workflowStepTable = getWorkflowConfig("workflowStepTable");

        StringBuffer buf = new StringBuffer();
        for (String ds : desiredStates)
            buf.append("'" + ds + "',");

        String state_str = undo_step_id == null ? "state" : "undo_state";
        String sql = "select name, workflow_step_id," + state_str
                + ", end_time, CASE WHEN start_time IS NULL THEN -1 "
                + "  ELSE (nvl(end_time, SYSDATE) - start_time) * 24 "
                + "  END AS hours " + " from " + workflowStepTable
                + " where workflow_id = '" + workflow_id + "'" + " and "
                + state_str + " in(" + buf.substring(0, buf.length() - 1) + ")"
                + " order by end_time ASC, start_time ASC";

        Statement stmt = null;
        ResultSet rs = null;
        Formatter formatter = null;
        try {
            stmt = getDbConnection().createStatement();
            rs = stmt.executeQuery(sql);
            StringBuilder sb = new StringBuilder();
            formatter = new Formatter(sb);
            if (!oneColumnOutput) {
              formatter.format("%5$-17s %1$-6s %2$-8s %3$-12s  %4$s ", "SPENT",
                  "STATUS", "STEP_ID", "NAME", "END_AT");
              System.out.println(sb.toString());
            }
            while (rs.next()) {
                String nm = rs.getString("name");
                Integer ws_id = rs.getInt("workflow_step_id");
                String stat = rs.getString(state_str);
                Date endTime = rs.getTimestamp("end_time");
                double spent = rs.getFloat("hours");

                if (oneColumnOutput) System.out.println(nm);
                else {
                    sb = new StringBuilder();
                    formatter = new Formatter(sb);
                    if (spent != -1) {
                        int hour = (int) Math.floor(spent);
                        int minute = (int)Math.round((spent - hour) * 60);
                        if (endTime != null) {
                            formatter.format("%6$tm/%6$td/%6$ty %6$tH:%6$tM:%6$tS %1$03d:%2$02d %3$-8s %4$-12s  %5$s",
                                hour, minute, stat, ws_id, nm, endTime);
                        } else {
                            formatter.format("%6$-17s %1$03d:%2$02d %3$-8s %4$-12s  %5$s",
                                hour, minute, stat, ws_id, nm, "--");
                        }
                    } else {
                        formatter.format("%5$-17s %1$6s %2$-8s %3$-12s  %4$s", " ",
                                stat, ws_id, nm, " ");
                    }
                    System.out.println(sb.toString());
                }
            }
        }
        finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (formatter != null) formatter.close();
        }
    }

    // detailed reporting of steps
    void reportSteps(String[] desiredStates) throws Exception {
        if (!workflowTableInitialized())
            Utilities.error("Workflow not initialized.  Please run controller first.");

        getDbSnapshot(); // read state of Workflow and WorkflowSteps

        quickReportWorkflow();

        if (desiredStates.length == 0 || desiredStates[0].equals("ALL")) {
            String[] ds = { READY, ON_DECK, RUNNING, DONE, FAILED };
            desiredStates = ds;
        }

        String undoStr = undo_step_id != null ? " (Undo Mode) " : "";

        for (String desiredState : desiredStates) {
            System.out.println("=============== " + undoStr + desiredState
                    + " steps " + "================");
            for (T step : workflowGraph.getSortedSteps()) {
                if (step.getOperativeState().equals(desiredState)) {
                    System.out.println(step.toString());
                    /*
                     * FIX System.out.println(stepsConfig.toString(stepName));
                     */
                    System.out.println("-----------------------------------------");
                }
            }
        }
    }

    // brute force reset of workflow. for test workflows only.
    // cleans out Workflow and WorkflowStep tables and the home dir, except
    // config/
    void reset() throws SQLException, FileNotFoundException, IOException {
        getDbState();

	if (!test_mode)
	  error("Cannot reset a workflow unless it was run in test mode (-t)");

        for (String dirName : homeDirSubDirs) {
            File dir = new File(getHomeDir() + "/" + dirName);
	    if (dir.exists()) {
	      IoUtil.deleteDirectoryTree(Paths.get(dir.getAbsolutePath()));
	      System.out.println("rm -rf " + dir);
	    }
        }

        String sql = "update " + workflowTable
	    + " set undo_step_id = null where workflow_id = " + workflow_id;
        executeSqlUpdate(sql);

        sql = "delete from " + workflowStepParamValTable
	    + " where workflow_step_id in (select workflow_step_id from "
	    + workflowStepTable + " where workflow_id = " + workflow_id
	    + ")";
        executeSqlUpdate(sql);
        System.out.println(sql);

        sql = "delete from " + workflowStepTable + " where workflow_id = "
                + workflow_id;
        executeSqlUpdate(sql);
        System.out.println(sql);

        sql = "delete from " + workflowTable + " where workflow_id = "
                + workflow_id;
        executeSqlUpdate(sql);
        System.out.println(sql);
    }

    // brute force reset of workflow. for test workflows only.
    // cleans out Workflow and WorkflowStep tables and the home dir, except
    // config/
    void resetMachine() throws SQLException, FileNotFoundException, IOException {
        getDbState();

        String hostname = java.net.InetAddress.getLocalHost().getHostName();

        if (host_machine.equals(hostname)) {
            error("The workflow last ran on your current machine.  You can only reset a different machine.");
        }
        log("Reseting host_machine in database");

        String sql = "update " + workflowTable
                + " set host_machine = null where workflow_id = " + workflow_id;
        executeSqlUpdate(sql);
        log(sql);
        log("Please double check that NO workflow processes are running on "
                + host_machine + " before running on " + hostname + ".");
    }

    // //////////////////////////////////////////////////////////////////////
    // Static methods
    // //////////////////////////////////////////////////////////////////////

    public static void main(String[] args) throws Exception {
        String cmdName = System.getProperty("cmdName");

        // parse command line
        Options options = declareOptions();
        String cmdlineSyntax = cmdName
                + " -h workflow_home_dir <-r | -t | -m | -q | -c | -s <states>| -d <states>> <-u step_name> <-db [login[/pass]@]instance>";
        String cmdDescrip = "Test or really run a workflow (regular or undo), or, print a report about a workflow.";
        CommandLine cmdLine = CliUtil.parseOptions(cmdlineSyntax, cmdDescrip,
                getUsageNotes(), options, args);

        String homeDirName = cmdLine.getOptionValue("h");

        boolean oops = false;

        // use alternative database
        if (cmdLine.hasOption("db")) 
            Utilities.setDatabase(cmdLine.getOptionValue("db"));

        // runnable workflow, either test or run mode
        if (cmdLine.hasOption("r") || cmdLine.hasOption("t") || (cmdLine.hasOption("u") && cmdLine.hasOption("c"))) {
            System.err.println("Initializing...");
            RunnableWorkflow runnableWorkflow = new RunnableWorkflow(
                    homeDirName);

            // get references to the Class types we'll be using
            Class<RunnableWorkflowStep> stepClass = RunnableWorkflowStep.class;
            @SuppressWarnings("unchecked")
            Class<WorkflowGraph<RunnableWorkflowStep>> containerClass = Utilities.getXmlContainerClass(
                    RunnableWorkflowStep.class, WorkflowGraph.class);

            WorkflowGraph<RunnableWorkflowStep> rootGraph = WorkflowGraphUtil.constructFullGraph(
                    stepClass, containerClass, runnableWorkflow);
            runnableWorkflow.setWorkflowGraph(rootGraph);
            runnableWorkflow.undoStepName = cmdLine.hasOption("u") ? cmdLine.getOptionValue("u")
                    : null;
            boolean testOnly = cmdLine.hasOption("t");
	    if (cmdLine.hasOption("c")) {
		runnableWorkflow.getDbSnapshot(); // read state of Workflow and WorkflowSteps
  		rootGraph.convertToUndo();
		System.out.println("Steps in the Undo Graph:");
		System.out.println(rootGraph.getStepsAsString());
	    } else {
		runnableWorkflow.run(testOnly);
	    }
        }

        // quick workflow report
        else if (cmdLine.hasOption("q")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName);
            workflow.quickReportWorkflow();
        }

        // change machine
        else if (cmdLine.hasOption("m")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName);
            workflow.resetMachine();
        }

        // quick step report (three column output)
        else if (cmdLine.hasOption("s")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName);
            String[] desiredStates = getDesiredStates(cmdLine, "s");
            oops = desiredStates.length < 1;
            if (!oops) workflow.quickReportSteps(desiredStates, false);
        }

        // quick step report (one column output)
        else if (cmdLine.hasOption("s1")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName);
            String[] desiredStates = getDesiredStates(cmdLine, "s1");
            oops = desiredStates.length < 1;
            if (!oops) workflow.quickReportSteps(desiredStates, true);
        }

        // compile check or detailed step report
        else if (cmdLine.hasOption("c") || cmdLine.hasOption("d")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName);

            // get references to the Class types we'll be using
            Class<WorkflowStep> stepClass = WorkflowStep.class;
            @SuppressWarnings("unchecked")
            Class<WorkflowGraph<WorkflowStep>> containerClass = Utilities.getXmlContainerClass(
                    WorkflowStep.class, WorkflowGraph.class);

            WorkflowGraph<WorkflowStep> rootGraph = WorkflowGraphUtil.constructFullGraph(
                    stepClass, containerClass, workflow);
            workflow.setWorkflowGraph(rootGraph);
            if (cmdLine.hasOption("d")) {
                String[] desiredStates = getDesiredStates(cmdLine, "d");
                oops = desiredStates.length < 1;
                if (!oops) workflow.reportSteps(desiredStates);
            }
        }

        else if (cmdLine.hasOption("reset")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName);
            workflow.reset();
        }

        else {
            oops = true;
        }
        if (oops) {
            CliUtil.usage(cmdlineSyntax, cmdDescrip, getUsageNotes(), options);
            System.exit(1);

        } else {
            System.exit(0);
        }
    }

    private static String[] getDesiredStates(CommandLine cmdLine,
            String optionName) {
        boolean oops = false;
        String desiredStatesStr = cmdLine.getOptionValue(optionName);
        String[] desiredStates = desiredStatesStr.split(",");
        String[] allowedStates = { READY, ON_DECK, RUNNING, DONE, FAILED, ALL };
        Arrays.sort(allowedStates);
        for (String state : desiredStates) {
            if (Arrays.binarySearch(allowedStates, state) < 0) oops = true;
        }
        String[] none = {};
        return oops ? none : desiredStates;
    }

    private static String getUsageNotes() {
        return

        nl
                + "Home dir must contain the following:"
                + nl
                + "   config/"
                + nl
                + "     initOfflineSteps   (steps to take offline at startup)"
                + nl
                + "     loadThrottle.prop   (configure load throttling)"
                + nl
                + "     failThrottle.prop   (configure fail throttling)"
                + nl
                + "     rootParams.prop    (root parameter values)"
                + nl
                + "     stepsShared.prop   (steps shared config)"
                + nl
                + "     steps.prop         (steps config)"
                + nl
                + "     workflow.prop      (meta config)"
                + nl
                + nl
                + nl
                + "Allowed states:  READY, ON_DECK, RUNNING, DONE, FAILED, ALL"
                + nl
                + nl
                + "Examples:"
                + nl
                + nl
                + "  run a workflow:"
                + nl
                + "    % workflow -h workflow_dir -r"
                + nl
                + nl
                + "  test a workflow:"
                + nl
                + "    % workflow -h workflow_dir -t"
                + nl
                + nl
                + "  report steps that would be undone for a given step:"
                + nl
                + "    % workflow -h workflow_dir -c -u step_name"
                + nl
                + nl
                + "  undo a step:"
                + nl
                + "    % workflow -h workflow_dir -r -u step_name"
                + nl
                + nl
                + "  undo a step in a test workflow:"
                + nl
                + "    % workflow -h workflow_dir -t -u step_name"
                + nl
                + nl
                + "  check the graph for compile errors"
                + nl
                + "    % workflow -h workflow_dir -c"
                + nl
                + nl
                + "  quick report of workflow state (no steps)"
                + nl
                + "    % workflow -h workflow_dir -q"
                + nl
                + nl
                + "  print steps report (three column output)."
                + nl
                + "    % workflow -h workflow_dir -s FAILED ON_DECK"
                + nl
                + nl
                + "  print steps report (one column output)."
                + nl
                + "    % workflow -h workflow_dir -s1 FAILED ON_DECK"
                + nl
                + nl
                + "  print detailed steps report."
                + nl
                + "    % workflow -h workflow_dir -d"
                + nl
                + nl
                + "  limit steps report to steps in particular states"
                + nl
                + "    % workflow -h workflow_dir -d FAILED RUNNING"
                + nl
                + nl
                + "  print steps report, using the optional offline flag to only include steps"
                + nl
                + "  that have the flag in the indicated state.  [not implemented yet]"
                + nl + "    % workflow -h workflow_dir -d0 ON_DECK" + nl
                + "    % workflow -h workflow_dir -d1 READY ON_DECK" + nl;
    }

    private static Options declareOptions() {
        Options options = new Options();

        CliUtil.addOption(options, "h", "Workflow homedir (see below)", true);

        OptionGroup actions = new OptionGroup();
        Option run = new Option("r", "Run a workflow for real");
        actions.addOption(run);

        Option test = new Option("t", "Test a workflow");
        actions.addOption(test);

        Option compile = new Option("c", "Compile check a workflow graph.  If combined with -u, report the steps that would be undone (without doing the undo!)");
        actions.addOption(compile);

        Option detailedRep = new Option("d", true,
                "Print detailed steps report");
        actions.addOption(detailedRep);

        Option quickRep = new Option("s", true,
                "Print quick steps report (three column output)");
        actions.addOption(quickRep);

        Option quickRep1 = new Option("s1", true,
                "Print quick steps report (one column output)");
        actions.addOption(quickRep1);

        Option quickWorkflowRep = new Option("q",
                "Print quick workflow report (no steps)");
        actions.addOption(quickWorkflowRep);

        Option resetMachine = new Option(
                "m",
                "Reset the workflow's host machine.  Only use this if there are no workflow processes running on any machine.");
        actions.addOption(resetMachine);

        Option reset = new Option(
                "reset",
                "Reset workflow. DANGER! Will destroy your workflow.  Use only if you know exactly what you are doing.");
        actions.addOption(reset);

        options.addOptionGroup(actions);

        CliUtil.addOption(options, "u", "Undo the specified step", false);

        CliUtil.addOption(options, "db", "Use alternative database "
                + "(and login, password, optional). "
                + "Example: user/pass@instance", false, true);

        return options;
    }
}
