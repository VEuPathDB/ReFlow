package org.gusdb.workflow;

import static org.gusdb.fgputil.FormatUtil.NL;
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
import org.gusdb.fgputil.db.platform.DBPlatform;
import org.gusdb.fgputil.db.platform.PostgreSQL;
import org.gusdb.fgputil.db.platform.SupportedPlatform;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.db.pool.SimpleDbConfig;
import org.gusdb.workflow.xml.WorkflowClassFactory;
import org.gusdb.workflow.RunnableWorkflow.RunnableWorkflowGraphClassFactory;

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

  public static class WorkflowGraphClassFactory implements WorkflowClassFactory<WorkflowStep, WorkflowGraph<WorkflowStep>> {
    @SuppressWarnings("unchecked")
    @Override
    public Class<WorkflowGraph<WorkflowStep>> getContainerClass() {
      return (Class<WorkflowGraph<WorkflowStep>>) (Class<?>) WorkflowGraph.class;
    }

    @Override
    public Class<WorkflowStep> getStepClass() {
      return WorkflowStep.class;
    }
  }

  // static
  // my parents are not done yet -- default state
  public static final String READY = "READY";
  // my parents are done, but there is no slot for me
  public static final String ON_DECK = "ON_DECK";
  public static final String FAILED = "FAILED";
  public static final String DONE = "DONE";
  public static final String RUNNING = "RUNNING";
  public static final String ALL = "ALL";

  public static final String LOAD_THROTTLE_FILE = "loadThrottle.prop";
  public static final String FAIL_THROTTLE_FILE = "failThrottle.prop";

  // configuration
  private final String homeDir;
  private final Connection connection;
  private final DBPlatform platform;
  private Properties workflowProps; // from workflow config file
  protected Properties loadThrottleConfig = new Properties();
  protected Properties failThrottleConfig = new Properties();
  private String[] homeDirSubDirs = {"logs", "steps", "data", "backups"};
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
  protected List<String> multipleUndoStepNames = null;

  // list of processes to clean
  private List<Process> bgdProcesses = new ArrayList<Process>();

  public Workflow(String homeDir, Connection connection, DBPlatform platform) throws FileNotFoundException, IOException {
    this.homeDir = homeDir.replaceAll("/$", "");
    this.connection = connection;
    this.platform = platform;
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

  void setMultipleUndoStepNames(List<String> multipleUndoStepNames){
    this.multipleUndoStepNames = multipleUndoStepNames;
  }

  List<String> getMultipleUndoStepNames() {
    return multipleUndoStepNames;
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

    Integer getId() throws SQLException {
        getDbState();
        return workflow_id;
    }

    protected void getDbSnapshot() throws SQLException {
        getDbState();
        getStepsDbState();
    }

    protected void getDbState() throws SQLException {
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
                stmt = connection.createStatement();
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
    protected void getStepsDbState() throws SQLException {
        String sql = WorkflowStep.getBulkSnapshotSql(workflow_id,
                workflowStepTable);

        // run query to get all rows from WorkflowStep for this workflow
        // stuff each row into the snapshot, keyed on step name
        Statement stmt = null;
        ResultSet rs = null;
        
        resetStepCounts();  // used for throttling
        
        try {
            stmt = connection.createStatement();
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
            stmt = connection.createStatement();
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

    static DatabaseInstance getDb() throws IOException {
      String dsn = Utilities.getGusConfig("jdbcDsn");
      String login = Utilities.getGusConfig("databaseLogin");

      SupportedPlatform platform;
      if (Utilities.getGusConfig("dbVendor").trim().equals("Postgres")){
          platform = SupportedPlatform.POSTGRESQL;
      } else {
        platform = SupportedPlatform.ORACLE;
      }

      System.err.println("Connecting to " + dsn + " (" + login + ")");
      DatabaseInstance db = new DatabaseInstance(
          SimpleDbConfig.create(platform, dsn, login,
              Utilities.getGusConfig("databasePassword")));
      System.err.println("Connected");

      return db;
    }

    Connection getDbConnection() {
      return connection;
    }

    DBPlatform getDbPlatform() {
        return platform;
    }

    void executeSqlUpdate(String sql) throws SQLException {
        Statement stmt = connection.createStatement();
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

    public String getWorkflowXmlFileName() throws IOException {
        return getWorkflowConfig("workflowXmlFile");
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
    return Integer.valueOf(config.getProperty(key));
  }

    void error(String msg) {
        Utilities.error(msg);
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
    void quickReportWorkflow() throws SQLException {

      getDbState();

        System.out.println("Workflow '" + name + " " + version + "'" + NL
                + "workflow_id:           " + workflow_id + NL
                + "state:                 " + state + NL
                + "undo_step:             " + undoStepName + NL
                + "process_id:            " + process_id + NL
                + "host_machine:          " + host_machine + NL);
    }

    // light reporting of state of workflow with steps
    void quickReportSteps(String[] desiredStates, boolean oneColumnOutput)
            throws SQLException, FileNotFoundException, IOException {
        getDbState();
        workflowStepTable = getWorkflowConfig("workflowStepTable");

        StringBuffer buf = new StringBuffer();
        for (String ds : desiredStates)
            buf.append("'" + ds + "',");


        String sql;
        String state_str = undo_step_id == null ? "state" : "undo_state";

        if (getDbPlatform().getClass().equals(PostgreSQL.class)){
            sql = "SELECT name, workflow_step_id," + state_str
                    + ", end_time, CASE WHEN start_time IS NULL THEN -1 "
                    + "  ELSE EXTRACT(EPOCH FROM (COALESCE(end_time, LOCALTIMESTAMP) - start_time))/3600 "
                    + "  END AS hours " + " FROM " + workflowStepTable
                    + " WHERE workflow_id = '" + workflow_id + "'" + " AND "
                    + state_str + " in(" + buf.substring(0, buf.length() - 1) + ")"
                    + " ORDER BY end_time ASC, start_time ASC"
            ;
        } else {
            sql = "SELECT name, workflow_step_id," + state_str
                    + ", end_time, CASE WHEN start_time IS NULL THEN -1 "
                    + "  ELSE (nvl(end_time, SYSDATE) - start_time) * 24 "
                    + "  END AS hours " + " from " + workflowStepTable
                    + " WHERE workflow_id = '" + workflow_id + "'" + " AND "
                    + state_str + " in(" + buf.substring(0, buf.length() - 1) + ")"
                    + " ORDER BY end_time ASC, start_time ASC"
            ;
        }

        Statement stmt = null;
        ResultSet rs = null;
        Formatter formatter = null;
        try {
            stmt = connection.createStatement();
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
                + " -h workflow_home_dir <-r | -t | -m | -q | -c | -s <states>| -d <states>> <-u step_name | -undoStepsFile file> <-db [login[/pass]@]instance>";
        String cmdDescrip = "Test or really run a workflow (regular or undo), or, print a report about a workflow.";
        CommandLine cmdLine = CliUtil.parseOptions(cmdlineSyntax, cmdDescrip,
                getUsageNotes(), options, args);

        String homeDirName = cmdLine.getOptionValue("h");

        boolean oops = false;

        // use alternative database
        if (cmdLine.hasOption("db")) 
            Utilities.setDatabase(cmdLine.getOptionValue("db"));

        try (DatabaseInstance db = getDb();
             Connection conn = db.getDataSource().getConnection()) {

        // runnable workflow, either test or run mode
        if (cmdLine.hasOption("r") || cmdLine.hasOption("t") || (cmdLine.hasOption("u") && cmdLine.hasOption("c"))) {
            System.err.println("Initializing...");
            RunnableWorkflow runnableWorkflow = new RunnableWorkflow(homeDirName, conn, db.getPlatform());
            WorkflowGraph<RunnableWorkflowStep> rootGraph = WorkflowGraphUtil.constructFullGraph(
                new RunnableWorkflowGraphClassFactory(), runnableWorkflow);
            runnableWorkflow.setWorkflowGraph(rootGraph);
            runnableWorkflow.undoStepName = cmdLine.hasOption("u") ? cmdLine.getOptionValue("u") : null;

            // Read undo steps from file if provided
            if (cmdLine.hasOption("undoStepsFile")) {
              if (runnableWorkflow.undoStepName != null) throw new IllegalArgumentException("-u and -undoStepsFile are not both allowed");
              runnableWorkflow.setMultipleUndoStepNames(readMultiUndoFile( cmdLine));
            }

            boolean testOnly = cmdLine.hasOption("t");
            if (cmdLine.hasOption("c")) {
                if (cmdLine.hasOption("undoStepsFile")) {
                    throw new IllegalArgumentException(
                        "The -c option (compile check) is not allowed when using -undoStepsFile. " +
                        "The -c option can only be used with the -u option to report steps that would be undone for a single step.");
                }
                runnableWorkflow.getDbSnapshot(); // read state of Workflow and WorkflowSteps
                rootGraph.convertToUndo();
                System.out.println("Steps in the Undo Graph:");
                System.out.println(rootGraph.getStepsAsString());
            }
            else {
              runnableWorkflow.run(testOnly);
            }
        }

        // quick workflow report
        else if (cmdLine.hasOption("q")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName, conn, db.getPlatform());
            workflow.quickReportWorkflow();
        }

        // change machine
        else if (cmdLine.hasOption("m")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName, conn, db.getPlatform());
            workflow.resetMachine();
        }

        // quick step report (three column output)
        else if (cmdLine.hasOption("s")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName, conn, db.getPlatform());
            String[] desiredStates = getDesiredStates(cmdLine, "s");
            oops = desiredStates.length < 1;
            if (!oops) workflow.quickReportSteps(desiredStates, false);
        }

        // quick step report (one column output)
        else if (cmdLine.hasOption("s1")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(
                    homeDirName, conn, db.getPlatform());
            String[] desiredStates = getDesiredStates(cmdLine, "s1");
            oops = desiredStates.length < 1;
            if (!oops) workflow.quickReportSteps(desiredStates, true);
        }

        // compile check or detailed step report
        else if (cmdLine.hasOption("c") || cmdLine.hasOption("d")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(homeDirName, conn, db.getPlatform());
            WorkflowGraph<WorkflowStep> rootGraph = WorkflowGraphUtil.constructFullGraph(
                    new WorkflowGraphClassFactory(), workflow);
            workflow.setWorkflowGraph(rootGraph);
            if (cmdLine.hasOption("d")) {
                String[] desiredStates = getDesiredStates(cmdLine, "d");
                oops = desiredStates.length < 1;
                if (!oops) workflow.reportSteps(desiredStates);
            }
        }

        else if (cmdLine.hasOption("reset")) {
            Workflow<WorkflowStep> workflow = new Workflow<WorkflowStep>(homeDirName, conn, db.getPlatform());
            workflow.reset();
        }

        else {
            oops = true;
        }

        }

        if (oops) {
            CliUtil.usage(cmdlineSyntax, cmdDescrip, getUsageNotes(), options);
            System.exit(1);

        } else {
            System.exit(0);
        }
    }

    private static List<String> readMultiUndoFile(CommandLine cmdLine) {
      String undoStepFileName = cmdLine.getOptionValue("undoStepsFile");
      List<String> multipleUndoStepNames = new ArrayList<>();
      try (java.io.BufferedReader reader = new java.io.BufferedReader(
          new java.io.FileReader(undoStepFileName))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String trimmedLine = line.trim();
          // Skip empty lines and comments
          if (!trimmedLine.isEmpty() && !trimmedLine.startsWith("#")) {
            multipleUndoStepNames.add(trimmedLine);
          }
        }
        if (multipleUndoStepNames.isEmpty()) {
          throw new IllegalArgumentException("Undo step file '" + undoStepFileName + "' contains no valid step names");
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to read undo step file: " + undoStepFileName, e);
      }
      return multipleUndoStepNames;
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

        NL
                + "Home dir must contain the following:"
                + NL
                + "   config/"
                + NL
                + "     initOfflineSteps   (steps to take offline at startup)"
                + NL
                + "     loadThrottle.prop   (configure load throttling)"
                + NL
                + "     failThrottle.prop   (configure fail throttling)"
                + NL
                + "     rootParams.prop    (root parameter values)"
                + NL
                + "     stepsShared.prop   (steps shared config)"
                + NL
                + "     steps.prop         (steps config)"
                + NL
                + "     workflow.prop      (meta config)"
                + NL
                + NL
                + NL
                + "Allowed states:  READY, ON_DECK, RUNNING, DONE, FAILED, ALL"
                + NL
                + NL
                + "Examples:"
                + NL
                + NL
                + "  run a workflow:"
                + NL
                + "    % workflow -h workflow_dir -r"
                + NL
                + NL
                + "  test a workflow:"
                + NL
                + "    % workflow -h workflow_dir -t"
                + NL
                + NL
                + "  report steps that would be undone for a given step:"
                + NL
                + "    % workflow -h workflow_dir -c -u step_name"
                + NL
                + NL
                + "  undo a step:"
                + NL
                + "    % workflow -h workflow_dir -r -u step_name"
                + NL
                + NL
                + "  undo a step in a test workflow:"
                + NL
                + "    % workflow -h workflow_dir -t -u step_name"
                + NL
                + NL
                + "  check the graph for compile errors"
                + NL
                + "    % workflow -h workflow_dir -c"
                + NL
                + NL
                + "  quick report of workflow state (no steps)"
                + NL
                + "    % workflow -h workflow_dir -q"
                + NL
                + NL
                + "  print steps report (three column output)."
                + NL
                + "    % workflow -h workflow_dir -s FAILED ON_DECK"
                + NL
                + NL
                + "  print steps report (one column output)."
                + NL
                + "    % workflow -h workflow_dir -s1 FAILED ON_DECK"
                + NL
                + NL
                + "  print detailed steps report."
                + NL
                + "    % workflow -h workflow_dir -d"
                + NL
                + NL
                + "  limit steps report to steps in particular states"
                + NL
                + "    % workflow -h workflow_dir -d FAILED RUNNING"
                + NL
                + NL
                + "  print steps report, using the optional offline flag to only include steps"
                + NL
                + "  that have the flag in the indicated state.  [not implemented yet]"
                + NL + "    % workflow -h workflow_dir -d0 ON_DECK" + NL
                + "    % workflow -h workflow_dir -d1 READY ON_DECK" + NL;
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

        CliUtil.addOption(options, "undoStepsFile", "Undo steps listed in file (one per line)", false);

        CliUtil.addOption(options, "db", "Use alternative database "
                + "(and login, password, optional). "
                + "Example: user/pass@instance", false, true);

        return options;
    }
}
