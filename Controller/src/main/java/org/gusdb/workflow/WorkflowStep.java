package org.gusdb.workflow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gusdb.fgputil.EncryptionUtil;
import org.gusdb.fgputil.xml.Name;
import org.gusdb.fgputil.xml.NamedValue;
import org.gusdb.fgputil.JavaScript;
import javax.script.ScriptException;
import org.gusdb.workflow.xml.WorkflowNode;

/*

 the following "state diagram" shows allowed state transitions by
 different parts of the system

 controller
 READY   --> ON_DECK
 RUNNING --> FAILED (if wrapper itself dies, ie, controller can't find PID)
 (state_handled --> true)

 step invoker
 ON_DECK --> RUNNING
 RUNNING --> DONE | FAILED
 (state_handled --> false)

 Pilot UI (GUI or command line)
 RUNNING    --> FAILED  (or, just kill the process and let the controller change the state)
 FAILED     --> READY  (ie, the pilot has fixed the problem)
 (state_handled --> false)
 [note: going from done to ready is the province of undo]

 Pilot UI (GUI or command line)
 OFFLINE --> 1/0  (change not allowed if step is running)

 */

public class WorkflowStep implements Comparable<WorkflowStep>, WorkflowNode {

    public static final Character PATH_DIVIDER = '.';

    // static
    private static final String nl = System.getProperty("line.separator");
    static final String defaultLoadType = "total";
    private static final JavaScript javaScriptInterpreter = new JavaScript();

    // from construction and configuration
    // file referenced as subgraph in this step
    protected String subgraphXmlFileName;
    protected String sourceXmlFileName; // file containing this step
    protected WorkflowGraph<? extends WorkflowStep> workflowGraph;
    protected String invokerClassName;
    private String baseName;
    private String path = "";
    private List<WorkflowStep> parents = new ArrayList<WorkflowStep>();
    private List<WorkflowStep> children = new ArrayList<WorkflowStep>();
    private boolean isSubgraphCall;
    private boolean isSubgraphReturn;
    // set iff this step is call or return of a global subgraph.
    private boolean isGlobal = false;
    private WorkflowStep callingStep; // step that called our subgraph, if any
    // if this step is a caller, its associated return step.
    public WorkflowStep subgraphReturnStep;
    private String paramsDigest;
    private int depthFirstOrder;
    private Set<String> loadTypes;
    private String includeIf_string;
    private String excludeIf_string;
    private String excludeIfNoXml_string;
    private Boolean excludeFromGraph = null;
    private String undoRoot;
    private String skipIfFileName;

    // state from db
    protected Integer workflow_step_id = null;
    protected String state;
    protected boolean state_handled;
    protected String undo_state;
    protected boolean undo_state_handled;
    protected boolean skipped;
    protected boolean off_line;
    protected boolean stop_after;
    protected String process_id;
    protected Date start_time;
    protected Date end_time;

    // other
    private String stepDir;
    private List<Name> dependsNames = new ArrayList<Name>();
    private List<Name> dependsGlobalNames = new ArrayList<Name>();
    private List<Name> dependsExternalNames = new ArrayList<Name>();
    private String externalName; // only used so steps can declare a
                                 // dependsExternal to this step
    private String dependsString;
    protected Map<String, String> paramValues = new LinkedHashMap<String, String>();
    protected String prevState;
    protected boolean prevOffline;
    protected boolean prevStopAfter;

    public WorkflowStep() {
        loadTypes = new LinkedHashSet<String>();
        loadTypes.add(defaultLoadType);
    }

    @Override
    public void setName(String name) {
        this.baseName = name;
    }

    public void setExternalName(String externalName) {
        this.externalName = externalName;
    }

    public String getExternalName() {
        return externalName;
    }

    public void setStepClass(String invokerClassName) {
        this.invokerClassName = invokerClassName;
    }

    public void setStepLoadTypes(String loadTypes) {
        String[] tmp = loadTypes.split(",\\s*");
        this.loadTypes = new LinkedHashSet<String>();
        this.loadTypes.add(defaultLoadType);
        this.loadTypes.addAll(Arrays.asList(tmp));
    }

    public void addLoadTypes(String[] loadTypes) {
        // check if a tag should be applied to this step. if so, remove
        // the step name from the path, and add the remaining tag to the
        // step.
        String name = baseName + PATH_DIVIDER;
        for (String loadType : loadTypes) {
            // skip the default type
            if (loadType.equals(WorkflowStep.defaultLoadType)) continue;

            String[] parts = loadType.split("\\" + WorkflowGraph.FLAG_DIVIDER,
                    2);
            if (getIsSubgraphCall()) { // a sub-graph node;
                if (parts[0].equals(getBaseName())) {
                    throw new RuntimeException("The path points to "
                            + "sub-graph [" + getFullName() + "], "
                            + "but no step specified: " + loadType);
                } else if (loadType.startsWith(name)) {
                    // remove the name from path, and attach
                    // the rest to the step.
                    String type = loadType.substring(name.length());
                    addLoadType(type);
                }
            } else { // a normal step,
                // the path has to match the exact name
                if (parts[0].equals(getBaseName())) {
                    addLoadType(parts[1]);
                } else if (loadType.startsWith(name)) {
                    // a normal step cannot have children, the path is bad
                    throw new RuntimeException("The step [" + getFullName()
                            + "] is not a sub-graph,"
                            + " the path in load type is wrong: '" + loadType
                            + "'");
                }
            }
        }
    }

    public void addLoadType(String loadType) {
        loadTypes.add(loadType.trim());
    }

    /**
     * the load types are used to flag the type of a step; sometimes there are
     * constraints that allows only a certain number of steps of a given type to
     * be run at the same time.
     * 
     * @return
     */
    public String[] getLoadTypes() {
        return loadTypes.toArray(new String[0]);
    }

    public void setUndoRoot(String undoRoot) {
        this.undoRoot = undoRoot;
    }

    public String getUndoRoot() {
        return undoRoot;
    }

    public void setIsGlobal(boolean isGlobal) {
        this.isGlobal = isGlobal;
    }

    protected boolean getIsGlobal() {
        return isGlobal;
    }

    public boolean getIsSubgraphCall() {
        return isSubgraphCall;
    }

    void setCallingStep(WorkflowStep callingStep) {
        this.callingStep = callingStep;
    }

    public boolean getIsSubgraphReturn() {
        return isSubgraphReturn;
    }

    public WorkflowStep getSubgraphReturnStep() {
        return subgraphReturnStep;
    }

    boolean getStopAfter() {
        return stop_after;
    }

    String getStepClassName() {
        return invokerClassName;
    }

    public void setWorkflowGraph(
            WorkflowGraph<? extends WorkflowStep> workflowGraph) {
        this.workflowGraph = workflowGraph;
    }

    void checkLoadTypes() throws FileNotFoundException, IOException {
        if (isSubgraphCall) return;
        for (String loadType : loadTypes) {
            Integer val = workflowGraph.getWorkflow().getLoadBalancingConfig(
                    loadType);
            if (val == null) {
                if (loadType.equals(defaultLoadType)) error("Config file loadBalancing.prop must have a line with "
                        + defaultLoadType
                        + "=xxxxx where xxxxx is your choice for the total number of steps that can run at one time.  A reasonable default would be 10.");

                else error("Unknown stepLoadType: " + loadType);
            }
        }
    }

    public void setIncludeIf(String includeIf_str) {
        includeIf_string = includeIf_str;
    }

    public void setExcludeIf(String excludeIf_str) {
        excludeIf_string = excludeIf_str;
    }

    public void setExcludeIfXmlFileDoesNotExist(String excludeIfNoXml_str) {
        excludeIfNoXml_string = excludeIfNoXml_str;
    }

    String getExcludeIfNoXmlString() {
        return excludeIfNoXml_string;
    }

    // called by xml parser
    public void setSkipIfFile(String skipIfFileName) {
	if (this.skipIfFileName != null) {
	    error("It has a forceDoneFileName attribute but is also getting that value from its calling graph.  Only one is allowed. ");
	}
	this.skipIfFileName = skipIfFileName;
    }

    String getSkipIfFileName() {
	return skipIfFileName;
    }

    // parse string versions of includeIf and excludeIf, and return final combined value
    // this will be done only once inside getExcludeFromGraph, which saves the final state
    // (the xml schema prevents having both includeIf and excludeIf)
    private boolean evalIncludeIfExcludeIf() throws Exception {
	boolean exclude = false;
	String s = null;
	try {
	    if (includeIf_string != null) {
		s = includeIf_string;
		exclude = !javaScriptInterpreter.evaluateBooleanExpression(includeIf_string);
	    } else if (excludeIf_string != null) {
		s = excludeIf_string;
		exclude = javaScriptInterpreter.evaluateBooleanExpression(excludeIf_string);
	    }
	} catch (ScriptException e) {
	    error("The following includeIf or excludeIf expression is not formatted legally: '" + s + "'");
	}
	return exclude;
    }

    public boolean getExcludeFromGraph() throws FileNotFoundException, Exception,
            IOException {
        if (excludeFromGraph == null) {
            boolean efg = false;
            if (callingStep != null && callingStep.getExcludeFromGraph()) {
                efg = true;
            } else if (evalIncludeIfExcludeIf()) {
                efg = true;
                String gr = isSubgraphCall ? "SUBGRAPH " : "";
                if (!isSubgraphReturn)
                    workflowGraph.getWorkflow().log(
                            "Excluding " + gr + getFullName());
            } else if (isSubgraphCall && excludeIfNoXml_string != null
                    && excludeIfNoXml_string.equals("true")) {
                String gusHome = System.getProperty("GUS_HOME");
                File xmlFile = new File(gusHome + "/lib/xml/workflow/"
                        + subgraphXmlFileName);
                if (!xmlFile.exists()) {
                    efg = true;
                    workflowGraph.getWorkflow().log(
                            "Excluding SUBGRAPH "
                                    + getFullName()
                                    + ": optional xml file "
                                    + subgraphXmlFileName
                                    + " is absent from $GUS_HOME/lib/xml/workflow");
                }
            }
            excludeFromGraph = new Boolean(efg);
        }
        return excludeFromGraph;
    }

    void addParent(WorkflowStep parent) {
        if (!parents.contains(parent)) parents.add(parent);
    }

    void removeParent(WorkflowStep parent) {
        parents.remove(parent);
    }

    public List<WorkflowStep> getParents() {
        return parents;
    }

    void addChild(WorkflowStep child) {
        if (!children.contains(child)) children.add(child);
    }

    void removeChild(WorkflowStep child) {
        children.remove(child);
    }

    void removeAllChildren() {
        children = new ArrayList<WorkflowStep>();
    }

    public List<WorkflowStep> getChildren() {
        return children;
    }

    // all kids, recursively
    Set<WorkflowStep> getDescendants() {
        Set<WorkflowStep> descendants = new HashSet<WorkflowStep>(children);
        for (WorkflowStep kid : children) {
            descendants.addAll(kid.getDescendants());
        }
        return descendants;
    }

    public Map<String, String> getParamValues() {
        return paramValues;
    }

    // use poppedSteps to detect cycles. we have a cycle if we see this step
    // again before it has completed processing its kids
    void addToList(List<WorkflowStep> list, Set<WorkflowStep> poppedSteps) {

        // check if we have already seen this step
        if (list.contains(this)) {
            if (poppedSteps.contains(this)) return; // ok if done processing
                                                    // kids
            else error("It is reached by an illegal cycle in the graph. Please check if it is referenced by a dependsExternal that might be causing the cycle ");
        }
        list.add(this);
        for (WorkflowStep child : getChildren()) {
            child.addToList(list, poppedSteps);
        }
        poppedSteps.add(this);
    }

    // insert a child between this step and its previous children
    protected WorkflowStep insertSubgraphReturnChild() {
        WorkflowStep newStep = newStep();
        newStep.setXmlFile(subgraphXmlFileName); // remember this in case of
                                                 // undo
        newStep.isSubgraphCall = false;
        newStep.isSubgraphReturn = true;
        newStep.setWorkflowGraph(workflowGraph);
        newStep.setIncludeIf(includeIf_string);
        newStep.setExcludeIf(excludeIf_string);
        newStep.setIsGlobal(isGlobal);

        newStep.setName(getFullName() + ".return");
        subgraphReturnStep = newStep;
        List<WorkflowStep> oldChildren = new ArrayList<WorkflowStep>(children);
        for (WorkflowStep oldChild : oldChildren) {
            oldChild.removeParent(this);
            removeChild(oldChild);
            newStep.addChild(oldChild);
            oldChild.addParent(newStep);
        }
        newStep.addParent(this);
        addChild(newStep);
        return newStep;
    }

    @Override
    public void addParamValue(NamedValue paramValue) {
        paramValues.put(paramValue.getName(), paramValue.getValue());
    }

    @Override
    public String getBaseName() {
        return baseName;
    }

    String getFullName() {
        return getPath() + getBaseName();
    }

    void setPath(String path) {
        this.path = path;
    }

    String getPath() {
        return path;
    }

    public int getId() {
        return workflow_step_id;
    }

    String getState() {
        return state;
    }

    String getUndoState() {
        return undo_state;
    }

    Boolean getSkipped() {
        return skipped;
    }

    String getOperativeState() {
        return getUndoing() ? undo_state : state;
    }

    boolean getOperativeStateHandled() {
        return getUndoing() ? undo_state_handled : state_handled;
    }

    boolean getUndoing() {
        return workflowGraph.getWorkflow().getUndoStepId() != null;
    }

    @Override
    public List<Name> getDependsNames() {
        return dependsNames;
    }

    String getDependsString() {
        if (dependsString == null) {
            List<String> d = new ArrayList<String>();
            for (WorkflowStep parent : parents)
                d.add(parent.getBaseName());
            Collections.sort(d);
            dependsString = d.toString();
        }
        return dependsString;
    }

    @Override
    public void addDependsName(Name dependsName) {
        dependsNames.add(dependsName);
    }

    List<Name> getDependsGlobalNames() {
        return dependsGlobalNames;
    }

    @Override
    public void addDependsGlobalName(Name dependsName) {
        dependsGlobalNames.add(dependsName);
    }

    List<Name> getDependsExternalNames() {
        return dependsExternalNames;
    }

    @Override
    public void addDependsExternalName(Name dependsName) {
        dependsExternalNames.add(dependsName);
    }

    @Override
    public void setXmlFile(String subgraphXmlFileName) {
        this.subgraphXmlFileName = subgraphXmlFileName;
        isSubgraphCall = true;
    }

    @Override
    public String getSubgraphXmlFileName() {
        return subgraphXmlFileName;
    }

    @Override
    public void setSourceXmlFileName(String fileName) {
        this.sourceXmlFileName = fileName;
    }

    @Override
    public String getSourceXmlFileName() {
        return workflowGraph.getXmlFileName();
    }

    String getParamsDigest() throws NoSuchAlgorithmException, Exception {
        if (paramsDigest == null)
            paramsDigest = EncryptionUtil.encrypt(paramValues.toString());
        return paramsDigest;
    }

    int getDepthFirstOrder() {
        return depthFirstOrder;
    }

    void setDepthFirstOrder(int o) {
        depthFirstOrder = o;
    }

    static PreparedStatement getPreparedInsertStmt(Connection dbConnection,
            int workflowId, String workflowStepTable) throws SQLException {
        String sql = "INSERT INTO "
                + workflowStepTable
                + " (workflow_step_id, workflow_id, name, state, state_handled, undo_state, undo_state_handled, off_line, stop_after, depends_string, step_class, params_digest, depth_first_order)"
                + " VALUES (" + workflowStepTable + "_sq.nextval, "
                + workflowId + ", ?, ?, 1, null, 1, 0, 0, ?, ?, ?, ?)";
        return dbConnection.prepareStatement(sql);
    }

    static PreparedStatement getPreparedUpdateStmt(Connection dbConnection,
            int workflowId, String workflowStepTable) throws SQLException {
        String sql = "UPDATE " + workflowStepTable
                + " SET depends_string = ?, depth_first_order = ?"
                + " WHERE name = ?" + " AND workflow_id = " + workflowId;
        return dbConnection.prepareStatement(sql);
    }

    static PreparedStatement getPreparedUndoUpdateStmt(Connection dbConnection,
            int workflowId, String workflowStepTable) throws SQLException {
        String sql = "UPDATE " + workflowStepTable + " SET undo_state = '"
                + Workflow.READY + "'" + " WHERE name = ?"
                + " AND undo_state is NULL" + " AND workflow_id = "
                + workflowId;
        return dbConnection.prepareStatement(sql);
    }

    static PreparedStatement getPreparedParamValInsertStmt(Connection dbConnection,
							String workflowStepParamValTable) throws SQLException {
        String sql = "INSERT INTO "
	    + workflowStepParamValTable
	    + " (workflow_step_param_value_id, workflow_step_id, param_name, param_value)"
	    + " VALUES (" + workflowStepParamValTable + "_sq.nextval, ?, ?, ?)";
        return dbConnection.prepareStatement(sql);
    }

    static PreparedStatement getPreparedParamValStmt(Connection dbConnection,
						     String workflowStepParamValTable) throws SQLException {
        String sql = "select param_name,param_value from "
	    + workflowStepParamValTable
	    + " where workflow_step_id = ?";
        return dbConnection.prepareStatement(sql);
    }

    // write this step to the db, if not already there.
    // called during workflow initialization
    void initializeStepTable(Set<String> stepNamesInDb,
			     PreparedStatement insertStepTableStmt, PreparedStatement updateStepTableStmt, PreparedStatement insertStepTableParamValStmt)
            throws SQLException, NoSuchAlgorithmException, Exception {
        if (stepNamesInDb.contains(getFullName())) {
            updateStepTableStmt.setString(1, getDependsString());
            updateStepTableStmt.setInt(2, getDepthFirstOrder());
            updateStepTableStmt.setString(3, getFullName());
            updateStepTableStmt.execute();
        } else {
            insertStepTableStmt.setString(1, getFullName());
            insertStepTableStmt.setString(2, Workflow.READY);
            insertStepTableStmt.setString(3, getDependsString());
            insertStepTableStmt.setString(4, invokerClassName);
            insertStepTableStmt.setString(5, getParamsDigest());
            insertStepTableStmt.setInt(6, getDepthFirstOrder());
            insertStepTableStmt.execute();
        }
    }

    void initializeStepParamValTable(Set<String> stepNamesInDb, PreparedStatement insertStmt)
	throws SQLException {

	if (stepNamesInDb.contains(getFullName())) return;
	
        for (String paramName : paramValues.keySet()) {
            String paramValue = paramValues.get(paramName);
            insertStmt.setInt(1, getId());
            insertStmt.setString(2, paramName);
            insertStmt.setString(3, paramValue);
            insertStmt.execute();
        }
    }

    Map<String, String> getDbParamValues(PreparedStatement stmt, Integer dbId) throws SQLException {
	Map<String, String> dbParamValues = new LinkedHashMap<String, String>();
	
        ResultSet rs = null;
        try {
	    stmt.setInt(1, dbId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String paramName = rs.getString(1);
                String paramValue = rs.getString(2);
		if (paramValue == null) paramValue = "";
                dbParamValues.put(paramName, paramValue);
            }
        }
        finally {
            if (rs != null) rs.close();
        }
	return dbParamValues;
    }

    // update provided data structures with info about changed params
    // dbParamValuesDiff:  params that are in the db but are absent or different in memory
    // newParamValuesDiff: params that in memory but are absent or different in memory
    // return true if the changes are illegal
    boolean checkChangedParams(PreparedStatement paramValuesStatement,
			       Integer dbId,
			       String dbParamsDigest,
			       String dbState,
			       Map<String, String> dbParamValuesDiff,
			       Map<String, String> newParamValuesDiff) throws SQLException, Exception {

	if (dbParamsDigest.equals(getParamsDigest())) return false;

	boolean illegalChange = false;
	boolean runningOrFailed = dbState.equals(Workflow.RUNNING) || dbState.equals(Workflow.FAILED);
	boolean done = dbState.equals(Workflow.DONE);

	Map<String, String> dbParamValues = getDbParamValues(paramValuesStatement, dbId);

	// find new params that are different
        for (String paramName : paramValues.keySet()) {
	    String newValue = paramValues.get(paramName);

	    // new in memory - illegal if failed or running
	    if (!dbParamValues.containsKey(paramName)) {
		newParamValuesDiff.put(paramName, newValue);
		if (runningOrFailed) illegalChange = true;
	    } 

	    // different in memory and db - illegal if failed, running or done
	    else if (!newValue.equals(dbParamValues.get(paramName))) {
		newParamValuesDiff.put(paramName, newValue);
		if (runningOrFailed || done) illegalChange = true;
	    }
	}

	// find db params that are different
        for (String dbParamName : dbParamValues.keySet()) {
	    String dbValue = dbParamValues.get(dbParamName);

	    // deleted in memory or different - illegal if failed, running or done
	    if (!paramValues.containsKey(dbParamName) || !dbValue.equals(paramValues.get(dbParamName))) {
		dbParamValuesDiff.put(dbParamName, dbValue);
		if (runningOrFailed || done) illegalChange = true;
	    } 
	}

	return illegalChange;
    }

    // static method
    static String getBulkSnapshotSql(int workflow_id, String workflowStepTable) {
        return "SELECT name, workflow_step_id, state, state_handled, skipped, undo_state, undo_state_handled, off_line, stop_after, process_id, start_time, end_time, host_machine"
                + " FROM "
                + workflowStepTable
                + " WHERE workflow_id = "
                + workflow_id;
    }

    void setFromDbSnapshot(ResultSet rs) throws SQLException {
        prevState = getOperativeState();
        prevOffline = off_line;
        prevStopAfter = stop_after;

        workflow_step_id = rs.getInt("WORKFLOW_STEP_ID");
        state = rs.getString("STATE");
        state_handled = rs.getBoolean("STATE_HANDLED");
        skipped = rs.getBoolean("SKIPPED");
        undo_state = rs.getString("UNDO_STATE");
        undo_state_handled = rs.getBoolean("UNDO_STATE_HANDLED");
        off_line = rs.getBoolean("OFF_LINE");
        stop_after = rs.getBoolean("STOP_AFTER");
        process_id = rs.getString("PROCESS_ID");
        start_time = rs.getDate("START_TIME");
        end_time = rs.getDate("END_TIME");
    }

    // interpolate variables into subgraphXmlFileName, param values, includeIf
    // and excludeIf
    // check is set to true only for final subsitution.
    void substituteValues(Map<String, String> variables, boolean check) {

	String where = workflowGraph.getXmlFileName() + ", step " + getFullName() + " ";

        if (subgraphXmlFileName != null) {
            subgraphXmlFileName =
		Utilities.substituteVariablesIntoString(subgraphXmlFileName, variables,
							where, check, "subgraphXmlFileName", null);
        }

        if (externalName != null) {
            externalName =
		Utilities.substituteVariablesIntoString(externalName, variables,
							where, check, "externalName", null);
        }

        if (skipIfFileName != null) {
	    skipIfFileName = Utilities.substituteVariablesIntoString(skipIfFileName, variables,
								     where, check, "skipIfFile", null);
        }

        for (String paramName : paramValues.keySet()) {
            String paramValue = paramValues.get(paramName);
            String newParamValue =
		Utilities.substituteVariablesIntoString(paramValue, variables,
							where, check, "paramValue", paramName);
            paramValues.put(paramName, newParamValue);
        }
        if (includeIf_string != null)
            includeIf_string = processIfString("includeIf", includeIf_string,
                    variables, check);

        if (excludeIf_string != null)
            excludeIf_string = processIfString("excludeIf", excludeIf_string,
                    variables, check);

    }

    void substituteMacros(Map<String, String> globalProps) {

      includeIf_string = substituteMacrosIntoIfString("includeIf", includeIf_string, globalProps);
      excludeIf_string = substituteMacrosIntoIfString("excludeIf", excludeIf_string, globalProps);

      for (String paramName : paramValues.keySet()) {
	String paramValue = paramValues.get(paramName);
	String newParamValue = Utilities.substituteMacrosIntoString(
								    paramValue, globalProps);
	paramValues.put(paramName, newParamValue);
	if (newParamValue.indexOf("@@") != -1)
	  error("Parameter '"
		+ paramName 
		+ "' includes an unresolvable macro reference: '"
		+ newParamValue + "'.  Likely solution: add the macro to the config/stepsShared.prop file");
      }
    }

    private String substituteMacrosIntoIfString(String type, String ifString, Map<String, String> globalProps) {
      if (ifString != null) {
	ifString = Utilities.substituteMacrosIntoString(ifString, globalProps);
	if (ifString.indexOf("@@") != -1)
	  error(type + " includes an unresolvable macro reference: '"
		+ ifString + "'.  Likely solution: add the macro to the config/stepsShared.prop file");
      
      }
      return ifString;
    }

    private String processIfString(String type, String ifString,
				   Map<String, String> variables, boolean check) {

	String where = workflowGraph.getXmlFileName() + ", step " + getFullName() + " ";

        String newIf =
	    Utilities.substituteVariablesIntoString(ifString, variables,
						    where, check, type, null);

        if (check) {
	    try {
		javaScriptInterpreter.evaluateBooleanExpression(newIf);
	    } catch (ScriptException e) {
		error(nl + type + " is not a valid boolean expression " + nl + e);
	    }
	}
	return newIf;
    }

    protected String getStepDir() {
        if (stepDir == null) {
            stepDir = workflowGraph.getWorkflow().getHomeDir() + "/steps/"
                    + getFullName();
            File dir = new File(stepDir);
            if (!dir.exists()) dir.mkdir();
        }
        return stepDir;
    }

    void invert(Set<String> allowedSteps) {
        List<WorkflowStep> temp = new ArrayList<WorkflowStep>(parents);
        for (WorkflowStep parent : parents) {
            if (!allowedSteps.contains(parent.getFullName())) {
                temp.remove(parent);
            }
        }
        List<WorkflowStep> temp2 = new ArrayList<WorkflowStep>(children);
        for (WorkflowStep child : children) {
            if (!allowedSteps.contains(child.getFullName())) {
                temp2.remove(child);
            }
        }
        parents = temp2;
        children = temp;
        boolean temp3 = isSubgraphCall;
        isSubgraphCall = isSubgraphReturn;
        isSubgraphReturn = temp3;
    }

    // //////////////////////// utilities
    // /////////////////////////////////////////

    WorkflowStep newStep() {
        return new WorkflowStep();
    }

    private void error(String msg) {
	Utilities.error("Step "
			+ getFullName()
			+ " in graph file " 
			+ workflowGraph.getXmlFileName() + " has an error. " + msg);
    }


    protected void executeSqlUpdate(String sql) throws SQLException,
            FileNotFoundException, IOException {
        workflowGraph.getWorkflow().executeSqlUpdate(sql);
    }

    @Override
    public String toString() {

        String s = nl + "name:       " + getFullName() + nl + "id:         "
                + workflow_step_id + nl + "stepClass:  " + invokerClassName
                + nl + "sourceXml:  " + workflowGraph.getXmlFileName() + nl
                + "subgraphXml " + subgraphXmlFileName + nl + "state:      "
                + state + nl + "undo_state: " + undo_state + nl
                + "off_line:   " + off_line + nl + "stop_after: " + stop_after
                + nl + "handled:    " + state_handled + nl + "process_id: "
                + process_id + nl + "start_time: " + start_time + nl
                + "end_time:   " + end_time + nl + "depth:      "
                + depthFirstOrder + nl + "depends on: ";

        String delim = "";
        StringBuffer buf = new StringBuffer(s);
        for (WorkflowStep parent : getParents()) {
            buf.append(delim + parent.getFullName());
            delim = ", ";
        }
        buf.append(nl + "params: " + paramValues);
        buf.append(nl + nl);
        return buf.toString();
    }

    @Override
    public int compareTo(WorkflowStep s) {
        return baseName.compareTo(s.getBaseName());
    }

    public void setGroupName(Name groupName) {
        // value is not needed in this implementation;
        // added for interface compliance only
        // (groups are used for the ReFlow Graph Viewer)
    }

    @Override
    public String getGroupName() {
        throw new UnsupportedOperationException(
                "This implementation does not support this method.");
    }

    @Override
    public void setGroupName(String arg0) {
        // value is not needed in this implementation;
        // added for interface compliance only
    }
}
