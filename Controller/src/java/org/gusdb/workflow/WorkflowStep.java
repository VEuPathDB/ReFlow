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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;

import org.gusdb.fgputil.EncryptionUtil;
import org.gusdb.fgputil.xml.Name;
import org.gusdb.fgputil.xml.NamedValue;
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
    private String forceDoneFileName;

    // state from db
    protected Integer workflow_step_id = null;
    protected String state;
    protected boolean state_handled;
    protected String undo_state;
    protected boolean undo_state_handled;
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
    protected Map<String, String> paramValues = new HashMap<String, String>();
    protected String prevState;
    protected boolean prevOffline;
    protected boolean prevStopAfter;

    public WorkflowStep() {
        loadTypes = new LinkedHashSet<String>();
        loadTypes.add(defaultLoadType);
    }

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
                if (loadType.equals(defaultLoadType)) Utilities.error("Config file loadBalancing.prop must have a line with "
                        + defaultLoadType
                        + "=xxxxx where xxxxx is your choice for the total number of steps that can run at one time.  A reasonable default would be 10.");

                else Utilities.error("Step " + getFullName()
                        + " has unknown stepLoadType: " + loadType);
            }
        }
    }

    public void setIncludeIf(String includeIf_str) {
        includeIf_string = includeIf_str;
    }

    String getIncludeIfString() {
        return includeIf_string;
    }

    public void setExcludeIf(String excludeIf_str) {
        excludeIf_string = excludeIf_str;
    }

    String getExcludeIfString() {
        return excludeIf_string;
    }

    public void setExcludeIfXmlFileDoesNotExist(String excludeIfNoXml_str) {
        excludeIfNoXml_string = excludeIfNoXml_str;
    }

    String getExcludeIfNoXmlString() {
        return excludeIfNoXml_string;
    }

    // called by xml parser
    public void setForceDoneFileName(String forceDoneFileName) {
	if (this.forceDoneFileName != null) {
	    Utilities.error("Step "
			    + getFullName()
			    + " in graph file "
			    + workflowGraph.getXmlFileName()
			    + " has a forceDoneFileName attribute but is also getting that value from its calling graph.  Only one is allowed. ");
	}
	this.forceDoneFileName = forceDoneFileName;
    }

    String getForceDoneFileName() {
	return forceDoneFileName;
    }

    public boolean getExcludeFromGraph() throws FileNotFoundException,
            IOException {
        if (excludeFromGraph == null) {
            boolean efg = false;
            if (callingStep != null && callingStep.getExcludeFromGraph()) {
                efg = true;
            } else if ((includeIf_string != null && includeIf_string.equals("false"))
                    || (excludeIf_string != null && excludeIf_string.equals("true"))) {
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
            else Utilities.error("Step "
                    + getFullName()
                    + " in graph file "
                    + workflowGraph.getXmlFileName()
                    + " is reached by an illegal cycle in the graph. Please check if it is referenced by a dependsExternal that might be causing the cycle ");
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

    public void addParamValue(NamedValue paramValue) {
        paramValues.put(paramValue.getName(), paramValue.getValue());
    }

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

    String getOperativeState() {
        return getUndoing() ? undo_state : state;
    }

    boolean getOperativeStateHandled() {
        return getUndoing() ? undo_state_handled : state_handled;
    }

    boolean getUndoing() {
        return workflowGraph.getWorkflow().getUndoStepId() != null;
    }

    public List<Name> getDependsNames() {
        return dependsNames;
    }

    String getDependsString() throws NoSuchAlgorithmException, Exception {
        if (dependsString == null) {
            List<String> d = new ArrayList<String>();
            for (WorkflowStep parent : parents)
                d.add(parent.getBaseName());
            Collections.sort(d);
            dependsString = d.toString();
        }
        return dependsString;
    }

    public void addDependsName(Name dependsName) {
        dependsNames.add(dependsName);
    }

    List<Name> getDependsGlobalNames() {
        return dependsGlobalNames;
    }

    public void addDependsGlobalName(Name dependsName) {
        dependsGlobalNames.add(dependsName);
    }

    List<Name> getDependsExternalNames() {
        return dependsExternalNames;
    }

    public void addDependsExternalName(Name dependsName) {
        dependsExternalNames.add(dependsName);
    }

    // TODO Java6 @Override
    public void setXmlFile(String subgraphXmlFileName) {
        this.subgraphXmlFileName = subgraphXmlFileName;
        isSubgraphCall = true;
    }

    // TODO Java6 @Override
    public String getSubgraphXmlFileName() {
        return subgraphXmlFileName;
    }

    // TODO Java6 @Override
    public void setSourceXmlFileName(String fileName) {
        this.sourceXmlFileName = fileName;
    }

    // TODO Java6 @Override
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

    // write this step to the db, if not already there.
    // called during workflow initialization
    void initializeStepTable(Set<String> stepNamesInDb,
            PreparedStatement insertStmt, PreparedStatement updateStmt)
            throws SQLException, NoSuchAlgorithmException, Exception {
        if (stepNamesInDb.contains(getFullName())) {
            updateStmt.setString(1, getDependsString());
            updateStmt.setInt(2, getDepthFirstOrder());
            updateStmt.setString(3, getFullName());
            updateStmt.execute();
        } else {
            insertStmt.setString(1, getFullName());
            insertStmt.setString(2, Workflow.READY);
            insertStmt.setString(3, getDependsString());
            insertStmt.setString(4, invokerClassName);
            insertStmt.setString(5, getParamsDigest());
            insertStmt.setInt(6, getDepthFirstOrder());
            insertStmt.execute();
        }
    }

    // static method
    static String getBulkSnapshotSql(int workflow_id, String workflowStepTable) {
        return "SELECT name, workflow_step_id, state, state_handled, undo_state, undo_state_handled, off_line, stop_after, process_id, start_time, end_time, host_machine"
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
    void substituteValues(Map<String, String> variables, boolean check) {

	String where = workflowGraph.getXmlFileName() + ", step " + getFullName() + " ";

        if (subgraphXmlFileName != null) {
            subgraphXmlFileName =
		Utilities.substituteVariablesIntoString(subgraphXmlFileName, variables,
							where, check, "subgraphXmlFileName");
        }

        if (externalName != null) {
            externalName =
		Utilities.substituteVariablesIntoString(externalName, variables,
							where, check, "externalName");
        }

        if (forceDoneFileName != null) {
	    forceDoneFileName = Utilities.substituteVariablesIntoString(forceDoneFileName, variables,
									where, check, "forceDoneFileName");
        }

        for (String paramName : paramValues.keySet()) {
            String paramValue = paramValues.get(paramName);
            String newParamValue =
		Utilities.substituteVariablesIntoString(paramValue, variables,
							where, check, "paramValue");
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
        for (String paramName : paramValues.keySet()) {
            String paramValue = paramValues.get(paramName);
            String newParamValue = Utilities.substituteMacrosIntoString(
                    paramValue, globalProps);
            paramValues.put(paramName, newParamValue);
            if (newParamValue.indexOf("@@") != -1)
                Utilities.error("In graph file "
                        + workflowGraph.getXmlFileName() + ", parameter '"
                        + paramName + "' in step '" + getFullName()
                        + "' includes an unresolvable macro reference: '"
                        + newParamValue + "'");
        }
    }

    private String processIfString(String type, String ifString,
            Map<String, String> variables, boolean check) {

	String where = workflowGraph.getXmlFileName() + ", step " + getFullName() + " ";

        String newIf =
	    Utilities.substituteVariablesIntoString(ifString, variables,
						    where, check, type);

        if (check) {
            if (!newIf.equals("true") && !newIf.equals("false"))
                Utilities.error("In graph file "
                        + workflowGraph.getXmlFileName() + ", " + type
                        + " in step '" + getFullName()
                        + "' is neither 'true' nor 'false': '" + newIf + "'");
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

    protected void executeSqlUpdate(String sql) throws SQLException,
            FileNotFoundException, IOException {
        workflowGraph.getWorkflow().executeSqlUpdate(sql);
    }

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

    public int compareTo(WorkflowStep s) {
        return baseName.compareTo(s.getBaseName());
    }

    public void setGroupName(Name groupName) {
        // value is not needed in this implementation;
        // added for interface compliance only
    }

    // TODO Java6 @Override
    public String getGroupName() {
        throw new UnsupportedOperationException(
                "This implementation does not support this method.");
    }

    // TODO Java6 @Override
    public void setGroupName(String arg0) {
        // value is not needed in this implementation;
        // added for interface compliance only
    }
}
