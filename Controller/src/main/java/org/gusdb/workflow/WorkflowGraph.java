package org.gusdb.workflow;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gusdb.fgputil.xml.Name;
import org.gusdb.fgputil.xml.NamedValue;
import org.gusdb.workflow.xml.WorkflowXmlContainer;
import org.gusdb.workflow.xml.ParamDeclaration;
import org.gusdb.workflow.xml.WorkflowClassFactory;
import org.xml.sax.SAXException;

/*
 * Overall subgraph strategy
 *  (1) parse a graph (starting w/ root graph)
 *     - read xml, and digest it
 *     - set parent child links
 *     - insert subgraph return nodes
 *  
 *  (2) expand subgraphs
 *     - starting with root graph, bottom up recursion through graph/subgraph
 *       hierarchy.  
 *     - for each graph
 *         - parse as in (1)
 *         - expand its subgraphs
 *           - replace each calling step with a pair of steps: caller and return
 *           - move caller's children to return; make return be caller's only child
 *         - insert it into parent graph
 *            - attach its root steps to parent caller
 *            - attach its leaf steps to parent return
 *   
 *  (3) in a final pass, set the path of each of the steps (top down recursion)
 */

public class WorkflowGraph<T extends WorkflowStep> implements WorkflowXmlContainer<T> {

    public static final Character FLAG_DIVIDER = ':';

    private List<ParamDeclaration> paramDeclarations = new ArrayList<ParamDeclaration>();
    private Map<String, String> constants = new LinkedHashMap<String, String>();
    private Map<String, String> globalConstants;
    private Map<String, String> tmpGlobalConstants = new LinkedHashMap<String, String>();
    private Map<String, T> globalStepsByName;
    private Workflow<T> workflow;
    private String xmlFileName;
    private boolean isGlobal = false;

    private List<T> subgraphCallerSteps = new ArrayList<T>();
    private List<T> rootSteps = new ArrayList<T>();

    // following state must be updated after expansion
    private Map<String, T> stepsByName = new LinkedHashMap<String, T>();
    private List<T> leafSteps = new ArrayList<T>();
    private List<T> sortedSteps;

    final static String nl = System.getProperty("line.separator");

    public WorkflowGraph() {}
    
    @Override
    public void addConstant(NamedValue constant) {
        constants.put(constant.getName(), constant.getValue());
    }

    @Override
    public void addGlobalConstant(NamedValue constant) {
        tmpGlobalConstants.put(constant.getName(), constant.getValue());
    }

    @Override
    public void addParamDeclaration(ParamDeclaration paramDecl) {
        paramDeclarations.add(paramDecl);
    }

    public boolean getIsGlobal() {
        return isGlobal;
    }

    public void setIsGlobal(boolean isGlobal) {
        this.isGlobal = isGlobal;
    }

    // called in the order found in the XML file. stepsByName retains
    // that order. this keeps the global subgraph first, if there is one
    @Override
    public void addStep(T step) throws IOException {
        step.setWorkflowGraph(this);
        String stepName = step.getBaseName();
        if (stepsByName.containsKey(stepName))
            Utilities.error("In graph " + xmlFileName
                    + ", non-unique step name: '" + stepName + "'");

        stepsByName.put(stepName, step);
    }

    void setWorkflow(Workflow<T> workflow) {
        this.workflow = workflow;
    }

    // a step that is a call to a globalSubgraph
    @Override
    public void addGlobalStep(T step) throws IOException {
        step.setIsGlobal(true);
        addStep(step);
    }

    Workflow<T> getWorkflow() {
        return workflow;
    }

    @Override
    public void setXmlFileName(String xmlFileName) {
        this.xmlFileName = xmlFileName;
    }

    public String getXmlFileName() {
        return xmlFileName;
    }

    public void setGlobalConstants(Map<String, String> globalConstants) {
        this.globalConstants = globalConstants;
    }

    public void setGlobalSteps(Map<String, T> globalSteps) {
        this.globalStepsByName = globalSteps;
    }

    public List<T> getRootSteps() {
        return rootSteps;
    }

    // recurse through steps starting at roots.
    @SuppressWarnings("unchecked")
    public List<T> getSortedSteps() {
        if (sortedSteps == null) {
            int depthFirstOrder = 0;
            sortedSteps = new ArrayList<T>();
            Set<T> poppedSteps = new HashSet<T>(); // steps whose kids are done
                                                   // processing
            for (T rootStep : rootSteps) {
                rootStep.addToList((List<WorkflowStep>) sortedSteps,
                        (Set<WorkflowStep>) poppedSteps);
                poppedSteps.add(rootStep);
            }
            // second pass to give everybody their order number;
            for (T step : sortedSteps)
                step.setDepthFirstOrder(depthFirstOrder++);
        }
        return sortedSteps;
    }

    Map<String, T> getStepsByName() {
        return stepsByName;
    }

    Collection<T> getSteps() {
        return stepsByName.values();
    }

    String getStepsAsString() {
        StringBuffer buf = new StringBuffer();
        for (T step : getSortedSteps()) {
            buf.append(step.getFullName() + nl);
        }
        return buf.toString();
    }

    // clean up after building from xml
    void postprocessSteps() throws FileNotFoundException, IOException {

        // digester loads global constants into a tmp structure. we validate
        // as a post-process because validation must happen after other
        // properties are set, which digester does later
        if (isGlobal) globalConstants.putAll(tmpGlobalConstants);
        else if (tmpGlobalConstants.size() != 0)
            Utilities.error("In graph "
                    + xmlFileName
                    + " a <globalConstant> is declared, but this graph is not global");

        if (isGlobal) globalStepsByName.putAll(stepsByName);

        // getSteps retains the order in the XML file, so global subgraph
        // will come first, if there is one. this ensures that it is first
        // in subgraphCallerSteps
        Set<String> stepNamesSoFar = new HashSet<String>();
        for (T step : getSteps()) {

            // make the parent/child links from the remembered dependencies
            makeParentChildLinks(step.getDependsNames(), stepsByName, step,
                    false, stepNamesSoFar);
            stepNamesSoFar.add(step.getBaseName());

            // remember steps that call a subgraph
            if (step.getSubgraphXmlFileName() != null)
                subgraphCallerSteps.add(step);

            // validate loadType
            step.checkLoadTypes();
        }
    }

    // global = true if we are making links to global parents
    private void makeParentChildLinks(List<Name> dependsNames,
            Map<String, T> steps, T step, Boolean global,
            Set<String> stepNamesSoFar) {
        String globalStr = global ? "global " : "";
        for (Name dependName : dependsNames) {
            String dName = dependName.getName();
            T parent = steps.get(dName);
            if (parent == null) {
                Utilities.error("In file " + xmlFileName + ", step '"
                        + step.getBaseName() + "' depends on " + globalStr
                        + "step '" + dName + "' which is not found");
            }

            if (!global && !stepNamesSoFar.contains(dName)) {
                Utilities.error("In file " + xmlFileName + ", step '"
                        + step.getBaseName() + "' depends on " + globalStr
                        + "step '" + dName
                        + "' which is not above it in the XML file");

            }
            makeParentChildLink(parent, step, global);
        }
    }

    // global and external parent-child links are made after subgraph expansion,
    // not before as is done for standard parent-child links.
    // if processing global parents and the parent is a subgraph
    // call, compensate for the fact that expansion has already created
    // the subgraph return step.
    // force the children to be attached to the return.
    // if we don't, the global/external kids are attached to call, which is
    // wrong
    void makeParentChildLink(T parent, T step, boolean isExternalOrGlobal) {
        if (isExternalOrGlobal && parent.getIsSubgraphCall()) {
            step.addParent(parent.getSubgraphReturnStep());
            parent.getSubgraphReturnStep().addChild(step);
        } else {
            step.addParent(parent);
            parent.addChild(step);
        }
    }

    /*
     * External dependencies allow steps to depend on steps in other graphs.
     * Because this jumps across the graph, there are some contraints. The main
     * one is that reference to the parent step is not to its standard name, but
     * to a specially declared exportedName. This name does not have a path, so
     * it must be unique across the fully expanded graph. (This typically only
     * works for graphs that were generated by a plan which are typically only
     * called once in the whole graph, and thus steps in the graph are unique
     * even without a path.) The other means for a cross-graph-file dependency
     * is dependsGlobal. These use the standard full name (with path) and so are
     * available anywhere, but the constraint on them is that the parent steps
     * must be in the global.xml file.
     * 
     * This method should only be called on the fully expanded graph, after
     * graph expansion is complete
     */
    void resolveExternalDepends() {
        Set<T> stepsWithExternalDepends = new HashSet<T>();
        Map<String, T> externalName2Step = new HashMap<String, T>();
        // pass through steps to find all externalNames and dependsExternal
        for (T step : getSortedSteps()) {
            if (step.getExternalName() != null) {
                if (externalName2Step.containsKey(step.getExternalName()))
                    Utilities.error("Step " + step.getBaseName() + " in graph "
                            + step.getSourceXmlFileName()
                            + " has a non-unique externalName '"
                            + step.getExternalName() + "'");
                externalName2Step.put(step.getExternalName(), step);
            }
            if (step.getDependsExternalNames() != null
                    && step.getDependsExternalNames().size() != 0)
                stepsWithExternalDepends.add(step);
        }

        for (T step : stepsWithExternalDepends) {
            for (Name externalDependsName : step.getDependsExternalNames()) {
                String extDepStr = externalDependsName.getName();
                T externalStep = externalName2Step.get(extDepStr);
                if (externalStep == step) continue;
                String err = "Step '"
                        + step.getFullName()
                        + "' has a dependsExternal '"
                        + extDepStr
                        + "' for which no referent step can be found.  (Perhaps the step with externalName=\""
                        + extDepStr + "\" has an includeIf or an excludeIf)";
                if (externalStep == null) Utilities.error(err);
                makeParentChildLink(externalStep, step, true);
            }
        }

        // make another pass through all steps, by rebuilding the sorted
        // steps list, to check for cycles in graph from the new dependencies
        sortedSteps = null;
        getSortedSteps();
    }

    // delete steps with includeIf = false
    void deleteExcludedSteps() throws java.io.IOException, Exception {
        Map<String, T> stepsTmp = new HashMap<String, T>(stepsByName);
        for (T step : stepsTmp.values()) {
            if (step.getExcludeFromGraph()) {
                for (WorkflowStep parent : step.getParents()) {
                    parent.removeChild(step);

                }

                for (WorkflowStep child : step.getChildren()) {
                    child.removeParent(step);
                    for (WorkflowStep parent : step.getParents()) {
                        parent.addChild(child);
                        child.addParent(parent);
                    }
                }
                stepsByName.remove(step.getBaseName());
                subgraphCallerSteps.remove(step);
                rootSteps.remove(step);
                leafSteps.remove(step);
            }
        }
        // rediscover root steps: if we deleted one, then its kids become root
        for (T step : getSteps()) {
            if (step.getParents().size() == 0 && !rootSteps.contains(step)) {
                rootSteps.add(step);
                sortedSteps = null; // force re-creation of this list
            }
            if (step.getChildren().size() == 0 && !leafSteps.contains(step)) {
                leafSteps.add(step);
                sortedSteps = null; // force re-creation of this list
            }
        }
    }

    void setStepsSkipIfFileName (String skipIfFileName) {
	if (skipIfFileName == null) return;
        for (T step : getSteps()) {
	    step.setSkipIfFile(skipIfFileName);
	}
    }

    // for each step that calls a subgraph, add a fake step after it
    // called a "subgraph return child." move all children dependencies to
    // the src. this makes it easy to inject the subgraph between the step
    // and its src.
    @SuppressWarnings("unchecked")
    void insertSubgraphReturnChildren() {
        Map<String, T> currentStepsByName = new HashMap<String, T>(stepsByName);
        for (T step : currentStepsByName.values()) {
            T returnStep = step;
            if (step.getIsSubgraphCall()) {
                returnStep = (T) step.insertSubgraphReturnChild();
                stepsByName.put(returnStep.getBaseName(), returnStep);
            }
        }

    }

    void setRootsAndLeafs() {
        rootSteps = new ArrayList<T>();
        leafSteps = new ArrayList<T>();
        for (T step : getSteps()) {
            if (step.getParents().size() == 0) rootSteps.add(step);
            if (step.getChildren().size() == 0) leafSteps.add(step);
        }
        sortedSteps = null;
    }

    @Override
    public String toString() {
        return "Constants" + nl + constants.toString() + nl + nl + "Steps" + nl
                + getSortedSteps().toString();
    }

    void instantiateValues(String stepBaseName, String callerXmlFileName,
            Map<String, String> globalConstants,
            Map<String, String> paramValues,
            Map<String, Map<String, List<String>>> paramErrorsMap) {

        // confirm that caller has values for each of this graph's declared
        // parameters. gather all such errors into fileErrorsMap for reporting
        // in total later
        for (ParamDeclaration decl : paramDeclarations) {
	  if (!paramValues.containsKey(decl.getName()) && decl.getDefault() != null) 
	      paramValues.put(decl.getName(), decl.getDefault());
            if (!paramValues.containsKey(decl.getName())) {
                if (!paramErrorsMap.containsKey(callerXmlFileName))
                    paramErrorsMap.put(callerXmlFileName,
                            new HashMap<String, List<String>>());
                Map<String, List<String>> fileErrorsMap = paramErrorsMap.get(callerXmlFileName);
                if (!fileErrorsMap.containsKey(stepBaseName))
                    fileErrorsMap.put(stepBaseName, new ArrayList<String>());
                if (!fileErrorsMap.get(stepBaseName).contains(decl.getName()))
		  fileErrorsMap.get(stepBaseName).add(decl.getName());
            }
        }

        // substitute param values into constants
        substituteIntoConstants(paramValues, constants, false, false);

        // substitute param values and globalConstants into globalConstants
        if (isGlobal) {
            substituteIntoConstants(paramValues, globalConstants, false, false);

            substituteIntoConstants(new HashMap<String, String>(),
                    globalConstants, true, true);
        }

        // substitute globalConstants into constants
        substituteIntoConstants(globalConstants, constants, false, false);

        // substitute constants into constants
        substituteIntoConstants(new HashMap<String, String>(), constants, true,
                true);

        // substitute them all into step param values, xmlFileName,
        // includeIf and excludeIf
        for (T step : getSteps()) {
            step.substituteValues(globalConstants, false);
            step.substituteValues(constants, false);
            step.substituteValues(paramValues, true);
        }
    }

    void instantiateMacros(Map<String, String> macroValues) {

        // substitute them all into step param values, xmlFileName,
        // includeIf and excludeIf
        for (T step : getSteps()) {
            step.substituteMacros(macroValues);
        }
    }

    void substituteIntoConstants(Map<String, String> from,
            Map<String, String> to, boolean updateFrom, boolean check) {
        for (String constantName : to.keySet()) {
            String constantValue = to.get(constantName);
            String newConstantValue =
		Utilities.substituteVariablesIntoString(constantValue, from,
							xmlFileName, check, "constant", constantName);
            to.put(constantName, newConstantValue);
            if (updateFrom) from.put(constantName, newConstantValue);
        }
    }

    // //////////////////////////////////////////////////////////////////////
    // Invert
    // //////////////////////////////////////////////////////////////////////
    @SuppressWarnings("unchecked")
    void convertToUndo() throws FileNotFoundException, SQLException,
            IOException {

        // find all descendants of the undo root
        WorkflowStep undoRootStep = stepsByName.get(workflow.getUndoStepName());
	if (undoRootStep == null) Utilities.error("Trying to undo an unrecognized step: " + undoRootStep);
        Set<WorkflowStep> undoDescendants = undoRootStep.getDescendants();
        undoDescendants.add(undoRootStep);

        // reset stepsByName to hold only descendants of undo root that are DONE
        stepsByName = new HashMap<String, T>();
        for (WorkflowStep step : undoDescendants) {
            if (step.getState().equals(Workflow.DONE))
                stepsByName.put(step.getFullName(), (T) step);
        }

        // invert each step (in trimmed graph)
        for (T step : getSteps())
            step.invert(stepsByName.keySet());

        // remove undoRootStep's children (it is the new leaf)
        undoRootStep.removeAllChildren();

        // reset root and leaf sets
        setRootsAndLeafs();

        // make sure all undoable steps in db have state set
        PreparedStatement undoStepPstmt = WorkflowStep.getPreparedUndoUpdateStmt(
                workflow.getDbConnection(), workflow.getId(),
                workflow.getWorkflowStepTable());
        try {
            for (WorkflowStep step : getSteps()) {
                undoStepPstmt.setString(1, step.getFullName());
                undoStepPstmt.execute();
            }
        }
        finally {
            undoStepPstmt.close();
        }
    }

    // //////////////////////////////////////////////////////////////////////
    // Manage DB
    // //////////////////////////////////////////////////////////////////////

    /*
     Check if the in-memory graph matches that in the db exactly
     Return string holding differences, if any.

     Following are the kinds of differences.
     All are illegal if the step is RUNNING, FAILED or DONE, except as noted.
      1) step deleted (or excluded)
      2) name
      3) step class
      4) depends
      5) params (ignored for subgraph calls)
        - new in memory (Ok if DONE)
        - deleted in memory
        - different in memory and db
    */
    String inDbExactly(boolean stepTableEmpty) throws SQLException,
            FileNotFoundException, NoSuchAlgorithmException, IOException,
            Exception {

        if (stepTableEmpty) return "Step table empty";

        String workflowStepTable = getWorkflow().getWorkflowStepTable();
        String sql = "select name, params_digest, depends_string, step_class, state, workflow_step_id"
                + " from "
                + workflowStepTable
                + " where workflow_id = "
                + workflow.getId() + " order by depth_first_order";

        Statement stmt = null;
        ResultSet rs = null;
        PreparedStatement paramValuesStmt = null;

        StringBuffer diffs = new StringBuffer();
        StringBuffer errors = new StringBuffer();

        List<String> notInDb = new ArrayList<String>(stepsByName.keySet());

        try {
            stmt = workflow.getDbConnection().createStatement();
	    paramValuesStmt =
		WorkflowStep.getPreparedParamValStmt(getWorkflow().getDbConnection(),
						     getWorkflow().getWorkflowStepParamValTable());
	    
            rs = stmt.executeQuery(sql);
            
            while (rs.next()) {
                String dbName = rs.getString(1);
                String dbParamsDigest = rs.getString(2);
                String dbDependsString = rs.getString(3);
                String dbClassName = rs.getString(4);
                String dbState = rs.getString(5);
                Integer dbId = rs.getInt(6);

                T step = stepsByName.get(dbName);

                if (step == null) {
                    String diff = "Step '"
                            + dbName
                            + "' has been deleted (or excluded) from the XML file";
                    diffs.append(diff);
                    if (!(dbState.equals(Workflow.READY) || dbState.equals(Workflow.ON_DECK))) {
                        errors.append(diff + " while in the state '" + dbState
                                + '"' + nl + nl);
                    }
                } else {
                    notInDb.remove(dbName);

                    // update diffs and errors depending on mismatch found, if any
                    checkStepMismatch(step, dbId, dbName, dbParamsDigest, dbDependsString, dbClassName,
				      dbState, diffs, errors, paramValuesStmt);
                }
            }

            if (notInDb.size() != 0) {
                diffs.append("The following steps are in the XML graph, but not yet in the WorkflowStep table");
                for (String t : notInDb)
                    diffs.append("   " + t + nl);
            }

            if (errors.length() != 0) {
                workflow.log(errors.toString());
                Utilities.error("The XML graph has changed illegally.  See controller.log for details");
            }
            return diffs.toString();
        }
        finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
            if (paramValuesStmt != null) stmt.close();
        }
    }

    // allow ready steps to add new
    void checkStepMismatch(T step, Integer dbId, String dbName, String dbParamsDigest,
            String dbDependsString, String dbClassName, String dbState,
			   StringBuffer diffs, StringBuffer errors, PreparedStatement paramValuesStmt)
	throws NoSuchAlgorithmException, SQLException, Exception {

        boolean stepClassMatch = (dbClassName == null && step.getStepClassName() == null)
                || ((dbClassName != null && step.getStepClassName() != null)
		    && step.getStepClassName().equals(dbClassName));

	boolean runningOrFailed = dbState.equals(Workflow.RUNNING) || dbState.equals(Workflow.FAILED);
	boolean done = dbState.equals(Workflow.DONE);


        // don't require that the param digest of a subgraph call agrees.
        // this way steps can be grafted into a graph, and new params can
        // be passed to them. as long as existing steps have matching
        // param digests, all is ok
        boolean mismatch =
	    (!step.getIsSubgraphCall() && !dbParamsDigest.equals(step.getParamsDigest()))
	    || !stepClassMatch
	    || !step.getDependsString().equals(dbDependsString)
	    || !step.getFullName().equals(dbName);
	     
	if (!mismatch) return;

	String s = "Step '" + dbName + "' has changed in XML file " + step.getSourceXmlFileName();
	StringBuffer diff = new StringBuffer();

	boolean illegalChange = false;
	    	
	if (!dbName.equals(step.getFullName())) {
	    diff.append("  old name:            " + dbName + nl);
	    diff.append("  new name:            " + step.getFullName() + nl);
	    illegalChange |= (runningOrFailed || done);
	}

	if (!stepClassMatch) {
	    diff.append("  old class name:      " + dbClassName + nl);
	    diff.append("  new class name:      " + step.getStepClassName() + nl);
	    illegalChange |= (runningOrFailed || done);
	}

	if (!dbDependsString.equals(step.getDependsString())) {
	    diff.append("  old depends string:  " + dbDependsString + nl);
	    diff.append("  new depends string:  " + step.getDependsString() + nl);
	    illegalChange |= (runningOrFailed || done);
	}

	if (!dbParamsDigest.equals(step.getParamsDigest())) {
	    diff.append("  old params digest:   " + dbParamsDigest + nl);
	    diff.append("  new params digest:   " + step.getParamsDigest() + nl);
	    Map<String,String> dbParamValuesDiff = new LinkedHashMap<String, String>();
	    Map<String,String> newParamValuesDiff = new LinkedHashMap<String, String>();
	    illegalChange |= step.checkChangedParams(paramValuesStmt, dbId, dbParamsDigest, dbState, dbParamValuesDiff, newParamValuesDiff);
	    diff.append("  unmatched old params:" + nl);
	    for (String paramName : dbParamValuesDiff.keySet()) {
		diff.append("    " + paramName + ": " + dbParamValuesDiff.get(paramName) + nl);
	    }
	    diff.append("unmatched new params:" + nl);
	    for (String paramName : newParamValuesDiff.keySet()) {
		diff.append("  " + paramName + ": " + newParamValuesDiff.get(paramName) + nl);
	    }
	}

	diffs.append(s + diff);

	if (illegalChange)
	    errors.append(s + " while in the state '" + dbState + "'" + nl + diff + nl + nl);
    }

    // remove from the db all READY or ON_DECK steps
    void removeReadyStepsFromDb() throws SQLException, FileNotFoundException,
            IOException {

        String workflowStepTable = getWorkflow().getWorkflowStepTable();
        String workflowStepParamValTable = getWorkflow().getWorkflowStepParamValTable();
        String workflowStepTrackingTable = getWorkflow().getWorkflowStepTrackingTable();
        String sql = "select s.name"
                + " from "
                + workflowStepTable
                + " s, "
                + workflowStepTrackingTable
                + " t"
                + " where s.workflow_id = "
                + workflow.getId()
                + " and s.workflow_step_id = t.workflow_step_id and s.state = 'READY'";

        Set<String> stepNamesInDb = getStepNamesInDb(sql);
        if (stepNamesInDb.size() != 0) {
            String msg = nl
                    + "Error. The following steps are READY but have rows in "
                    + workflowStepTrackingTable + ":" + nl;
            for (String s : stepNamesInDb)
                msg += ("  " + s + nl);
            msg += nl
                    + "These steps ran, failed and were set to ready without being cleaned up in the database.  Refer to each step's step.err file for instructions on how to clean it up in the database. When they are all clean, try running again."
                    + nl;

            Utilities.error(msg);
        }

        sql = "delete from " + workflowStepParamValTable
	    + " where workflow_step_id in (select workflow_step_id from "
	    + workflowStepTable + " where workflow_id = "
                + workflow.getId()
                + " and (state = 'READY' or state = 'ON_DECK'))";
        workflow.executeSqlUpdate(sql);

        sql = "delete from " + workflowStepTable + " where workflow_id = "
                + workflow.getId()
                + " and (state = 'READY' or state = 'ON_DECK')";
        workflow.executeSqlUpdate(sql);
    }

    Set<String> getStepNamesInDb() throws SQLException, FileNotFoundException,
            IOException {

        String workflowStepTable = getWorkflow().getWorkflowStepTable();

        String sql = "select name" + " from " + workflowStepTable
                + " where workflow_id = " + workflow.getId()
                + " order by depth_first_order";
        return getStepNamesInDb(sql);
    }

    Set<String> getStepNamesInDb(String sql) throws SQLException,
            FileNotFoundException, IOException {
        Set<String> stepsInDb = new HashSet<String>();

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = workflow.getDbConnection().createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String dbName = rs.getString(1);
                stepsInDb.add(dbName);
            }
        }
        finally {
            if (rs != null) rs.close();
            if (stmt != null) stmt.close();
        }
        return stepsInDb;
    }

    // ///////////////////////////////////////////////////////////////////////
    // subgraph expansion
    // ///////////////////////////////////////////////////////////////////////
    void expandSubgraphs(String path, List<String> xmlFileNamesStack,
            WorkflowClassFactory<T, WorkflowGraph<T>> classFactory,
            Map<String, Map<String, List<String>>> paramErrorsMap,
            Map<String, T> globalSteps, Map<String, String> globalConstants,
            Map<String, String> macroValuesMap) throws SAXException, Exception {

        // iterate through all subgraph callers
        // (if there is a global subgraph caller, it will be first in the list.
        // this way we can gather global constants before any other graph is
        // processed)
        for (T subgraphCallerStep : subgraphCallerSteps) {

            if (subgraphCallerStep.getExcludeFromGraph()) continue;

            // get the xml file of a graph to insert, and check for circularity
            String subgraphXmlFileName = subgraphCallerStep.getSubgraphXmlFileName();
            if (xmlFileNamesStack.contains(subgraphXmlFileName)) {
                throw new Exception("Circular reference to graphXmlFile '"
                        + subgraphXmlFileName + "'" + " step path: '" + path
                        + "'");
            }

            // if is a global graph, check that it is a child of the root graph
            if (subgraphCallerStep.getIsGlobal() && !path.equals("")) {
                Utilities.error("Graph "
                        + xmlFileName
                        + " is not the root graph, but contains a <globalSubgraph> step '"
                        + subgraphCallerStep.getBaseName()
                        + "'.  They are only allowed in the root graph.");
            }

            String newPath = path + subgraphCallerStep.getBaseName() + ".";

            WorkflowGraph<T> subgraph = WorkflowGraphUtil.createExpandedGraph(
                    classFactory, workflow, paramErrorsMap,
                    globalSteps, globalConstants, subgraphXmlFileName,
                    xmlFileName, subgraphCallerStep.getSkipIfFileName(),
                    subgraphCallerStep.getIsGlobal(), newPath,
                    subgraphCallerStep.getBaseName(),
                    subgraphCallerStep.getParamValues(), macroValuesMap,
                    subgraphCallerStep, xmlFileNamesStack);

            // after expanding kids, process dependsGlobal. We do this after
            // expansion so that in root graph, the global graph is expanded
            // before processing dependsGlobal in that graph. This is needed
            // because it is not until we expand the global graph that the steps
            // within it are instantiated. the steps in non-root graphs would be
            // ok, but there are steps in root graph that have globalDepends,
            // and
            // they can't make the association before the global graph is
            // expanded
            for (T step : getSteps())
                makeParentChildLinks(step.getDependsGlobalNames(),
                        globalStepsByName, step, true, null);

            // inject it into the caller graph
            WorkflowStep subgraphReturnStep = subgraphCallerStep.getChildren().get(
                    0);
            subgraphCallerStep.removeChild(subgraphReturnStep);
            subgraphReturnStep.removeParent(subgraphCallerStep);
            subgraph.attachToCallingStep(subgraphCallerStep);
            subgraph.attachToReturnStep(subgraphReturnStep);

            // add its steps to stepsByName
            for (T subgraphStep : subgraph.getSteps()) {
                stepsByName.put(subgraphStep.getFullName(), subgraphStep);
            }
        }
    }

    void setPath(String path) {
        for (T step : getSteps()) {
            step.setPath(path);
        }
    }

    void setCallingStep(T callingStep) {
        String[] loadTypes = null;
        String[] failTypes = null;
        
        // validate that since calling step is always the entry point of a
        // sub-graph,
        // tags assigned to calling step has to have path in it.
        if (callingStep != null) {
            loadTypes = callingStep.getLoadTypes();
            for (String loadType : loadTypes) validateLoadType(callingStep, "Load", loadType);
            failTypes = callingStep.getFailTypes();
            for (String failType : failTypes) validateLoadType(callingStep, "Fail", failType);
                
        }

        for (T step : getSteps()) {
            if (callingStep != null) {
              step.addLoadTypes(loadTypes);
              step.addFailTypes(failTypes);
            }
            step.setCallingStep(callingStep);
        }
    }
    
    private void validateLoadType(T callingStep, String loadOrFail, String type) {

      if (type.indexOf(FLAG_DIVIDER) < 0)
          Utilities.error("Error: <subgraph name=\""
                  + callingStep.getBaseName()
                  + "\">"
                  + " in file "
                  + callingStep.getSourceXmlFileName()
                  + " has a step" + loadOrFail + "Types=\""
                  + type
                  + "\"."
                  + " The step" + loadOrFail + "Type of a <subgraph> must have a path that leads to a <step>.  For example, step" + loadOrFail + "Type=\"genome.blast.runOnCluster:"
                  + type
                  + "\" where runOnCluster is a <step> in a nested subgraph.");
      
    }

    // attach the roots of this graph to a step in a parent graph that is
    // calling it
    void attachToCallingStep(WorkflowStep callingStep) {
        for (T rootStep : rootSteps) {
            callingStep.addChild(rootStep);
            rootStep.addParent(callingStep);
        }
    }

    // attach the leafs of this graph to a step in a parent graph that is
    // the return from this graph
    void attachToReturnStep(WorkflowStep childStep) {
        for (T leafStep : leafSteps) {
            childStep.addParent(leafStep);
            leafStep.addChild(childStep);
        }
    }
}
