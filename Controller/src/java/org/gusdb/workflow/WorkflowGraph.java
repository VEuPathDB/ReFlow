package org.gusdb.workflow;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Properties;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.xml.sax.SAXException;

/*
 * Overall subgraph strategy
 *  (1) parse a graph (starting w/ root graph)
       - read xml, and digest it
 *     - set parent child links
       - insert sugraph return nodes
 *  
 *  (2) expand subgraphs
 *     - starting with root graph, bottom up recursion through graph/subgraph
 *       hierarchy.  
 *     - for each graph
           - parse as in (1)
           - expand its subgraphs
             - replace each calling step with a pair of steps: caller and return
             - move caller's children to return; make return be caller's only child
           - insert it into parent graph
              - attach its root steps to parent caller
              - attach its leaf steps to parent return
 *   
 *  (3) in a final pass, set the path of each of the steps (top down recursion)
 * 
 * 
 */

public class WorkflowGraph<T extends WorkflowStep> {
    private List<String> paramDeclarations = new ArrayList<String>();
    private Map<String,String> constants = new LinkedHashMap<String,String>();
    private Map<String,String> globalConstants;
    private Map<String,String> tmpGlobalConstants = new LinkedHashMap<String,String>();
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

    public void addConstant(NamedValue constant) {
	constants.put(constant.getName(),constant.getValue());
    }
    
    public void addGlobalConstant(NamedValue constant) {
        tmpGlobalConstants.put(constant.getName(),constant.getValue());
    }
    
    public void addParamDeclaration(Name paramName) {
	paramDeclarations.add(paramName.getName());
    }
    
    public boolean getIsGlobal() {
        return isGlobal;
    }

    void setIsGlobal(boolean isGlobal) {
	this.isGlobal = isGlobal;
    }

    // called in the order found in the XML file.  stepsByName retains
    // that order.  this keeps the global subgraph first, if there is one
    public void addStep(T step) throws FileNotFoundException, IOException {
        step.setWorkflowGraph(this);
        String stepName = step.getBaseName();
        if (stepsByName.containsKey(stepName))
            Utilities.error("In graph " + xmlFileName + ", non-unique step name: '" + stepName + "'");
        
        stepsByName.put(stepName, step);
    }
    
    void setWorkflow(Workflow<T> workflow) {
        this.workflow = workflow;
    }

    // a step that is a call to a globalSubgraph
    public void addGlobalStep(T step) throws FileNotFoundException, IOException {
	step.setIsGlobal(true);
	addStep(step);
    }
    
    Workflow<T> getWorkflow() {
        return workflow;
    }

    void setXmlFileName(String xmlFileName) {
     this.xmlFileName = xmlFileName;
    }

    public String getXmlFileName() {
	return xmlFileName;
    }
    
    void setGlobalConstants(Map<String,String> globalConstants) {
        this.globalConstants = globalConstants;
    }
    
    void setGlobalSteps(Map<String, T> globalSteps) {
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
            for (T rootStep : rootSteps) {
                rootStep.addToList((List<WorkflowStep>)sortedSteps);
            }
	    // second pass to give everybody their order number;
	    for (T step : sortedSteps) step.setDepthFirstOrder(depthFirstOrder++); 
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
    private void postprocessSteps() throws FileNotFoundException, IOException {

	// digester loads global constants into a tmp structure.  we validate
	// as a post-process because validation must happen after other
	// properties are set, which digester does later
        if (isGlobal) globalConstants.putAll(tmpGlobalConstants);
        else if (tmpGlobalConstants.size() != 0) 
            Utilities.error("In graph " + xmlFileName + " a <globalConstant> is declared, but this graph is not global");

	if (isGlobal) globalStepsByName.putAll(stepsByName);

        // getSteps retains the order in the XML file, so global subgraph
        // will come first, if there is one.  this ensures that it is first
        // in subgraphCallerSteps
        Set<String>stepNamesSoFar = new HashSet<String>();
        for (T step : getSteps()) {

	    // make the parent/child links from the remembered dependencies
            makeParentChildLinks(step.getDependsNames(), stepsByName, step, false, stepNamesSoFar);
            stepNamesSoFar.add(step.getBaseName());

            // remember steps that call a subgraph
            if (step.getSubgraphXmlFileName() != null) subgraphCallerSteps.add(step);      

	    // validate loadType
	    step.checkLoadTypes();
        }
    }
    
    // global = true if we are making links to global parents
    private void makeParentChildLinks(List<Name> dependsNames, Map<String, T> steps,
			      T step, Boolean global, Set<String>stepNamesSoFar) {
	String globalStr = global? "global " : "";
        for (Name dependName : dependsNames) {
	    String dName = dependName.getName();
            T parent = steps.get(dName);
            if (parent == null) {      
                Utilities.error("In file " + xmlFileName + ", step '"
                                + step.getBaseName() + "' depends on "
				+ globalStr + "step '"
				+ dName + "' which is not found");
            }
            
            if (!global && !stepNamesSoFar.contains(dName)) {
                Utilities.error("In file " + xmlFileName + ", step '"
                        + step.getBaseName() + "' depends on "
                        + globalStr + "step '"
                        + dName + "' which is not above it in the XML file");
                
            }

	    // global parent-child links are made after subgraph expansion,
	    // not before as is done for standard parent-child links.
	    // if processing global parents and the parent is a subgraph
	    // call, compensate for the fact that expansion has already created
	    // the subgraph return step.
	    // force the children to be attached to the return.
	    // if we don't, the global kids are attached to call, which is wrong
	    if (global && parent.getIsSubgraphCall()) {
		step.addParent(parent.getSubgraphReturnStep());
		parent.getSubgraphReturnStep().addChild(step);
	    }
	    else {
		step.addParent(parent);
		parent.addChild(step);
	    }
        }
        
    }

    // delete steps with includeIf = false
    private void deleteExcludedSteps() throws java.io.IOException {
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
                sortedSteps = null;  // force re-creation of this list
            }
        }    
    }

    // for each step that calls a subgraph, add a fake step after it
    // called a "subgraph return child."  move all children dependencies to
    // the src.  this makes it easy to inject the subgraph between the step
    // and its src.
    @SuppressWarnings("unchecked")
    void insertSubgraphReturnChildren() {
	Map<String, T> currentStepsByName = new HashMap<String, T>(stepsByName);
        for (T step : currentStepsByName.values()) {
	    T returnStep = step;
            if (step.getIsSubgraphCall()) {
                returnStep = (T)step.insertSubgraphReturnChild();
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
    
    public String toString() {
        return "Constants" + nl + constants.toString() + nl + nl
        + "Steps" + nl + getSortedSteps().toString();
    }
    
    private void instantiateValues(String stepBaseName, String callerXmlFileName,
            Map<String, String> globalConstants, Map<String,String> paramValues,
            Map<String,Map<String,List<String>>> paramErrorsMap) {

        // confirm that caller has values for each of this graph's declared
        // parameters.  gather all such errors into fileErrorsMap for reporting
        // in total later
        for (String decl : paramDeclarations) {
            if (!paramValues.containsKey(decl)) {
                if (!paramErrorsMap.containsKey(callerXmlFileName))
                    paramErrorsMap.put(callerXmlFileName, new HashMap<String,List<String>>());
                Map<String,List<String>> fileErrorsMap = paramErrorsMap.get(callerXmlFileName);
                if (!fileErrorsMap.containsKey(stepBaseName)) 
                    fileErrorsMap.put(stepBaseName, new ArrayList<String>());
                if (!fileErrorsMap.get(stepBaseName).contains(decl))
                    fileErrorsMap.get(stepBaseName).add(decl);
            }
        }

        // substitute param values into constants
        substituteIntoConstants(paramValues, constants, false, false);

        // substitute param values and globalConstants into globalConstants
        if (isGlobal) {
            substituteIntoConstants(paramValues, globalConstants, false, false);
            
            substituteIntoConstants(new HashMap<String,String>(), globalConstants, true, true);
        }

        // substitute globalConstants into constants
        substituteIntoConstants(globalConstants, constants, false, false);

        // substitute constants into constants
        substituteIntoConstants(new HashMap<String,String>(), constants, true, true);

        // substitute them all into step param values, xmlFileName,
        // includeIf and excludeIf
        for (T step : getSteps()) {
            step.substituteValues(globalConstants, false);
            step.substituteValues(constants, false);
            step.substituteValues(paramValues, true);
        }
    }

    private void instantiateMacros(Map<String,String> macroValues) {

        // substitute them all into step param values, xmlFileName,
        // includeIf and excludeIf
        for (T step : getSteps()) {
            step.substituteMacros(macroValues);
        }
    }

    void substituteIntoConstants(Map<String,String> from, Map<String,String> to,
            boolean updateFrom, boolean check) {
        for (String constantName : to.keySet()) {
            String constantValue = to.get(constantName);
	    String newConstantValue = 
		Utilities.substituteVariablesIntoString(constantValue,
							from);
	    to.put(constantName, newConstantValue); 
	    if (updateFrom) from.put(constantName, newConstantValue);   
            if (check) {
                if (newConstantValue.indexOf("$$") != -1) 
                    Utilities.error("Constant '" + constantName + "' in graph '" 
                                    + xmlFileName 
                                    + "' includes an unresolvable variable reference: '"
                                    + newConstantValue + "'");
            }

        }
    }


    ////////////////////////////////////////////////////////////////////////
    //    Invert 
    ////////////////////////////////////////////////////////////////////////
    @SuppressWarnings("unchecked")
    void convertToUndo() throws FileNotFoundException, SQLException, IOException {
        
        // find all descendants of the undo root
        WorkflowStep undoRootStep = stepsByName.get(workflow.getUndoStepName());
        Set<WorkflowStep> undoDescendants = undoRootStep.getDescendants();
	undoDescendants.add(undoRootStep);

	// reset stepsByName to hold only descendants of undo root that are DONE
        stepsByName = new HashMap<String,T>();
        for (WorkflowStep step : undoDescendants) {
            if (step.getState().equals(Workflow.DONE))
		stepsByName.put(step.getFullName(), (T)step);
        }

        // invert each step (in trimmed graph)
        for (T step : getSteps()) step.invert(stepsByName.keySet());
 
	// remove undoRootStep's children (it is the new leaf)
	undoRootStep.removeAllChildren();
        
	// reset root and leaf sets
	setRootsAndLeafs();

        // make sure all undoable steps in db have state set
        PreparedStatement undoStepPstmt = WorkflowStep.getPreparedUndoUpdateStmt(workflow.getDbConnection(), workflow.getId()); 
        try {
            for (WorkflowStep step : getSteps()) {
                undoStepPstmt.setString(1, step.getFullName());
                undoStepPstmt.execute();
            }
        } finally {
            undoStepPstmt.close();;
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //    Manage DB 
    ////////////////////////////////////////////////////////////////////////

    // check if the in-memory graph matches that in the db exactly
    boolean inDbExactly(boolean stepTableEmpty) throws SQLException, FileNotFoundException, NoSuchAlgorithmException, IOException, Exception {

        if (stepTableEmpty) return false;
        
        String sql = "select name, params_digest, depends_string, step_class, state"
	    + " from apidb.workflowstep"  
	    + " where workflow_id = " + workflow.getId()
	    + " order by depth_first_order";

	Statement stmt = null;
	ResultSet rs = null;
	
        StringBuffer diffs = new StringBuffer();
        StringBuffer errors = new StringBuffer();
	
	List<String> notInDb = new ArrayList<String>(stepsByName.keySet());
	
	try {
	    stmt = workflow.getDbConnection().createStatement();
	    rs = stmt.executeQuery(sql);
	    int count = 0;
	    while(rs.next()) {
	        count++;
		String dbName = rs.getString(1);
		String dbParamsDigest = rs.getString(2);
		String dbDependsString = rs.getString(3);
		String dbClassName = rs.getString(4);
		String dbState = rs.getString(5);
		
		T step = stepsByName.get(dbName);
		
		if (step == null) {
		    String diff = "Step '" + dbName +
		                "' has been deleted (or excluded) from the XML file";
		    diffs.append(diff);
		    if (!(dbState.equals(Workflow.READY) || dbState.equals(Workflow.ON_DECK))) {
		        errors.append(diff + " while in the state '" + dbState + '"' + nl + nl);
		    } 
		} else {
		    notInDb.remove(dbName);
		
		    // update diffs and errors depending on mismatch found, if any
		    checkStepMismatch(step, 
		            dbName, dbParamsDigest, dbDependsString, dbClassName, dbState,
		            diffs, errors);
		}
	    }

	    if (notInDb.size() != 0) {
	        diffs.append("The following steps are in the XML graph, but not yet in the WorkflowStep table");
	        for (String t : notInDb) diffs.append("   " + t + nl);
	    }
	    
	    if (errors.length() != 0) {
	        workflow.log(errors.toString());
	        Utilities.error("The XML graph has changed illegally.  See log for details");
	    } 
            return diffs.length() == 0;
	} finally {
	    if (rs != null) rs.close();
	    if (stmt != null) stmt.close(); 
	}  
    }

    void checkStepMismatch(T step, String dbName, String dbParamsDigest,
            String dbDependsString, String dbClassName, String dbState,
            StringBuffer diffs, StringBuffer errors) throws NoSuchAlgorithmException, Exception {
        
        boolean stepClassMatch = 
            (dbClassName == null && step.getStepClassName() == null)
            || ((dbClassName != null && step.getStepClassName() != null)
                    && step.getStepClassName().equals(dbClassName));

        // don't require that the param digest of a subgraph call agrees.
        // this way steps can be grafted into a graph, and new params can
        // be passed to them.  as long as existing steps have matching
        // param digests, all is ok
        boolean mismatch =
            (!step.getIsSubgraphCall() && !step.getParamsDigest().equals(dbParamsDigest))
            || !stepClassMatch
            || !step.getDependsString().equals(dbDependsString);

        if (mismatch) {
            String s = "Step '" + dbName +
            "' has changed in the XML file";
            String diff = 
                "old name:              " + dbName + nl
                + "old params digest:     " + dbParamsDigest + nl
                + "old depends string:    " + dbDependsString + nl
                + "old class name:        " + dbClassName + nl
                + nl
                + "new name:              " + step.getFullName() + nl
                + "new params digest:     " + step.getParamsDigest() + nl
                + "new depends string:    " + step.getDependsString() + nl
                + "new class name:        " + step.getStepClassName();
            diffs.append(s + diff);
            if (!dbState.equals(Workflow.READY) && !dbState.equals(Workflow.ON_DECK))
                errors.append(s + " while in the state '" + dbState + "'" + nl + diff + nl + nl);
        }
    }

	
	
    // remove from the db all READY or ON_DECK steps
    void removeReadyStepsFromDb() throws SQLException, FileNotFoundException, IOException {
	String sql = "delete from apidb.workflowstep where workflow_id = "
	    + workflow.getId() + 
	    " and (state = 'READY' or state = 'ON_DECK')";
	workflow.executeSqlUpdate(sql);
    }

    Set<String> getStepNamesInDb() throws SQLException, FileNotFoundException, IOException {
	Set<String> stepsInDb = new HashSet<String>();

	String sql = "select name"
	    + " from apidb.workflowstep"  
	    + " where workflow_id = " + workflow.getId()
	    + " order by depth_first_order";

	Statement stmt = null;
	ResultSet rs = null;
	try {
	    stmt = workflow.getDbConnection().createStatement();
	    rs = stmt.executeQuery(sql);
	    while (rs.next()) {
		String dbName = rs.getString(1);
		stepsInDb.add(dbName);
	    }
	} finally {
	    if (rs != null) rs.close();
	    if (stmt != null) stmt.close(); 
	}  
	return stepsInDb;
    }
    
    /////////////////////////////////////////////////////////////////////////
    //   subgraph expansion
    /////////////////////////////////////////////////////////////////////////
    
    private void expandSubgraphs(String path, List<String> xmlFileNamesStack,
            Class<T> stepClass, Map<String, Map<String,List<String>>> paramErrorsMap,
            Map<String, T> globalSteps, Map<String,String> globalConstants, Map<String,String> macroValuesMap) throws SAXException, Exception {

        // iterate through all subgraph callers
        // (if there is a global subgraph caller, it will be first in the list.
        //  this way we can gather global constants before any other graph is processed)
        for (T subgraphCallerStep : subgraphCallerSteps) {

	    if (subgraphCallerStep.getExcludeFromGraph()) continue;
        
            // get the xml file of a graph to insert, and check for circularity
            String subgraphXmlFileName = subgraphCallerStep.getSubgraphXmlFileName();
            if (xmlFileNamesStack.contains(subgraphXmlFileName)) {
                throw new Exception("Circular reference to graphXmlFile '"
                                    + subgraphXmlFileName + "'"
                                    + " step path: '" + path + "'");
            }      
            
            // if is a global graph, check that it is a child of the root graph
            if (subgraphCallerStep.getIsGlobal() && !path.equals("")) {
                Utilities.error("Graph " + xmlFileName
                        + " is not the root graph, but contains a <globalSubgraph> step '"
                        + subgraphCallerStep.getBaseName() +"'.  They are only allowed in the root graph.");
            }
            
	    String newPath = path + subgraphCallerStep.getBaseName() + ".";
        
            WorkflowGraph<T> subgraph = createExpandedGraph (
                    stepClass,
                    workflow, 
                    paramErrorsMap,
                    globalSteps, 
                    globalConstants, 
                    subgraphXmlFileName, 
                    xmlFileName,
                    subgraphCallerStep.getIsGlobal(), 
                    newPath,
                    subgraphCallerStep.getBaseName(),
                    subgraphCallerStep.getParamValues(),
                    macroValuesMap,
                    subgraphCallerStep,
                    xmlFileNamesStack);       
            
            // after expanding kids, process dependsGlobal.  We do this after
            // expansion so that in root graph, the global graph is expanded
            // before processing dependsGlobal in that graph.  This is needed
	    // because it is not until we expand the global graph that the steps
	    // within it are instantiated.  the steps in non-root graphs would be
	    // ok, but there are steps in root graph that have globalDepends, and
	    // they can't make the association before the global graph is expanded
            for (T step : getSteps())                 
                makeParentChildLinks(step.getDependsGlobalNames(), globalStepsByName, step, true, null);
         
            // inject it into the caller graph
            WorkflowStep subgraphReturnStep = subgraphCallerStep.getChildren().get(0);
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
    
    private void setPath(String path) {
        for (T step : getSteps()) {
	    step.setPath(path);
	}
    }
    
    private void setCallingStep(T callingStep) {
        for (T step : getSteps()) step.setCallingStep(callingStep);
    }
    
    // attach the roots of this graph to a step in a parent graph that is
    // calling it
    private void attachToCallingStep(WorkflowStep callingStep) {
        for (T rootStep : rootSteps) {
            callingStep.addChild(rootStep);
            rootStep.addParent(callingStep);
        }
    }
    
    // attach the leafs of this graph to a step in a parent graph that is
    // the return from this graph
    private void attachToReturnStep(WorkflowStep childStep) {
        for (T leafStep : leafSteps) {
            childStep.addParent(leafStep);
            leafStep.addChild(childStep);
        }
    }
    
    ////////////////////////////////////////////////////////////////////////
    //     Static methods
    ////////////////////////////////////////////////////////////////////////
    
    /*
     * top down.  for the graph XML passed in, create that graph then iterate
     * through its calls
     * 
     */
    static <S extends WorkflowStep > WorkflowGraph<S> createExpandedGraph (
            Class<S> stepClass,
            Workflow<S> workflow, 
            Map<String, Map<String, List<String>>> paramErrorsMap,
            Map<String, S> globalSteps, 
            Map<String,String> globalConstants, 
            String xmlFileName, 
            String callerXmlFileName, 
            boolean isGlobal, 
            String path,
            String baseName,
            Map<String,String> paramValuesMap,
            Map<String,String> macroValuesMap,
	    S subgraphCallerStep,
            List<String> xmlFileNamesStack) throws FileNotFoundException, SAXException, IOException, Exception {
        
        /////////////////////////
        // create graph from XML
        ///////////////////////
        
        // parse XML into objects
        WorkflowXmlParser<S> parser = new WorkflowXmlParser<S>();
        WorkflowGraph<S> graph =
                parser.parseWorkflow(workflow, stepClass, xmlFileName,
                        globalSteps, globalConstants, isGlobal); 

        // clean up the unexpanded graph, after its creation by digester
	graph.postprocessSteps();
	
	
	/////////////////////////
	// prepare for expansion
	///////////////////////
	
	// for each step that calls a subgraph, give it a child step
	// that is the subgraph return, and move its kids there.
	graph.insertSubgraphReturnChildren();

        // set the full path of its unexpanded steps
        graph.setPath(path);

        // set the caller step of its unexpanded steps, since we now know it now
        graph.setCallingStep(subgraphCallerStep);
        
	// instantiate local macros before expanding subgraphs
        graph.instantiateMacros(macroValuesMap);

        // instantiate global and local param values before expanding subgraphs
        graph.instantiateValues(baseName,
                                callerXmlFileName,
                                globalConstants,
                                paramValuesMap,
                                paramErrorsMap);
        
	graph.setRootsAndLeafs();

	////////////////////
        // expand subgraphs
	//////////////////
        List<String> newXmlFileNamesStack = new ArrayList<String>(xmlFileNamesStack);
        newXmlFileNamesStack.add(xmlFileName);
        graph.expandSubgraphs(path, newXmlFileNamesStack, stepClass, paramErrorsMap,
                globalSteps, globalConstants, macroValuesMap);
        
        // delete excluded steps
	graph.deleteExcludedSteps();
        
        return graph;
    }
    
    public static <S extends WorkflowStep > WorkflowGraph<S> constructFullGraph(Class<S> stepClass,
            Workflow<S> workflow) throws FileNotFoundException, SAXException, IOException, Exception {
        
        // create structures to hold global steps and constants
        Map<String, S> globalSteps = new HashMap<String, S>();
        Map<String,String> globalConstants = new LinkedHashMap<String,String>();
        
        // construct map that will accumulate error messages
        Map<String,Map<String,List<String>>> paramErrorsMap =
            new HashMap<String,Map<String,List<String>>>();
        
        List<String> xmlFileNamesStack = new ArrayList<String>();
       
        WorkflowGraph<S> graph = createExpandedGraph (
                stepClass,
                workflow, 
                paramErrorsMap,
                globalSteps, 
                globalConstants, 
                workflow.getWorkflowXmlFileName(), 
                "rootParams.prop",
                false, 
                "",
                "root",
                getRootGraphParamValues(workflow),
                getGlobalPropValues(workflow),
                null,
                xmlFileNamesStack);       
        
        // report param errors, if any
        if (paramErrorsMap.size() != 0) {
            StringBuffer buf = new StringBuffer();
            for (String file : paramErrorsMap.keySet()) {
                buf.append(nl + "  File " + file + ":" + nl);
                for (String step : paramErrorsMap.get(file).keySet()) {
                    buf.append("      step: " + step + nl);
                    for (String param : paramErrorsMap.get(file).get(step)) 
                        buf.append("          > " + param + nl);
                }
            }                        
            Utilities.error("Graph \"compilation\" failed.  The following subgraph parameter values are missing:" + nl + buf);
        }
        
        return graph;
    }   
    
    
    static <S extends WorkflowStep > Map<String,String>getRootGraphParamValues(Workflow<S> workflow) throws FileNotFoundException, IOException {
        return readPropFile(workflow, "rootParams.prop");
    }
    
    static <S extends WorkflowStep > Map<String,String>getGlobalPropValues(Workflow<S> workflow) throws FileNotFoundException, IOException {
        return readPropFile(workflow, "stepsShared.prop");
    }
    
    @SuppressWarnings("unchecked")
    static <S extends WorkflowStep > Map<String,String>readPropFile(Workflow<S> workflow, String propFile) throws FileNotFoundException, IOException {
        Properties paramValues = new Properties();
        paramValues.load(new FileInputStream(workflow.getHomeDir() + "/config/" + propFile));
        Map<String,String>map = new HashMap<String,String>();
        Enumeration<String> e = (Enumeration<String>) paramValues.propertyNames();
        while(e.hasMoreElements()) {
            String k = e.nextElement();
            map.put(k,paramValues.getProperty(k));
        }
        return map;
    }
    
}
