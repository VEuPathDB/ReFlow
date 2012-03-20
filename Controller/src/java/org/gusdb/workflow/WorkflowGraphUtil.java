package org.gusdb.workflow;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.gusdb.workflow.xml.WorkflowXmlParser;
import org.xml.sax.SAXException;

public class WorkflowGraphUtil {

    private final static String NL = System.getProperty("line.separator");

    // //////////////////////////////////////////////////////////////////////
    // Static methods
    // //////////////////////////////////////////////////////////////////////

    /*
     * top down. for the graph XML passed in, create that graph then iterate
     * through its calls
     */
    static <S extends WorkflowStep> WorkflowGraph<S> createExpandedGraph(
            Class<S> stepClass, Class<WorkflowGraph<S>> containerClass,
            Workflow<S> workflow,
            Map<String, Map<String, List<String>>> paramErrorsMap,
            Map<String, S> globalSteps, Map<String, String> globalConstants,
            String xmlFileName, String callerXmlFileName, 
            String skipIfFileName, boolean isGlobal,
            String path, String baseName, Map<String, String> paramValuesMap,
            Map<String, String> macroValuesMap, S subgraphCallerStep,
            List<String> xmlFileNamesStack) throws FileNotFoundException,
            SAXException, IOException, Exception {

        // ///////////////////////
        // create graph from XML
        // /////////////////////

        // parse XML into objects
        WorkflowXmlParser<S, WorkflowGraph<S>> parser = new WorkflowXmlParser<S, WorkflowGraph<S>>();
        WorkflowGraph<S> graph = parser.parseWorkflow(stepClass,
                containerClass, xmlFileName, callerXmlFileName);
        graph.setWorkflow(workflow);
        graph.setIsGlobal(isGlobal);
        graph.setGlobalConstants(globalConstants);
        graph.setGlobalSteps(globalSteps);

        // clean up the unexpanded graph, after its creation by digester
        graph.postprocessSteps();

        // ///////////////////////
        // prepare for expansion
        // /////////////////////

        // for each step that calls a subgraph, give it a child step
        // that is the subgraph return, and move its kids there.
        graph.insertSubgraphReturnChildren();

        // set the full path of its unexpanded steps
        graph.setPath(path);

        // set the caller step of its unexpanded steps, since we now know it now
        graph.setCallingStep(subgraphCallerStep);

	// must happen before instantiation.  
	graph.setStepsSkipIfFileName(skipIfFileName);

        // instantiate global and local param values before expanding subgraphs
        graph.instantiateValues(baseName, callerXmlFileName, globalConstants,
                paramValuesMap, paramErrorsMap);

        // instantiate local macros before expanding subgraphs
        graph.instantiateMacros(macroValuesMap);

        graph.setRootsAndLeafs();

        // //////////////////
        // expand subgraphs
        // ////////////////
        List<String> newXmlFileNamesStack = new ArrayList<String>(
                xmlFileNamesStack);
        newXmlFileNamesStack.add(xmlFileName);
        graph.expandSubgraphs(path, newXmlFileNamesStack, stepClass,
                containerClass, paramErrorsMap, globalSteps, globalConstants,
                macroValuesMap);

        // delete excluded steps
        graph.deleteExcludedSteps();

        return graph;
    }

    public static <S extends WorkflowStep> WorkflowGraph<S> constructFullGraph(
            Class<S> stepClass, Class<WorkflowGraph<S>> containerClass,
            Workflow<S> workflow) throws FileNotFoundException, SAXException,
            IOException, Exception {

        // create structures to hold global steps and constants
        Map<String, S> globalSteps = new HashMap<String, S>();
        Map<String, String> globalConstants = new LinkedHashMap<String, String>();

        // construct map that will accumulate error messages
        Map<String, Map<String, List<String>>> paramErrorsMap = new HashMap<String, Map<String, List<String>>>();

        List<String> xmlFileNamesStack = new ArrayList<String>();

        WorkflowGraph<S> graph = createExpandedGraph(stepClass, containerClass,
                workflow, paramErrorsMap, globalSteps, globalConstants,
		workflow.getWorkflowXmlFileName(), "rootParams.prop", null, false,
                "", "root", getRootGraphParamValues(workflow),
                getGlobalPropValues(workflow), null, xmlFileNamesStack);

        // report param errors, if any
        if (paramErrorsMap.size() != 0) {
            StringBuffer buf = new StringBuffer();
            for (String file : paramErrorsMap.keySet()) {
                buf.append(NL + "  File " + file + ":" + NL);
                for (String step : paramErrorsMap.get(file).keySet()) {
                    buf.append("      step: " + step + NL);
                    for (String param : paramErrorsMap.get(file).get(step))
                        buf.append("          > " + param + NL);
                }
            }
            Utilities.error("Graph \"compilation\" failed.  The following subgraph parameter values are missing:"
                    + NL + buf);
        }

        graph.resolveExternalDepends();

        return graph;
    }

    static <S extends WorkflowStep> Map<String, String> getRootGraphParamValues(
            Workflow<S> workflow) throws FileNotFoundException, IOException {
        return readPropFile(workflow, "rootParams.prop");
    }

    static <S extends WorkflowStep> Map<String, String> getGlobalPropValues(
            Workflow<S> workflow) throws FileNotFoundException, IOException {
        Map<String, String> map = readPropFile(workflow, "stepsShared.prop");

        // make these available to graph files
        map.put("workflowName", workflow.getName());
        map.put("workflowVersion", workflow.getName());
        return map;
    }

    @SuppressWarnings("unchecked")
    static <S extends WorkflowStep> Map<String, String> readPropFile(
            Workflow<S> workflow, String propFile)
            throws FileNotFoundException, IOException {
        Properties paramValues = new Properties();
        paramValues.load(new FileInputStream(workflow.getHomeDir() + "/config/"
                + propFile));
        Map<String, String> map = new HashMap<String, String>();
        Enumeration<String> e = (Enumeration<String>) paramValues.propertyNames();
        while (e.hasMoreElements()) {
            String k = e.nextElement();
            map.put(k, paramValues.getProperty(k));
        }
        return map;
    }
}
