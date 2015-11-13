package org.gusdb.workflow.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.digester.Digester;
import org.apache.log4j.Logger;
import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.fgputil.xml.Name;
import org.gusdb.fgputil.xml.NamedValue;
import org.gusdb.fgputil.xml.XmlParser;
import org.gusdb.fgputil.xml.XmlValidator;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class WorkflowXmlParser<T extends WorkflowNode, S extends WorkflowXmlContainer<T>> extends XmlParser {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(WorkflowXmlParser.class);

  private final Digester _digester;

  public WorkflowXmlParser(Class<T> stepClass, Class<S> containerClass) {
    _digester = configureDigester(stepClass, containerClass);
  }

  public S parseWorkflow(String xmlFileName, String callerXmlFileName)
      throws SAXException, IOException, ParserConfigurationException, TransformerException {
    return parseWorkflow(xmlFileName, callerXmlFileName, true);
  }

  public S parseWorkflow(String xmlFileName, String callerXmlFileName, boolean useGusHome)
      throws SAXException, IOException, ParserConfigurationException, TransformerException {

    // construct urls to model file, prop file, and config file
    URL modelURL = makeURL(useGusHome ? GusHome.getGusHome() + "/lib/xml/workflow/" + xmlFileName : xmlFileName);

    XmlValidator validator = new XmlValidator(GusHome.getGusHome() + "/lib/rng/workflow.rng");
    if (!validator.validate(modelURL)) {
      System.err.println("Called from: " + callerXmlFileName);
      System.exit(1);
    }

    Document doc = buildDocument(modelURL);

    System.err.println("Parsing " + xmlFileName);
    // currently there is no property substitution in ReFlow; leaving here in case we want to add later
    Map<String,String> substitutionProperties = Collections.EMPTY_MAP;
    S workflowGraph = parseXml(doc, substitutionProperties);

    workflowGraph.setXmlFileName(xmlFileName);
    return workflowGraph;
  }

  @SuppressWarnings("unchecked")
  private S parseXml(Document doc, Map<String, String> properties) throws TransformerException, IOException,
      SAXException {
    InputStream xmlStream = substituteProps(doc, properties);
    return (S) _digester.parse(xmlStream);
  }

  private Digester configureDigester(Class<T> stepClass, Class<S> containerClass) {
    Digester digester = new Digester();
    digester.setValidating(false);

    // Root -- WDK Model
    System.err.println("Class??? " + (containerClass == null ? null : containerClass.getName()));
    digester.addObjectCreate("workflowGraph", containerClass);

    configureNode(digester, "workflowGraph/param", ParamDeclaration.class, "addParamDeclaration");

    configureNode(digester, "workflowGraph/constant", NamedValue.class, "addConstant");
    digester.addCallMethod("workflowGraph/constant", "setValue", 0);

    configureNode(digester, "workflowGraph/globalConstant", NamedValue.class, "addGlobalConstant");
    digester.addCallMethod("workflowGraph/globalConstant", "setValue", 0);

    configureNode(digester, "workflowGraph/step", stepClass, "addStep");

    configureNode(digester, "workflowGraph/step/depends", Name.class, "addDependsName");

    configureNode(digester, "workflowGraph/step/dependsGlobal", Name.class, "addDependsGlobalName");

    configureNode(digester, "workflowGraph/step/dependsExternal", Name.class, "addDependsExternalName");

    configureNode(digester, "workflowGraph/step/paramValue", NamedValue.class, "addParamValue");
    digester.addCallMethod("workflowGraph/step/paramValue", "setValue", 0);

    configureNode(digester, "workflowGraph/subgraph", stepClass, "addStep");

    configureNode(digester, "workflowGraph/subgraph/depends", Name.class, "addDependsName");

    configureNode(digester, "workflowGraph/subgraph/dependsGlobal", Name.class, "addDependsGlobalName");

    configureNode(digester, "workflowGraph/subgraph/dependsExternal", Name.class, "addDependsExternalName");

    configureNode(digester, "workflowGraph/subgraph/paramValue", NamedValue.class, "addParamValue");
    digester.addCallMethod("workflowGraph/subgraph/paramValue", "setValue", 0);

    configureNode(digester, "workflowGraph/globalSubgraph", stepClass, "addGlobalStep");

    configureNode(digester, "workflowGraph/globalSubgraph/paramValue", NamedValue.class, "addParamValue");
    digester.addCallMethod("workflowGraph/globalSubgraph/paramValue", "setValue", 0);

    return digester;
  }

  private static InputStream substituteProps(Document masterDoc, Map<String, String> properties)
      throws TransformerException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    // transform the DOM doc to a string
    Source source = new DOMSource(masterDoc);
    Result result = new StreamResult(out);
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.transform(source, result);
    String content = new String(out.toByteArray());

    // substitute prop macros
    for (String propName : properties.keySet()) {
      String propValue = properties.get(propName);
      content = content.replaceAll("\\@" + propName + "\\@", Matcher.quoteReplacement(propValue));
    }

    // construct input stream
    return new ByteArrayInputStream(content.getBytes());
  }

}
