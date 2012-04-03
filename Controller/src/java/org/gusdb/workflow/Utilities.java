package org.gusdb.workflow;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import org.gusdb.workflow.xml.WorkflowNode;
import org.gusdb.workflow.xml.WorkflowXmlContainer;

public class Utilities {

    private final static String NL = System.getProperty("line.separator");

    private static Properties gusProps; // from gus.config (db stuff)

    static void runCmd(String cmd) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(cmd);
        process.waitFor();
        if (process.exitValue() != 0)
            error("Failed with status $status running: " + NL + cmd);
        process.destroy();
    }

    static void error(String msg) {
        System.err.println(msg);
        System.exit(1);
    }

    public static String substituteVariablesIntoString(String string, Map<String, String> variables,
						       String where, boolean check, String type, String name) {
        if (string.indexOf("$$") == -1) return string;
        String newString = string;
        for (String variableName : variables.keySet()) {
            String variableValue = variables.get(variableName);
            newString = newString.replaceAll(
                    "\\$\\$" + variableName + "\\$\\$",
                    Matcher.quoteReplacement(variableValue));
        }
	if (check) {
	    String nm = name != null? (" '" + name + "'") : "";
	    if (newString.indexOf("$$") != -1)
		Utilities.error(type + nm 
				+ " in " + where
				+ " includes an unresolvable variable reference: '"
				+ newString + "'");
	}

        return newString;
    }

    public static String substituteMacrosIntoString(String string,
            Map<String, String> macros) {
        if (string.indexOf("@@") == -1) return string;
        String newString = string;
        for (String variableName : macros.keySet()) {
            String variableValue = macros.get(variableName);
            newString = newString.replaceAll(
                    "\\@\\@" + variableName + "\\@\\@",
                    Matcher.quoteReplacement(variableValue));
        }
        return newString;
    }

    @SuppressWarnings("unchecked")
    public static <T extends WorkflowNode, S extends WorkflowXmlContainer<T>> Class<S> getXmlContainerClass(
            Class<T> nodeClass, Class<S> containerClass)
            throws InstantiationException, IllegalAccessException {
        S instance = containerClass.newInstance();
        return (Class<S>) instance.getClass();
    }

    public static void setDatabase(String dbString)
            throws FileNotFoundException, IOException {
        initializeGusProps();
        
        if (dbString == null || dbString.length() == 0) return;
        
        String dbName = null, login = null, password = null;
        int pos = dbString.indexOf('@');
        if (pos >= 0) {
            dbName = dbString.substring(pos + 1);
            login = dbString.substring(0, pos);
            pos = login.indexOf('/');
            if (pos >= 0) {
                password = login.substring(pos + 1);
                login = login.substring(0, pos);
            }
        } else dbName = dbString;

        gusProps.setProperty("dbiDsn", "dbi:Oracle:" + dbName);
        gusProps.setProperty("jdbcDsn", "jdbc:oracle:oci:@" + dbName);
        if (login != null) gusProps.setProperty("databaseLogin", login);
        if (password != null)
            gusProps.setProperty("databasePassword", password);
    }

    public static String getGusConfig(String key) throws FileNotFoundException,
            IOException {
        initializeGusProps();
        String gusHome = System.getProperty("GUS_HOME");
        String configFileName = gusHome + "/config/gus.config";
        String value = gusProps.getProperty(key);
        if (value == null)
            error("Required property " + key
                    + " not found in gus.config file: " + configFileName);
        return value;
    }

    private static void initializeGusProps() throws FileNotFoundException,
            IOException {
        if (gusProps != null) return;

        String gusHome = System.getProperty("GUS_HOME");
        String configFileName = gusHome + "/config/gus.config";
        gusProps = new Properties();
        gusProps.load(new FileInputStream(configFileName));
    }
}
