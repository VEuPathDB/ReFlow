package org.gusdb.workflow;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.regex.Matcher;

public class Utilities {
    
    final static String nl = System.getProperty("line.separator");

    public static void addOption(Options options, String argName, String desc, boolean isRequired) {

        Option option = new Option(argName, true, desc);
        option.setRequired(isRequired);
        option.setArgName(argName);

        options.addOption(option);
    }

    public static CommandLine parseOptions(String cmdlineSyntax, String cmdDescrip, String usageNotes, Options options,
            String[] args) {

        CommandLineParser parser = new BasicParser();
        CommandLine cmdLine = null;
        try {
            // parse the command line arguments
            cmdLine = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("");
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.err.println("");
            usage(cmdlineSyntax, cmdDescrip, usageNotes, options);
        }

        return cmdLine;
    }
    
    public static void usage(String cmdlineSyntax, String cmdDescrip, String usageNotes, Options options) {
        
        String header = nl + cmdDescrip + nl + nl + "Options:";

        // PrintWriter stderr = new PrintWriter(System.err);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(75, cmdlineSyntax, header, options, nl);
	System.out.println(usageNotes);
        System.exit(1);
    }
    
    static void runCmd(String cmd) throws IOException, InterruptedException {
      Process process = Runtime.getRuntime().exec(cmd);
      process.waitFor();
      if (process.exitValue() != 0) 
          error("Failed with status $status running: " + nl + cmd);
      process.destroy();
    }
    
    static void error(String msg) {
        System.err.println(msg);
        System.exit(1);
    }
    
    static public void deleteDir(File dir) {
	if( dir.exists() ) {
	    for (File f : dir.listFiles()) {
		if (f.isDirectory()) deleteDir(f);
		else f.delete();
	    }
	    dir.delete();
	}
    }	
    
    public static String encrypt(String data) throws Exception,
    NoSuchAlgorithmException {
        // cannot encrypt null value
        if (data == null || data.length() == 0)
            throw new Exception("Cannot encrypt an empty/null string");

        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] byteBuffer = digest.digest(data.toString().getBytes());
        // convert each byte into hex format
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < byteBuffer.length; i++) {
            int code = (byteBuffer[i] & 0xFF);
            if (code < 0x10) buffer.append('0');
            buffer.append(Integer.toHexString(code));
        }
        return buffer.toString();
    }

    public static String substituteVariablesIntoString(String string, Map<String,String>variables) {
        if (string.indexOf("$$") == -1) return string;
        String newString = string;
        for (String variableName : variables.keySet()) {
            String variableValue = variables.get(variableName);
            newString =
                newString.replaceAll("\\$\\$" + variableName + "\\$\\$",
                                    Matcher.quoteReplacement(variableValue));
        }
        return newString;
    }
    
    public static String substituteMacrosIntoString(String string, Map<String,String>macros) {
        if (string.indexOf("@@") == -1) return string;
        String newString = string;
        for (String variableName : macros.keySet()) {
            String variableValue = macros.get(variableName);
            newString =
                newString.replaceAll("\\@\\@" + variableName + "\\@\\@",
                                    Matcher.quoteReplacement(variableValue));
        }
        return newString;
    }
}
