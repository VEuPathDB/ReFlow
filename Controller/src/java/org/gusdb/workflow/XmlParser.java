/**
 * 
 */
package org.gusdb.workflow;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.digester.Digester;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.thaiopensource.util.PropertyMap;
import com.thaiopensource.util.SinglePropertyMap;
import com.thaiopensource.validate.ValidateProperty;
import com.thaiopensource.validate.ValidationDriver;
import com.thaiopensource.xml.sax.ErrorHandlerImpl;

/**
 * @author Jerric
 * 
 */
public abstract class XmlParser {
    
    protected String gusHome;
    protected String schemaPath;
    
    protected ValidationDriver validator;
    protected Digester digester;
    
    public XmlParser(String schemaPath) {
        this.gusHome = System.getProperty("GUS_HOME");
        this.schemaPath = schemaPath;
    }
     
    protected void configure() throws SAXException, IOException {
        // get model schema file and xml schema file
        URL schemaURL = makeURL(gusHome + "/" + schemaPath );
        
       // config validator and digester
        validator = configureValidator( schemaURL );
        digester = configureDigester();
    }
    
    private ValidationDriver configureValidator( URL schemaURL )
            throws SAXException, IOException {
        System.setProperty(
                "org.apache.xerces.xni.parser.XMLParserConfiguration",
                "org.apache.xerces.parsers.XIncludeParserConfiguration" );
        
        ErrorHandler errorHandler = new ErrorHandlerImpl( System.err );
        PropertyMap schemaProperties = new SinglePropertyMap(
                ValidateProperty.ERROR_HANDLER, errorHandler );
        ValidationDriver validator = new ValidationDriver( schemaProperties,
                PropertyMap.EMPTY, null );
        validator.loadSchema( ValidationDriver.uriOrFileInputSource( schemaURL.toExternalForm() ) );
        return validator;
    }
    
    protected URL makeURL(String fullPath)
            throws MalformedURLException {
        String url = fullPath;
        String lower = url.toLowerCase();
        if ( lower.startsWith( "file:/" ) || lower.startsWith( "http://" )
                || lower.startsWith( "https://" )
                || lower.startsWith( "ftp://" ) || lower.startsWith( "ftps://" ) ) {
            return new URL( url );
        } else {
            File file = new File( url );
            return file.toURI().toURL();
        }
    }
    
    protected boolean validate( URL modelXmlURL ) throws SAXException,
            IOException {
        // System.out.println("Validating model " + modelXmlURL);
        try {
            InputSource is = ValidationDriver.uriOrFileInputSource( modelXmlURL.toExternalForm() );
            boolean result = validator.validate( is );
            if ( !result )
                System.err.println( "Validation failed: "
                        + modelXmlURL.toExternalForm() );
            return result;
        } catch ( SAXException ex ) {
            System.err.println( "Cannot validate: "
                    + modelXmlURL.toExternalForm() );
            throw ex;
        } catch ( IOException ex ) {
            System.err.println( "Cannot validate: "
                    + modelXmlURL.toExternalForm() );
            throw ex;
        }
    }
    
    protected Document buildDocument( URL modelXmlURL ) throws SAXException,
            IOException, ParserConfigurationException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        // turn off validation here, since we don't use DTD; validation is done
        // before this point
        factory.setValidating( false );
        factory.setNamespaceAware( false );
        DocumentBuilder builder;
        builder = factory.newDocumentBuilder();
        
        // ErrorHandler errorHandler = new ErrorHandlerImpl(System.err);
        // builder.setErrorHandler(errorHandler);
        builder.setErrorHandler( new org.xml.sax.ErrorHandler() {
            
            // ignore fatal errors (an exception is guaranteed)
            public void fatalError( SAXParseException exception )
                    throws SAXException {
                exception.printStackTrace( System.err );
            }
            
            // treat validation errors as fatal
            public void error( SAXParseException e ) throws SAXParseException {
                e.printStackTrace( System.err );
                throw e;
            }
            
            // dump warnings too
            public void warning( SAXParseException err )
                    throws SAXParseException {
                System.err.println( "** Warning" + ", line "
                        + err.getLineNumber() + ", uri " + err.getSystemId() );
                System.err.println( "   " + err.getMessage() );
            }
        } );
        
        Document doc = builder.parse( modelXmlURL.openStream() );
        return doc;
    }
    
    protected void configureNode( Digester digester, String path,
            Class<?> nodeClass, String method ) {
        digester.addObjectCreate( path, nodeClass );
        digester.addSetProperties( path );
        digester.addSetNext( path, method );
    }
    
    protected abstract Digester configureDigester();
}
