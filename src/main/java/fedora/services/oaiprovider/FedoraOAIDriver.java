package fedora.services.oaiprovider;

import org.apache.log4j.Logger;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.http.HttpInputStream;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.driver.impl.RemoteIteratorImpl;
import proai.error.RepositoryException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Implementation of the OAIDriver interface for Fedora.
 *
 * @author Edwin Shin, cwilper@cs.cornell.edu
 */
public class FedoraOAIDriver
        implements OAIDriver {

    public static final String NS = "driver.fedora.";
    public static final String PROP_BASEURL = NS + "baseURL";
    public static final String PROP_USER = NS + "user";
    public static final String PROP_PASS = NS + "pass";
    public static final String PROP_QUERY_CONN_TIMEOUT =
            NS + "queryConnectionTimeout";
    public static final String PROP_QUERY_SOCK_TIMEOUT =
            NS + "querySocketTimeout";
    public static final String PROP_DISS_CONN_TIMEOUT =
            NS + "disseminationConnectionTimeout";
    public static final String PROP_DISS_SOCK_TIMEOUT =
            NS + "disseminationSocketTimeout";
    public static final String PROP_IDENTIFY = NS + "identify";
    public static final String PROP_ITEMID = NS + "itemID";
    public static final String PROP_SETSPEC = NS + "setSpec";
    public static final String PROP_SETSPEC_NAME = NS + "setSpec.name";
    public static final String PROP_SETSPEC_DESC_DISSTYPE =
            NS + "setSpec.desc.dissType";
    public static final String PROP_QUERY_FACTORY = NS + "queryFactory";
    public static final String PROP_FORMATS = NS + "md.formats";
    public static final String PROP_FORMAT_START = NS + "md.format.";
    public static final String PROP_DELETED = NS + "deleted";
    public static final String PROP_FORMAT_PFX_END = ".mdPrefix";
    public static final String PROP_FORMAT_LOC_END = ".loc";
    public static final String PROP_FORMAT_URI_END = ".uri";
    public static final String PROP_FORMAT_DISSTYPE_END = ".dissType";
    public static final String PROP_FORMAT_ABOUT_END = ".about.dissType";
    public static final String PROP_FORMAT_ABOUT = ".about";
    public static final String PROP_ITEM_SETSPEC_PATH = NS + "itemSetSpecPath";
    public static final String PROP_VOLATILE = NS + "volatile";
    private static final String _DC_SCHEMALOCATION =
            "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ "
                    + "http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"";
    private static final String _XSI_URI = "http://www.w3.org/2001/XMLSchema-instance";
    private static final String _XSI_DECLARATION = "xmlns:xsi=\"" + _XSI_URI + "\"";
    private static final Logger logger =
            Logger.getLogger(FedoraOAIDriver.class.getName());
    private FedoraClient m_fedora;
    private String m_fedoraBaseURL;
    private String m_fedoraPass;
    private String m_fedoraUser;
    private URL m_identify;
    private Map<String, FedoraMetadataFormat> m_metadataFormats;
    private QueryFactory m_queryFactory;
    private InvocationSpec m_setSpecDiss;

    public FedoraOAIDriver() {
    }

    //////////////////////////////////////////////////////////////////////////
    ///////////////////// Methods from proai.driver.OAIDriver ////////////////
    //////////////////////////////////////////////////////////////////////////

    private static void writeRecordHeader(String itemID,
                                          boolean deleted,
                                          String date,
                                          List<String> setSpecs,
                                          PrintWriter out) {
        if (deleted) {
            out.println("  <header status=\"deleted\">");
        } else {
            out.println("  <header>");
        }
        out.println("    <identifier>" + itemID + "</identifier>");
        out.println("    <datestamp>" + date + "</datestamp>");
        for (int i = 0; i < setSpecs.size(); i++) {
            out.println("    <setSpec>" + (String) setSpecs.get(i)
                    + "</setSpec>");
        }
        out.println("  </header>");
    }

    protected static String getRequired(Properties props, String key)
            throws RepositoryException {
        String val = props.getProperty(key);
        if (val == null) {
            throw new RepositoryException("Required property is not set: "
                    + key);
        }
        logger.debug("Required property: " + key + " = " + val);
        return val.trim();
    }

    protected static int getRequiredInt(Properties props, String key)
            throws RepositoryException {
        String val = getRequired(props, key);
        try {
            return Integer.parseInt(val);
        } catch (Exception e) {
            throw new RepositoryException("Value of property " + key
                    + " is not an integer: " + val);
        }
    }

    /**
     * @param props
     * @param key
     * @return the value associated with key or the empty String ("")
     */
    protected static String getOptional(Properties props, String key) {
        String val = props.getProperty(key);
        logger.debug(key + " = " + val);
        if (val == null) {
            return "";
        }
        return val.trim();
    }

    public void init(Properties props) throws RepositoryException {

        m_fedoraBaseURL = getRequired(props, PROP_BASEURL);
        if (!m_fedoraBaseURL.endsWith("/")) m_fedoraBaseURL += "/";
        m_fedoraUser = getRequired(props, PROP_USER);
        m_fedoraPass = getRequired(props, PROP_PASS);
        m_metadataFormats = getMetadataFormats(props);

        try {
            m_identify = new URL(getRequired(props, PROP_IDENTIFY));
        } catch (MalformedURLException e) {
            throw new RepositoryException("Identify property is not a valid URL: "
                    + props
                    .getProperty(PROP_IDENTIFY),
                    e);
        }

        String className = getRequired(props, PROP_QUERY_FACTORY);
        try {
            m_fedora =
                    new FedoraClient(m_fedoraBaseURL,
                            m_fedoraUser,
                            m_fedoraPass);
            // FIXME Find a proper way to set up connection timeout
//            m_fedora.TIMEOUT_SECONDS =
//                    getRequiredInt(props, PROP_DISS_CONN_TIMEOUT);
//            m_fedora.SOCKET_TIMEOUT_SECONDS =
//                    getRequiredInt(props, PROP_DISS_SOCK_TIMEOUT);
        } catch (Exception e) {
            throw new RepositoryException("Error parsing baseURL", e);
        }

        try {
            Class<?> queryFactoryClass = Class.forName(className);
            m_queryFactory = (QueryFactory) queryFactoryClass.newInstance();
            FedoraClient queryClient =
                    new FedoraClient(m_fedoraBaseURL,
                            m_fedoraUser,
                            m_fedoraPass);
            // FIXME Find a proper way to set up connection timeout
//            queryClient.TIMEOUT_SECONDS =
//                    getRequiredInt(props, PROP_QUERY_CONN_TIMEOUT);
//            queryClient.SOCKET_TIMEOUT_SECONDS =
//                    getRequiredInt(props, PROP_QUERY_SOCK_TIMEOUT);
            m_queryFactory.init(m_fedora, queryClient, props);
        } catch (Exception e) {
            throw new RepositoryException("Unable to initialize " + className,
                    e);
        }

        m_setSpecDiss =
                InvocationSpec
                        .getInstance(getOptional(props,
                                PROP_SETSPEC_DESC_DISSTYPE));
    }

    public void write(PrintWriter out) throws RepositoryException {
        HttpInputStream in = null;
        try {
            in = m_fedora.get(m_identify.toString(), true);
            writeStream(in, out, m_identify.toString());
        } catch (IOException e) {
            throw new RepositoryException("Error getting identify.xml from "
                    + m_identify.toString(), e);
        } finally {
            if (in != null) try {
                in.close();
            } catch (Exception e) {
            }
        }
    }

    // TODO: date for volatile disseminations?
    public Date getLatestDate() throws RepositoryException {
        return m_queryFactory.latestRecordDate(m_metadataFormats.values()
                .iterator());
    }

    public RemoteIterator<FedoraMetadataFormat> listMetadataFormats()
            throws RepositoryException {
        return new RemoteIteratorImpl<FedoraMetadataFormat>(m_metadataFormats
                .values().iterator());
    }

    public RemoteIterator<SetInfo> listSetInfo() throws RepositoryException {

        return m_queryFactory.listSetInfo(m_setSpecDiss);
    }

    public RemoteIterator<FedoraRecord> listRecords(Date from,
                                                    Date until,
                                                    String mdPrefix)
            throws RepositoryException {
        if (from != null && until != null && from.after(until)) {
            throw new RepositoryException("from date cannot be later than until date.");
        }

        return m_queryFactory.listRecords(from, until, m_metadataFormats
                .get(mdPrefix));
    }

    public void writeRecordXML(String itemID,
                               String mdPrefix,
                               String sourceInfo,
                               PrintWriter out) throws RepositoryException {

        // Parse the sourceInfo string
        String[] parts = sourceInfo.trim().split(" ");
        if (parts.length < 4) {
            throw new RepositoryException("Error parsing sourceInfo (expecting "
                    + "4 or more parts): '" + sourceInfo + "'");
        }
        String dissURI = parts[0];
        String aboutDissURI = parts[1];
        boolean deleted = parts[2].equalsIgnoreCase("true");
        String date = parts[3];
        List<String> setSpecs = new ArrayList<String>();
        for (int i = 4; i < parts.length; i++) {
            setSpecs.add(parts[i]);
        }

        out.println("<record>");
        writeRecordHeader(itemID, deleted, date, setSpecs, out);
        if (!deleted) {
            writeRecordMetadata(dissURI, out);
            if (!aboutDissURI.equals("null")) {
                writeRecordAbouts(aboutDissURI, out);
            }
        } else {
            logger
                    .info("Record was marked deleted: " + itemID + "/"
                            + mdPrefix);
        }
        out.println("</record>");
    }

    //////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Helper Methods ////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    private void writeRecordMetadata(String dissURI, PrintWriter out)
            throws RepositoryException {

        InputStream in = null;
        try {
            in = m_fedora.get(dissURI, true);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }
            String xml =
                    buf.toString().replaceAll("\\s*<\\?xml.*?\\?>\\s*", "");
            if ((dissURI.split("/").length == 3) && (dissURI.endsWith("/DC"))) {
                // If it's a DC datastream dissemination, inject the
                // xsi:schemaLocation attribute if needed
                if (xml.indexOf(_XSI_URI) == -1) {
                    xml = xml.replaceAll("<oai_dc:dc ", "<oai_dc:dc "
                            + _XSI_DECLARATION + " " + _DC_SCHEMALOCATION + " ");
                }
            }
            out.println("  <metadata>");
            out.print(xml);
            out.println("  </metadata>");
        } catch (IOException e) {
            throw new RepositoryException("IO error reading " + dissURI, e);
        } finally {
            if (in != null) try {
                in.close();
            } catch (IOException e) {
            }
        }
    }

    private void writeRecordAbouts(String aboutDissURI, PrintWriter out)
            throws RepositoryException {
        String aboutWrapperStart = "<abouts>";
        String aboutWrapperEnd = "</abouts>";
        InputStream in = null;
        try {
            in = m_fedora.get(aboutDissURI, true);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }
            // strip xml declaration and leading whitespace
            String xml =
                    buf.toString().replaceAll("\\s*<\\?xml.*?\\?>\\s*", "");
            int i = xml.indexOf(aboutWrapperStart);
            if (i == -1)
                throw new RepositoryException("Bad abouts xml: opening "
                        + aboutWrapperStart + " not found");
            xml = xml.substring(i + aboutWrapperStart.length());
            i = xml.lastIndexOf(aboutWrapperEnd);
            if (i == -1)
                throw new RepositoryException("Bad abouts xml: closing "
                        + aboutWrapperEnd + " not found");
            out.print(xml.substring(0, i));
        } catch (IOException e) {
            throw new RepositoryException("IO error reading aboutDiss "
                    + aboutDissURI, e);
        } finally {
            if (in != null) try {
                in.close();
            } catch (IOException e) {
            }
        }
    }

    public void close() throws RepositoryException {
        // TODO Auto-generated method stub

    }

    /**
     * @param props
     */
    private Map<String, FedoraMetadataFormat> getMetadataFormats(Properties props)
            throws RepositoryException {
        String formats[], prefix, namespaceURI, schemaLocation;
        FedoraMetadataFormat mf;
        Map<String, FedoraMetadataFormat> map =
                new HashMap<String, FedoraMetadataFormat>();

        // step through formats, getting appropriate properties for each
        formats = getRequired(props, PROP_FORMATS).split(" ");
        for (int i = 0; i < formats.length; i++) {
            prefix = formats[i];
            namespaceURI =
                    getRequired(props, PROP_FORMAT_START + prefix
                            + PROP_FORMAT_URI_END);
            schemaLocation =
                    getRequired(props, PROP_FORMAT_START + prefix
                            + PROP_FORMAT_LOC_END);

            String otherPrefix =
                    props.getProperty(PROP_FORMAT_START + prefix
                            + PROP_FORMAT_PFX_END);
            if (otherPrefix != null) prefix = otherPrefix;

            String mdDissType =
                    PROP_FORMAT_START + prefix + PROP_FORMAT_DISSTYPE_END;
            String mdAboutDissType =
                    PROP_FORMAT_START + prefix + PROP_FORMAT_ABOUT_END;

            mf =
                    new FedoraMetadataFormat(prefix,
                            namespaceURI,
                            schemaLocation,
                            InvocationSpec
                                    .getInstance(getRequired(props,
                                            mdDissType)),
                            InvocationSpec
                                    .getInstance(getOptional(props,
                                            mdAboutDissType)));
            map.put(prefix, mf);
        }
        return map;
    }

    private void writeStream(InputStream in, PrintWriter out, String source)
            throws RepositoryException {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(in));
            String line = reader.readLine();
            while (line != null) {
                out.println(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RepositoryException("Error reading " + source, e);
        } finally {
            if (reader != null) try {
                reader.close();
            } catch (Exception e) {
            }
        }
    }
}
