/*
 * Copyright 2016 Saxon State and University Library Dresden (SLUB)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oaiprovider.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.http.HttpInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import oaiprovider.FedoraMetadataFormat;
import oaiprovider.FedoraRecord;
import oaiprovider.InvocationSpec;
import oaiprovider.QueryFactory;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.driver.impl.RemoteIteratorImpl;
import proai.driver.impl.SetSpecMerge;
import proai.error.BadArgumentException;
import proai.error.RepositoryException;

/**
 * Implementation of the OAIDriver interface for Fedora.
 *
 * @author Edwin Shin, cwilper@cs.cornell.edu
 */
public class FedoraOAIDriver
        implements OAIDriver {

    public static final String NS = "driver.fedora.";
    public static final String PROP_ITEMID = NS + "itemID";
    public static final String PROP_SETSPEC = NS + "setSpec";
    public static final String PROP_SETSPEC_NAME = NS + "setSpec.name";
    public static final String PROP_DELETED = NS + "deleted";
    public static final String PROP_ITEM_SETSPEC_PATH = NS + "itemSetSpecPath";
    private static final Logger logger = LoggerFactory.getLogger(FedoraOAIDriver.class);
    private static final String PROP_BASEURL = NS + "baseURL";
    private static final String PROP_USER = NS + "user";
    private static final String PROP_PASS = NS + "pass";
    private static final String PROP_IDENTIFY = NS + "identify";
    private static final String PROP_SETSPEC_DESC_DISSTYPE =
            NS + "setSpec.desc.dissType";
    private static final String PROP_QUERY_FACTORY = NS + "queryFactory";
    private static final String PROP_FORMATS = NS + "md.formats";
    private static final String PROP_FORMAT_START = NS + "md.format.";
    private static final String PROP_FORMAT_PFX_END = ".mdPrefix";
    private static final String PROP_FORMAT_LOC_END = ".loc";
    private static final String PROP_FORMAT_URI_END = ".uri";
    private static final String PROP_FORMAT_DISSTYPE_END = ".dissType";
    private static final String PROP_FORMAT_ABOUT_END = ".about.dissType";
    private static final String _DC_SCHEMALOCATION =
            "xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/oai_dc/ "
                    + "http://www.openarchives.org/OAI/2.0/oai_dc.xsd\"";
    private static final String _XSI_URI = "http://www.w3.org/2001/XMLSchema-instance";
    private static final String _XSI_DECLARATION = "xmlns:xsi=\"" + _XSI_URI + "\"";

    private FedoraClient m_fedora;
    private URL m_identify;
    private Map<String, FedoraMetadataFormat> m_metadataFormats;
    private QueryFactory m_queryFactory;
    private InvocationSpec m_setSpecDiss;

    private SetSpecMerge setSpecMerge = new SetSpecMerge();

    public FedoraOAIDriver() {
    }

    //////////////////////////////////////////////////////////////////////////
    ///////////////////// Methods from proai.driver.OAIDriver ////////////////
    //////////////////////////////////////////////////////////////////////////

    private static void writeRecordHeader(String itemID,
                                          boolean deleted,
                                          String date,
                                          List<String> setSpecs,
            PrintWriter out, SetSpecMerge setSpecMerge) {
        if (deleted) {
            out.println("  <header status=\"deleted\">");
        } else {
            out.println("  <header>");
        }
        out.println("    <identifier>" + itemID + "</identifier>");
        out.println("    <datestamp>" + date + "</datestamp>");

        setSpecMerge.setSetSpecs(new HashSet<String>(setSpecs));

        for (String setSpec : setSpecs) {
            out.println("    <setSpec>" + setSpec
                    + "</setSpec>");
        }
        out.println("  </header>");
    }

    static String getRequired(Properties props, String key)
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
    static String getOptional(Properties props, String key) {
        String val = props.getProperty(key);
        logger.debug(key + " = " + val);
        if (val == null) {
            return "";
        }
        return val.trim();
    }

    @Override
    public void init(Properties props) throws RepositoryException {
        m_metadataFormats = getMetadataFormats(props);

        try {
            m_identify = new URL(getRequired(props, PROP_IDENTIFY));
        } catch (MalformedURLException e) {
            throw new RepositoryException(String.format(
                    "Identify property is not a valid URL: %s", props.getProperty(PROP_IDENTIFY)), e);
        }

        String m_fedoraBaseURL = getRequired(props, PROP_BASEURL);
        if (!m_fedoraBaseURL.endsWith("/")) {
            m_fedoraBaseURL += "/";
        }
        String m_fedoraUser = getRequired(props, PROP_USER);
        String m_fedoraPass = getRequired(props, PROP_PASS);

        try {
            m_fedora = new FedoraClient(m_fedoraBaseURL, m_fedoraUser, m_fedoraPass);
        } catch (Exception e) {
            throw new RepositoryException("Error parsing baseURL", e);
        }

        String className = getRequired(props, PROP_QUERY_FACTORY);
        try {
            Class<?> queryFactoryClass = Class.forName(className);
            m_queryFactory = (QueryFactory) queryFactoryClass.newInstance();

            FedoraClient queryClient = new FedoraClient(m_fedoraBaseURL, m_fedoraUser, m_fedoraPass);
            m_queryFactory.init(m_fedora, queryClient, props);
        } catch (Exception e) {
            throw new RepositoryException("Unable to initialize " + className, e);
        }

        m_setSpecDiss = InvocationSpec.getInstance(getOptional(props, PROP_SETSPEC_DESC_DISSTYPE));
    }

    @Override
    public void write(PrintWriter out) throws RepositoryException {
        try (HttpInputStream in = m_fedora.get(m_identify.toString(), true)) {
            writeStream(in, out, m_identify.toString());
        } catch (IOException e) {
            throw new RepositoryException(String.format(
                    "Error getting identify.xml from %s", m_identify.toString()), e);
        }
    }

    // TODO: date for volatile disseminations?
    @Override
    public Date getLatestDate() throws RepositoryException {
        return m_queryFactory.latestRecordDate(m_metadataFormats.values().iterator());
    }

    @Override
    public RemoteIterator<FedoraMetadataFormat> listMetadataFormats()
            throws RepositoryException {
        return new RemoteIteratorImpl<>(m_metadataFormats
                .values().iterator());
    }

    @Override
    public RemoteIterator<SetInfo> listSetInfo() throws RepositoryException {
        return m_queryFactory.listSetInfo(m_setSpecDiss);
    }

    @Override
    public RemoteIterator<FedoraRecord> listRecords(Date from, Date until, String mdPrefix) throws RepositoryException {
        if (from != null && until != null && from.after(until)) {
            throw new BadArgumentException("from date cannot be later than until date.");
        }

        return m_queryFactory.listRecords(from, until, m_metadataFormats.get(mdPrefix));
    }

    @Override
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
        List<String> setSpecs = new ArrayList<>();
        setSpecs.addAll(Arrays.asList(parts).subList(4, parts.length));

        setSpecMerge.setDocXml(getXml(dissURI));

        out.println("<record>");
        writeRecordHeader(itemID, deleted, date, setSpecs, out, setSpecMerge);
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
    private String getXml(String dissURI) {
        InputStream in = null;
        String xml = null;

        try {
            in = m_fedora.get(dissURI, true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();

            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }

            xml = buf.toString().replaceAll("\\s*<\\?xml.*?\\?>\\s*", "");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }

        return xml;
    }

    private void writeRecordMetadata(String dissURI, PrintWriter out)
            throws RepositoryException {
        String docXml = getXml(dissURI);

        if ((dissURI.split("/").length == 3) && (dissURI.endsWith("/DC"))) {
            // If it's a DC datastream dissemination, inject the
            // xsi:schemaLocation attribute if needed
            if (!docXml.contains(_XSI_URI)) {
                docXml = docXml.replaceAll("<oai_dc:dc ",
                        "<oai_dc:dc " + _XSI_DECLARATION + " " + _DC_SCHEMALOCATION + " ");
            }
        }

        out.println("  <metadata>");
        out.print(docXml);
        out.println("  </metadata>");



        // InputStream in = null;
        // try {
        // in = m_fedora.get(dissURI, true);
        // BufferedReader reader =
        // new BufferedReader(new InputStreamReader(in));
        // StringBuffer buf = new StringBuffer();
        // String line = reader.readLine();
        // while (line != null) {
        // buf.append(line + "\n");
        // line = reader.readLine();
        // }
        // String xml =
        // buf.toString().replaceAll("\\s*<\\?xml.*?\\?>\\s*", "");
        // if ((dissURI.split("/").length == 3) && (dissURI.endsWith("/DC"))) {
        // // If it's a DC datastream dissemination, inject the
        // // xsi:schemaLocation attribute if needed
        // if (!xml.contains(_XSI_URI)) {
        // xml = xml.replaceAll("<oai_dc:dc ", "<oai_dc:dc "
        // + _XSI_DECLARATION + " " + _DC_SCHEMALOCATION + " ");
        // }
        // }
        // out.println(" <metadata>");
        // out.print(xml);
        // out.println(" </metadata>");
        // } catch (IOException e) {
        // throw new RepositoryException("IO error reading " + dissURI, e);
        // } finally {
        // if (in != null) try {
        // in.close();
        // } catch (IOException ignored) {
        // }
        // }
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
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * @param props
     */
    private Map<String, FedoraMetadataFormat> getMetadataFormats(Properties props)
            throws RepositoryException {
        String formats[], prefix, namespaceURI, schemaLocation;
        FedoraMetadataFormat mf;
        Map<String, FedoraMetadataFormat> map =
                new HashMap<>();

        // step through formats, getting appropriate properties for each
        formats = getRequired(props, PROP_FORMATS).split(" ");
        for (String format : formats) {
            prefix = format;
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
            } catch (Exception ignored) {
            }
        }
    }
}
