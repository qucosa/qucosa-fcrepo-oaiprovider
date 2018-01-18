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

import static javax.xml.transform.OutputKeys.METHOD;
import static javax.xml.transform.OutputKeys.OMIT_XML_DECLARATION;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import oaiprovider.mappings.DissTerms.Term;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.http.HttpInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import de.qucosa.xmlutils.SimpleNamespaceContext;
import oaiprovider.FedoraMetadataFormat;
import oaiprovider.FedoraRecord;
import oaiprovider.InvocationSpec;
import oaiprovider.QueryFactory;
import oaiprovider.mappings.ListSetConfJson;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.driver.daos.json.DissTermsDaoJson;
import proai.driver.daos.json.SetSpecDaoJson;
import proai.driver.impl.RemoteIteratorImpl;
import proai.error.BadArgumentException;
import proai.error.RepositoryException;

/**
 * Implementation of the OAIDriver interface for Fedora.
 *
 * @author Edwin Shin, cwilper@cs.cornell.edu
 */
public class FedoraOAIDriver implements OAIDriver {

    private static final Logger logger = LoggerFactory.getLogger(FedoraOAIDriver.class);
    private static final String NS = "driver.fedora.";
    private static final String PROP_BASEURL = NS + "baseURL";
    private static final String PROP_USER = NS + "user";
    private static final String PROP_PASS = NS + "pass";
    private static final String PROP_IDENTIFY = NS + "identify";
    private static final String PROP_SETSPEC_DESC_DISSTYPE = NS + "setSpec.desc.dissType";
    private static final String PROP_QUERY_FACTORY = NS + "queryFactory";
    private static final String PROP_FORMATS = NS + "md.formats";
    private static final String PROP_FORMAT_START = NS + "md.format.";
    private static final String PROP_FORMAT_PFX_END = ".mdPrefix";
    private static final String PROP_FORMAT_LOC_END = ".loc";
    private static final String PROP_FORMAT_URI_END = ".uri";
    private static final String PROP_FORMAT_DISSTYPE_END = ".dissType";
    private static final String PROP_FORMAT_ABOUT_END = ".about.dissType";

    public static final String PROP_ITEMID = NS + "itemID";
    public static final String PROP_SETSPEC = NS + "setSpec";
    public static final String PROP_SETSPEC_NAME = NS + "setSpec.name";
    public static final String PROP_DELETED = NS + "deleted";
    public static final String PROP_ITEM_SETSPEC_PATH = NS + "itemSetSpecPath";

    private static final String PROP_DISS_TERMS_DAO_JSON = "dissTermsData";
    private static final String PROP_SETSPEC_DAO_JSON = "dynSetSpecs";

    private FedoraClient m_fedora;
    private URL m_identify;
    private Map<String, FedoraMetadataFormat> m_metadataFormats;
    private QueryFactory m_queryFactory;
    private InvocationSpec m_setSpecDiss;
    private Properties props;

    private static final ThreadLocal<DocumentBuilder> threadLocalDocumentBuilder;
    private static final ThreadLocal<Transformer> threadLocalTransformer;

    static {
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);

        threadLocalDocumentBuilder = new ThreadLocal<DocumentBuilder>() {
            @Override
            protected DocumentBuilder initialValue() {
                try {
                    return documentBuilderFactory.newDocumentBuilder();
                } catch (ParserConfigurationException e) {
                    throw new RuntimeException("Wrapped initialization exception", e);
                }
            }
        };

        threadLocalTransformer = new ThreadLocal<Transformer>() {
            @Override
            protected Transformer initialValue() {
                try {
                    Transformer transformer = TransformerFactory.newInstance().newTransformer();
                    transformer.setOutputProperty(OMIT_XML_DECLARATION, "yes");
                    transformer.setOutputProperty(METHOD, "xml");
                    return transformer;
                } catch (TransformerConfigurationException e) {
                    throw new RuntimeException("Wrapped initialization exception", e);
                }
            }
        };
    }

    //////////////////////////////////////////////////////////////////////////
    ///////////////////// Methods from proai.driver.OAIDriver ////////////////
    //////////////////////////////////////////////////////////////////////////

    private void writeRecordHeader(String itemID, boolean deleted, String date, List<String> setSpecs, PrintWriter out) {

        out.println(deleted ? "  <header status=\"deleted\">" : "  <header>");
        out.println("    <identifier>" + itemID + "</identifier>");
        out.println("    <datestamp>" + date + "</datestamp>");

        for (String setSpec : setSpecs) {
            out.println("    <setSpec>" + setSpec + "</setSpec>");
        }

        out.println("  </header>");
    }

    private List<String> getDynamicSetSpecs(String mdPrefix, Document document) {
        List<String> result = new ArrayList<>();

        XPath xPath = XPathFactory.newInstance().newXPath();
        xPath.setNamespaceContext(new SimpleNamespaceContext(dissTermsData().getMapXmlNamespaces()));

        for (ListSetConfJson.Set set : ((SetSpecDaoJson) props.get(PROP_SETSPEC_DAO_JSON)).getSetObjects()) {

            String setSpec = set.getSetSpec();
            if (assertNotNullNotEmpty(setSpec)) {
                continue;
            }

            String setPredicate = set.getPredicate();
            if (assertNotNullNotEmpty(setPredicate)) {
                continue;
            }

            String predicateName;
            String predicateValue;

            if (setPredicate.contains("=")) {
                String[] split = setPredicate.split("=");
                predicateName = split[0];
                predicateValue = (split.length > 1) ? split[1] : "";
            } else {
                predicateName = setPredicate;
                predicateValue = null;
            }

            Term term = dissTermsData().getTerm(predicateName, mdPrefix);

            if (term == null) {
                continue;
            }

            String termExpression = term.getTerm();

            if (termExpression == null || termExpression.isEmpty()) {
                continue;
            }

            if (termMatches(document, xPath, predicateValue, termExpression)) {
                result.add(set.getSetSpec());
            }
        }

        return result;
    }

    private DissTermsDaoJson dissTermsData() {
        return ((DissTermsDaoJson) props.get(PROP_DISS_TERMS_DAO_JSON));
    }

    private boolean assertNotNullNotEmpty(String setName) {
        return setName == null || setName.isEmpty();
    }

    private static boolean termMatches(Document document, XPath xPath, String predicateValue, String termExpression) {
        String xpathTerm;
        if (predicateValue != null && termExpression.contains("$val")) {
            xpathTerm = termExpression.replace("$val", predicateValue);
        } else {
            xpathTerm = termExpression;
        }

        Node node = null;
        try {
            XPathExpression xPathExpression = xPath.compile(xpathTerm);
            node = (Node) xPathExpression.evaluate(document, XPathConstants.NODE);
        } catch (XPathExpressionException e) {
            logger.error(String.format("Cannot evaluate XPath expression '%s': %s", xPath, e.getMessage()));
        }
        return (node != null);
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
        this.props = props;
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
    public Properties getProps() {
        return this.props;
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

        out.println("<record>");

        Document disseminationDocument = getDissemination(dissURI);
        List<String> setSpecs = new ArrayList<>(Arrays.asList(parts).subList(4, parts.length));
        setSpecs.addAll(getDynamicSetSpecs(mdPrefix, disseminationDocument));

        writeRecordHeader(itemID, deleted, date, setSpecs, out);
        writeRecordMetadata(out, serializeXml(disseminationDocument));

        if (!aboutDissURI.equals("null")) {
            writeRecordAbouts(aboutDissURI, out);
        }

        out.println("</record>");
    }

    //////////////////////////////////////////////////////////////////////////
    ////////////////////////////// Helper Methods ////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    private Document getDissemination(String dissURI) {
        try (InputStream in = m_fedora.get(dissURI, true)) {
            return threadLocalDocumentBuilder.get().parse(in);
        } catch (IOException e) {
            throw new RepositoryException("Error obtaining dissemination from " + dissURI, e);
        } catch (SAXException e) {
            throw new RepositoryException("Error parsing dissemination XML", e);
        }
    }

    private String serializeXml(Document document) {
        try {
            StringWriter stringWriter = new StringWriter();
            threadLocalTransformer.get().transform(new DOMSource(document), new StreamResult(stringWriter));
            return stringWriter.toString();
        } catch (TransformerException e) {
            throw new RepositoryException("Error serializing dissemination XML", e);
        }
    }

    private void writeRecordMetadata(PrintWriter out, String xml) {
        out.println("  <metadata>");
        out.println(xml);
        out.println("  </metadata>");
    }

    private void writeRecordAbouts(String aboutDissURI, PrintWriter out) throws RepositoryException {
        String aboutWrapperEnd = "</abouts>";

        String xml = serializeXml(getDissemination(aboutDissURI));

        int i = xml.lastIndexOf(aboutWrapperEnd);
        if (i == -1) {
            throw new RepositoryException("Bad abouts xml: closing " + aboutWrapperEnd + " not found");
        }

        out.print(xml.substring(0, i));
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
