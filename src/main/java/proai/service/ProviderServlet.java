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

package proai.service;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.driver.daos.json.DissTermsDaoJson;
import proai.driver.daos.json.SetSpecDaoJson;
import proai.error.BadArgumentException;
import proai.error.BadVerbException;
import proai.error.ProtocolException;
import proai.error.ServerException;
import proai.util.StreamUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ProviderServlet extends HttpServlet {
    static final long serialVersionUID = 1;

    private static final Logger logger = LoggerFactory.getLogger(ProviderServlet.class);

    /**
     * Every response starts with this string.
     */
    private static final String _PROC_INST = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    private static final String _STYLESHEET_REFERENCE = "<?xml-stylesheet type=\"text/xsl\" href=\"%s/xslt/oai2.xsl\" ?>";
    private static final String _XMLSTART = "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/\n"
            + "                             http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">\n";
    private Responder m_responder;
    private boolean useStylesheet;

    /**
     * Close the Responder at shutdown-time.
     * <p/>
     * This makes a best-effort attempt to properly close any resources
     * (db connections, threads, etc) that are being held.
     */
    @Override
    public void destroy() {
        try {
            m_responder.close();
        } catch (Exception e) {
            logger.warn("Error trying to close Responder", e);
        }
    }

    private static void appendAttribute(String name, String[] values, StringBuffer buf) {
        if (values != null) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                sb.append(value);
                if (i <= values.length - 2) sb.append(", ");
            }
            appendAttribute(name, sb.toString(), buf);
        }
    }

    @Override
    public void init() throws ServletException {
        String s = firstOf(
                System.getProperty("proai.home"),
                getServletContext().getInitParameter("proai.home"));
        if (s == null) {
            throw new ServerException("Could not obtain proai.home directory path.");
        }

        final Path proaiHomePath = Paths.get(s);
        if (proaiHomePath != null) {
            if (!proaiHomePath.isAbsolute() || !proaiHomePath.toFile().exists()) {
                throw new ServletException(
                        String.format("Configured proai.home '%s' is not an existing directory.", proaiHomePath));
            }
        }

        final Path proaiConfigPath = (proaiHomePath != null) ? proaiHomePath.resolve("config") : null;

        configureLogback(proaiConfigPath);


        try {
            final InputStream propertiesStream =
                    getConfigurationResourceAsStream(proaiConfigPath, "application properties", "proai.properties");
            final InputStream disseminationConfiguration =
                    getConfigurationResourceAsStream(proaiConfigPath, "dissemination terms", "dissemination-config.json");
            final InputStream setSpecsConfiguration =
                    getConfigurationResourceAsStream(proaiConfigPath, "list of dynamic sets", "list-set-conf.json");

            Properties props = new Properties();
            props.load(propertiesStream);
            props.put("dissTermsData", new DissTermsDaoJson(disseminationConfiguration));
            props.put("dynSetSpecs", new SetSpecDaoJson(setSpecsConfiguration));

            useStylesheet = Boolean.valueOf(props.getProperty("proai.useStylesheet"));

            m_responder = new Responder(props);
        } catch (Exception e) {
            throw new ServletException("Unable to initialize ProviderServlet", e);
        }
    }

    @Override
    public void doPost(HttpServletRequest request,
                       HttpServletResponse response) {
        doGet(request, response);
    }

    private void configureLogback(Path proaiConfigPath) {
        final Path proaiLogbackConfigPath;
        if (proaiConfigPath != null) {
            proaiLogbackConfigPath = proaiConfigPath.resolve("logback.xml");
            File proaiLogbackConfig = proaiLogbackConfigPath.toFile();
            // assume SLF4J is bound to logback in the current environment
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset();
                configurator.doConfigure(proaiLogbackConfig);
                logger.info(String.format("Using logging configuration from '%s'", proaiLogbackConfig.getAbsolutePath()));
            } catch (JoranException ignored) {
                // StatusPrinter will handle this
            }
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);
        }
    }

    /**
     * Find configuration file in system config folder. Load from defaults from classpath as fallback.
     *
     * @param configPath Path where to look for configuration files on the file system
     * @param label Label for configuration file (e.g. "application properties")
     * @param name Name of the resource to load (file name)
     * @return Ready InputStream to obtain contents of the resource
     * @throws IOException Thrown if the resource cannot be opened
     */
    private InputStream getConfigurationResourceAsStream(Path configPath, String label, String name) throws IOException {
        InputStream resourceStream = null;
        String whereFound = "";

        // Try file system
        if (configPath != null) {
            File resourceFile = configPath.resolve(name).toFile();
            if (resourceFile.exists()) {
                whereFound = "configuration folder: " + resourceFile.getAbsolutePath();
                resourceStream = new FileInputStream(resourceFile);
            }
        }

        // Fallback to classpath
        if (resourceStream == null) {
            String classpathName = "/config/" + name;
            resourceStream = getClass().getResourceAsStream(classpathName);
            if (resourceStream != null) {
                whereFound = "classpath: " + classpathName;
            }
        }

        if (resourceStream == null) {
            throw new IOException("Cannot find configuration in either config folder or classpath: " + name);
        }

        logger.info(String.format("Found %s in %s ", label, whereFound));
        return resourceStream;
    }

    private static void appendAttribute(String name, String value, StringBuffer buf) {
        if (value != null) {
            buf.append(" ").append(name).append("=\"");
            buf.append(StreamUtil.xmlEncode(value));
            buf.append("\"");
        }
    }

    /**
     * Entry point for handling OAI requests.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) {

        if (logger.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder();
            buf.append("Started servicing request ( ");
            Map map = request.getParameterMap();
            for (Object o : map.keySet()) {
                String parmName = (String) o;
                String[] parmVals = (String[]) map.get(parmName);
                buf.append(parmName + "=" + parmVals[0] + " ");
            }
            buf.append(") from " + request.getRemoteAddr());
            logger.debug(buf.toString());
        }

        String url = request.getRequestURL().toString();

        String verb;
        verb = request.getParameter("verb");

        String identifier = request.getParameter("identifier");

        String from = request.getParameter("from");

        String until = request.getParameter("until");

        String metadataPrefix = request.getParameter("metadataPrefix");

        String[] sets;
        sets = request.getParameterValues("set");

        String resumptionToken;
        resumptionToken = request.getParameter("resumptionToken");


        try {
            if (verb == null) throw new BadVerbException("request did not specify a verb");

            // die if any other parameters are given, too
            // this is a bit draconian, but required by the spec nonetheless
            Set argKeys = request.getParameterMap().keySet();
            int argCount = argKeys.size() - 1;
            for (Object argKey : argKeys) {
                String n = (String) argKey;
                if (!n.equals("verb")
                        && !n.equals("identifier")
                        && !n.equals("from")
                        && !n.equals("until")
                        && !n.equals("metadataPrefix")
                        && !n.equals("set")
                        && !n.equals("resumptionToken")) {
                    throw new BadArgumentException("unknown argument: " + n);
                }
            }

            // lower-case if metadata prefix is xMetaDissPlus
            if (metadataPrefix != null && metadataPrefix.equals("xMetaDissPlus")) {
                metadataPrefix = metadataPrefix.toLowerCase();
            }

            ResponseData data;
            switch (verb) {
                case "GetRecord":
                    if (argCount != 2) throw new BadArgumentException("two arguments needed, got " + argCount);
                    data = m_responder.getRecord(identifier, metadataPrefix);
                    break;
                case "Identify":
                    if (argCount != 0) throw new BadArgumentException("zero arguments needed, got " + argCount);
                    data = m_responder.identify();
                    break;
                case "ListIdentifiers":
                    if (identifier != null)
                        throw new BadArgumentException("identifier argument is not valid for this verb");
                    data = m_responder.listIdentifiers(from, until, metadataPrefix, sets, resumptionToken);
                    break;
                case "ListMetadataFormats":
                    if (argCount > 1)
                        throw new BadArgumentException("one or zero arguments needed, got " + argCount);
                    data = m_responder.listMetadataFormats(identifier);
                    break;
                case "ListRecords":
                    if (identifier != null)
                        throw new BadArgumentException("identifier argument is not valid for this verb");
                    data = m_responder.listRecords(from, until, metadataPrefix, sets, resumptionToken);
                    break;
                case "ListSets":
                    if (argCount > 1)
                        throw new BadArgumentException("one or zero arguments needed, got " + argCount);
                    data = m_responder.listSets(resumptionToken);
                    break;
                default:
                    throw new BadVerbException("bad verb: " + verb);
            }
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/xml; charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.print(getResponseStart(url, verb, identifier, from, until, metadataPrefix, sets, resumptionToken, null));
            data.write(response.getWriter());
            writer.println("</OAI-PMH>");
            writer.flush();
            writer.close();
        } catch (ProtocolException e) {
            sendProtocolException(getResponseStart(url,
                    verb,
                    identifier,
                    from,
                    until,
                    metadataPrefix,
                    sets,
                    resumptionToken,
                    e),
                    e, response);
        } catch (ServerException e) {
            try {
                logger.error("OAI Service Error", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "OAI Service Error");
            } catch (IOException ioe) {
                logger.error("Could not send error to client", ioe);
            }
        } catch (Throwable th) {
            try {
                logger.error("Unexpected Error", th);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unexpected error");
            } catch (IOException ioe) {
                logger.error("Could not send error to client", ioe);
            }
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Finished servicing request from " + request.getRemoteAddr());
            }
        }
    }

    private String getResponseStart(String url,
                                    String verb,
                                    String identifier,
                                    String from,
                                    String until,
                                    String metadataPrefix,
                                    String[] sets,
                                    String resumptionToken,
                                    ProtocolException e) { // normally null
        boolean doParams = true;
        if (verb == null) doParams = false;
        if ((e instanceof BadVerbException
                || e instanceof BadArgumentException)) doParams = false;

        StringBuffer buf = new StringBuffer();
        buf.append(_PROC_INST);
        if (useStylesheet) {
            buf.append(String.format(_STYLESHEET_REFERENCE, getServletContext().getContextPath()));
            buf.append("\n");
        }
        buf.append(_XMLSTART);
        buf.append(String.format("  <responseDate>%s</responseDate>\n", StreamUtil.nowUTCString()));
        buf.append("  <request");
        if (doParams) {
            appendAttribute("verb", verb, buf);
            appendAttribute("identifier", identifier, buf);
            appendAttribute("from", from, buf);
            appendAttribute("until", until, buf);
            appendAttribute("metadataPrefix", metadataPrefix, buf);
            appendAttribute("set", sets, buf);
            appendAttribute("resumptionToken", resumptionToken, buf);
        }
        buf.append(">").append(url).append("</request>\n");
        return buf.toString();
    }

    private void sendProtocolException(String responseStart,
                                       ProtocolException e,
                                       HttpServletResponse response) {
        try {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/xml; charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.print(responseStart);
            writer.print("  <error code=\"" + e.getCode() + "\">");
            if (e.getMessage() != null) writer.print(StreamUtil.xmlEncode(e.getMessage()));
            writer.println("</error>");
            writer.println("</OAI-PMH>");
            writer.flush();
            writer.close();
        } catch (Throwable th) {
            logger.warn("Error while sending a protocol exception (" + e.getClass().getName() + ") response", th);
        }
    }

    private static String firstOf(String... strings) {
        for (String s : strings) {
            if (!orEmptyString(s).isEmpty()) return s;
        }
        return null;
    }

    private static String orEmptyString(String s) {
        return (s != null) ? s : "";
    }

}
