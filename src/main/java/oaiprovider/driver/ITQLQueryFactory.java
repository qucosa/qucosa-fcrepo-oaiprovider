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

import oaiprovider.FedoraMetadataFormat;
import oaiprovider.FedoraRecord;
import oaiprovider.InvocationSpec;
import oaiprovider.QueryFactory;
import oaiprovider.ResultCombiner;
import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.Constants;
import org.fcrepo.utilities.DateUtility;
import org.jrdf.graph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trippi.RDFFormat;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import proai.MetadataFormat;
import proai.SetInfo;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("unused") // dynamically loaded via configuration
public class ITQLQueryFactory
        implements QueryFactory, Constants {

    private static final Logger logger = LoggerFactory.getLogger(ITQLQueryFactory.class);

    private static final String QUERY_LANGUAGE = "itql";

    private String m_deleted;

    private FedoraClient m_fedora;
    private FedoraClient m_queryClient;

    private String m_itemSetSpecPath;
    private String m_oaiItemID;
    private String m_setSpec;
    private String m_setSpecName;

    public ITQLQueryFactory() {
    }

    public void init(FedoraClient client,
                     FedoraClient queryClient,
                     Properties props) {
        m_fedora = client;
        m_queryClient = queryClient;
        m_oaiItemID =
                FedoraOAIDriver.getRequired(props, FedoraOAIDriver.PROP_ITEMID);

        m_setSpec =
                FedoraOAIDriver
                        .getOptional(props, FedoraOAIDriver.PROP_SETSPEC);
        if (!m_setSpec.equals("")) {
            m_setSpecName =
                    FedoraOAIDriver
                            .getRequired(props,
                                    FedoraOAIDriver.PROP_SETSPEC_NAME);
            m_itemSetSpecPath =
                    parseItemSetSpecPath(FedoraOAIDriver
                            .getRequired(props,
                                    FedoraOAIDriver.PROP_ITEM_SETSPEC_PATH));
        }

        m_deleted =
                FedoraOAIDriver
                        .getOptional(props, FedoraOAIDriver.PROP_DELETED);
    }

    /**
     * Return latest modification date of any OAI relevant record.
     *
     * If no records match, the current date is returned as a fallback.
     *
     * @param formats Ignored. All items are queried.
     * @return Latest modification date according to Fedora.
     */
    public Date latestRecordDate(Iterator<? extends MetadataFormat> formats) throws RepositoryException {

        String query =
                "select $item $modified from <#ri>\n" +
                        "where $item <http://www.openarchives.org/OAI/2.0/itemID> $itemID\n" +
                        "and $item <fedora-view:lastModifiedDate> $modified\n" +
                        "order by $modified desc\n" +
                        "limit 1";
        TupleIterator it = getTuples(query);

        try {
            if (it.hasNext()) {
                Map<String, Node> t = it.next();
                String item = t.get("item").stringValue();
                String modified = t.get("modified").stringValue();
                return DateUtility.convertStringToDate(modified);
            }
        } catch (TrippiException e) {
            logger.error("Failed to query for latest modification date", e);
        }

        logger.warn("No OAI relevant elements for obtaining the latest modification date. Fallback to current host time.");
        return new Date();
    }

    public RemoteIterator<SetInfo> listSetInfo(InvocationSpec setInfoSpec) {
        if (m_itemSetSpecPath == null) {
            // return empty iterator if sets not configured
            return new FedoraSetInfoIterator();
        } else {
            TupleIterator tuples = getTuples(getListSetInfoQuery(setInfoSpec));
            return new FedoraSetInfoIterator(m_fedora, tuples);
        }
    }

    /**
     * Convert the given Date to a String while also adding or subtracting a
     * millisecond. The shift is necessary because the provided dates are
     * inclusive, whereas ITQL date operators are exclusive.
     */
    private String getExclusiveDateString(Date date, boolean isUntilDate) {
        if (date == null) {
            return null;
        } else {
            long time = date.getTime();
            if (isUntilDate) {
                time++; // add 1ms to make "until" -> "before"
            } else {
                time--; // sub 1ms to make "from" -> "after"
            }
            return DateUtility.convertDateToString(new Date(time));
        }
    }

    public RemoteIterator<FedoraRecord> listRecords(Date from,
                                                    Date until,
                                                    FedoraMetadataFormat format) {

        // Construct and get results of one to three queries, depending on conf

        // Parse and convert the dates once; they may be used more than once
        String afterUTC = getExclusiveDateString(from, false);
        String beforeUTC = getExclusiveDateString(until, true);

        // do primary query
        String primaryQuery =
                getListRecordsPrimaryQuery(afterUTC, beforeUTC, format
                        .getMetadataSpec());
        File primaryFile = getCSVResults(primaryQuery);

        // do set membership query, if applicable
        File setFile = null;
        if (m_itemSetSpecPath != null && m_itemSetSpecPath.length() > 0) { // need
            // set
            // membership
            // info
            String setQuery =
                    getListRecordsSetMembershipQuery(afterUTC,
                            beforeUTC,
                            format.getMetadataSpec());
            setFile = getCSVResults(setQuery);
        }

        // do about query, if applicable
        File aboutFile = null;
        if (format.getAboutSpec() != null) { // need
            // about
            // info
            String aboutQuery =
                    getListRecordsAboutQuery(afterUTC, beforeUTC, format);
            aboutFile = getCSVResults(aboutQuery);
        }

        // Get a FedoraRecordIterator over the combined results
        // that automatically cleans up the result files when closed

        String mdDissType = format.getMetadataSpec().getDisseminationType();
        String aboutDissType = null;

        if (format.getAboutSpec() != null) {
            aboutDissType = format.getAboutSpec().getDisseminationType();
        }

        try {
            ResultCombiner combiner =
                    new ResultCombiner(primaryFile, setFile, aboutFile, true);
            return new CombinerRecordIterator(format.getPrefix(),
                    mdDissType,
                    aboutDissType,
                    combiner);

        } catch (FileNotFoundException e) {
            throw new RepositoryException("Programmer error?  Query result "
                    + "file(s) not found!");
        }
    }

    // FedoraOAIDriver.PROP_DELETED is an optional, object-level (as opposed
    // to dissemination-level) property. If present, use it in place of
    // Fedora state.
    private String getStatePattern() {
        if (m_deleted.equals("")) {
            return "$item     <" + MODEL.STATE + "> $state";
        } else {
            return "$item           <" + m_deleted + "> $state";
        }
    }

    private void appendDateParts(String afterUTC,
                                 String beforeUTC,
                                 boolean alwaysSelectDate,
                                 StringBuilder out) {
        if (afterUTC == null && beforeUTC == null && !alwaysSelectDate) {
            // we don't have to select the date because
            // there are no date constraints and the query doesn't ask for it
            return;
        } else {
            out.append("and    $item     <" + VIEW.LAST_MODIFIED_DATE
                    + "> $date\n");
        }

        // date constraints are optional
        if (afterUTC != null) {
            out.append("and    $date           <" + MULGARA.AFTER + "> '"
                    + afterUTC + "'^^<" + RDF_XSD.DATE_TIME + "> in <#xsd>\n");
        }
        if (beforeUTC != null) {
            out.append("and    $date           <" + MULGARA.BEFORE + "> '"
                    + beforeUTC + "'^^<" + RDF_XSD.DATE_TIME + "> in <#xsd>\n");
        }
    }

    // ordering is required for the combiner to work
    private void appendOrder(StringBuilder out) {
        out.append("order  by $itemID asc");
    }

    // this is common for all listRecords queries
    private void appendCommonFromWhereAnd(StringBuilder out) {
        out.append("from   <#ri>\n");
        out.append("where  $item           <" + m_oaiItemID + "> $itemID\n");
    }

    private String getListRecordsPrimaryQuery(String afterUTC,
                                              String beforeUTC,
                                              InvocationSpec mdSpec) {
        StringBuilder out = new StringBuilder();

        String selectString;
        String contentDissString;

        selectString = "select $item $itemID $date $state\n";

        if (mdSpec.isDatastreamInvocation()) {
            contentDissString = getDatastreamDissType(mdSpec, "$item", "");
        } else {
            contentDissString = getServiceDissType(mdSpec, "$item", "");
        }

        out.append(selectString);
        appendCommonFromWhereAnd(out);
        out.append("and    " + getStatePattern() + "\n");

        out.append("and " + contentDissString);

        appendDateParts(afterUTC, beforeUTC, true, out);
        appendOrder(out);

        return out.toString();
    }

    private String getListRecordsSetMembershipQuery(String afterUTC,
                                                    String beforeUTC,
                                                    InvocationSpec mdSpec) {
        StringBuilder out = new StringBuilder();

        out.append("select $itemID $setSpec\n");
        appendCommonFromWhereAnd(out);

        if (mdSpec.isDatastreamInvocation()) {
            out.append("and " + getDatastreamDissType(mdSpec, "$item", ""));
        } else {
            out.append("and " + getServiceDissType(mdSpec, "$item", ""));
        }
        appendDateParts(afterUTC, beforeUTC, true, out);
        out.append("and    " + m_itemSetSpecPath + "\n");
        appendOrder(out);

        return out.toString();
    }

    private String getListRecordsAboutQuery(String afterUTC,
                                            String beforeUTC,
                                            FedoraMetadataFormat format) {
        StringBuilder out = new StringBuilder();

        InvocationSpec mdSpec = format.getMetadataSpec();
        InvocationSpec aboutSpec = format.getAboutSpec();

        out.append("select $itemID\n");

        appendCommonFromWhereAnd(out);
        if (mdSpec.isDatastreamInvocation()) {
            out.append("and " + getDatastreamDissType(mdSpec, "$item", "_md"));
        } else {
            out.append("and " + getServiceDissType(mdSpec, "$item", "_md"));
        }
        appendDateParts(afterUTC, beforeUTC, true, out);
        if (aboutSpec.isDatastreamInvocation()) {
            out.append("and "
                    + getDatastreamDissType(aboutSpec, "$item", "_about"));
        } else {
            out.append("and "
                    + getServiceDissType(aboutSpec, "$item", "_about"));
        }
        appendOrder(out);

        return out.toString();
    }

    /**
     * Get the results of the given itql tuple query as a temporary CSV file.
     */
    private File getCSVResults(String queryText) throws RepositoryException {

        logger.debug("getCSVResults() called with query:\n" + queryText);

        Map<String, String> parameters = new HashMap<>();
        parameters.put("lang", QUERY_LANGUAGE);
        parameters.put("query", queryText);

        File tempFile;
        OutputStream out;
        try {
            tempFile =
                    File.createTempFile("oaiprovider-listrec-tuples", ".csv");
            tempFile.deleteOnExit(); // just in case
            out = new FileOutputStream(tempFile);
        } catch (IOException e) {
            throw new RepositoryException("Error creating temp query result file",
                    e);
        }

        try {
            TupleIterator tuples = m_queryClient.getTuples(parameters);
            logger.debug("Saving query results to disk...");
            tuples.toStream(out, RDFFormat.CSV);
            logger.debug("Done saving query results");
            return tempFile;
        } catch (Exception e) {
            tempFile.delete();
            throw new RepositoryException("Error getting tuples from Fedora: "
                    + e.getMessage(), e);
        } finally {
            try {
                out.close();
            } catch (Exception ignored) {
            }
        }
    }

    private String getServiceDissType(InvocationSpec spec,
                                      String objectVar,
                                      String suffix) {

        StringBuilder s = new StringBuilder();
        String model = "$model" + suffix;
        String sDef = "$SDef" + suffix;
        s.append(objectVar + " <" + MODEL.HAS_MODEL + "> " + model
                + "\n");
        s.append("and " + model + " <" + MODEL.HAS_SERVICE + "> " + sDef + "\n");
        s.append("and " + sDef + " <" + MODEL.DEFINES_METHOD + "> '"
                + spec.method() + "'\n");
        if (spec.service() != null) {
            s.append(" and " + sDef + " <" + MULGARA.IS + "> <"
                    + spec.service().toURI() + ">\n");
        }
        return s.toString();
    }

    private String getDatastreamDissType(InvocationSpec spec,
                                         String objectVar,
                                         String suffix) {
        return String.format("%s\n and %s\n",
                String.format("%s <%s> $diss%s", objectVar, VIEW.DISSEMINATES, suffix),
                String.format("$diss%s <%s> <%s>", suffix, VIEW.DISSEMINATION_TYPE, spec.getDisseminationType()));
    }

    private String getListSetInfoQuery(InvocationSpec setInfoSpec) {
        StringBuilder query = new StringBuilder();

        String setInfoDissQuery =
                "      $setDiss <test:noMatch> <test:noMatch>\n";
        String target = "$setDiss";
        String dissType = "";
        String commonWhereClause =
                "where $set <" + m_setSpec + "> $setSpec\n" + "and $set <"
                        + m_setSpecName + "> $setName\n";

        if (setInfoSpec != null) {

            dissType = setInfoSpec.getDisseminationType();
            if (setInfoSpec.isDatastreamInvocation()) {
                setInfoDissQuery =
                        getDatastreamDissType(setInfoSpec, "$set", "");
                target = "$diss";
            } else {
                setInfoDissQuery = getServiceDissType(setInfoSpec, "$set", "");
                target = "$SDef";
            }
        }

        query.append("select $set $setSpec $setName '" + dissType + "'\n"
                + "  subquery(" + "    select " + target + "\n	  from <#ri>\n");
        query.append(commonWhereClause);
        query.append("and " + setInfoDissQuery);
        query.append(")\n");
        query.append("from <#ri>" + commonWhereClause);
        return query.toString();
    }

    private TupleIterator getTuples(String query) throws RepositoryException {
        logger.debug("getTuples() called with query:\n" + query);
        Map<String, String> parameters = new HashMap<>();
        parameters.put("lang", QUERY_LANGUAGE);
        parameters.put("query", query);
        parameters.put("stream", "true"); // stream immediately from server

        try {
            return m_queryClient.getTuples(parameters);
        } catch (IOException e) {
            throw new RepositoryException("Error getting tuples from Fedora: "
                    + e.getMessage(), e);
        }
    }

    /**
     * @param itemSetSpecPath
     * @return the setSpec, in the form "$item <$predicate> $setSpec"
     * @throws RepositoryException
     */
    private String parseItemSetSpecPath(String itemSetSpecPath)
            throws RepositoryException {
        String msg = "Required property, itemSetSpecPath, ";
        String[] path = itemSetSpecPath.split("\\s+");
        if (!itemSetSpecPath.contains("$item")) {
            throw new RepositoryException(msg + "must include \"$item\"");
        }
        if (!itemSetSpecPath.contains("$setSpec")) {
            throw new RepositoryException(msg + "must include \"$setSpec\"");
        }
        if (!itemSetSpecPath.matches("(\\$\\w+\\s+<\\S+>\\s+\\$\\w+\\s*)+")) {
            throw new RepositoryException(msg
                    + "must be of the form $item <predicate> $setSpec");
        }
        if (path.length == 3 && path[1].equals(m_setSpec)) {
            throw new RepositoryException(msg
                    + "may not use the same predicate as defined in setSpec");
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length; i++) {
            if (i != 0) {
                sb.append(" ");
                if (i % 3 == 0) {
                    sb.append("and ");
                }
            }
            sb.append(path[i]);
            if (path[i].startsWith("$")
                    && !(path[i].equals("$item") || path[i].equals("$set") || path[i]
                    .equals("$setSpec"))) {
                sb.append(path[i].hashCode());
            }
        }
        return (sb.toString());
    }

}
