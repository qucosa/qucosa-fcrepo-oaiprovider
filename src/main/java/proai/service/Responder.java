package proai.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.SetInfo;
import proai.Writable;
import proai.cache.CachedContent;
import proai.cache.RecordCache;
import proai.error.BadArgumentException;
import proai.error.BadResumptionTokenException;
import proai.error.CannotDisseminateFormatException;
import proai.error.IdDoesNotExistException;
import proai.error.NoMetadataFormatsException;
import proai.error.NoRecordsMatchException;
import proai.error.NoSetHierarchyException;
import proai.error.ServerException;

import java.io.Closeable;
import java.util.Date;
import java.util.Properties;

/**
 * Provides transport-neutral responses to OAI-PMH requests.
 * <p/>
 * <p/>
 * A single <code>Responder</code> instance handles multiple concurrent OAI-PMH
 * requests and provides responses without regard to the transport protocol.
 * <p/>
 * <p/>
 * Responses are provided via <code>ResponseData</code> objects that can write
 * their XML to a given PrintWriter. The XML provided does not include an XML
 * declaration or an OAI-PMH response header -- this is the responsibility of
 * higher-level application code.
 * <p/>
 * <p/>
 * At this level, errors are signaled by exceptions. Serializing them for
 * transport is the responsibility of higher-level application code.
 *
 * @author Chris Wilper
 */
public class Responder implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Responder.class);

    private static final String PROP_INCOMPLETESETLISTSIZE = "proai.incompleteSetListSize";
    private static final String PROP_INCOMPLETERECORDLISTSIZE = "proai.incompleteRecordListSize";
    private static final String PROP_INCOMPLETEIDENTIFIERLISTSIZE = "proai.incompleteIdentifierListSize";

    private final RecordCache m_cache;

    private final int m_incompleteIdentifierListSize;
    private final int m_incompleteRecordListSize;
    private final int m_incompleteSetListSize;

    private final SessionManager m_sessionManager;

    public Responder(Properties props) throws ServerException {
        m_cache = new RecordCache(props);
        m_sessionManager = new SessionManager(props);
        m_incompleteIdentifierListSize = nonNegativeValue(props, PROP_INCOMPLETEIDENTIFIERLISTSIZE, true);
        m_incompleteRecordListSize = nonNegativeValue(props, PROP_INCOMPLETERECORDLISTSIZE, true);
        m_incompleteSetListSize = nonNegativeValue(props, PROP_INCOMPLETESETLISTSIZE, true);
    }

    private static String q(String s) {
        if (s == null) {
            return null;
        } else {
            return "\"" + s + "\"";
        }
    }

    /**
     * Throw a <code>BadArgumentException<code> if <code>identifier</code> is
     * <code>null</code> or empty.
     */
    private static void checkIdentifier(String identifier)
            throws BadArgumentException {
        if (identifier == null || identifier.length() == 0) {
            throw new BadArgumentException("identifier must be specified");
        }
    }

    /**
     * Throw a <code>BadArgumentException<code> if <code>metadataPrefix</code>
     * is <code>null</code> or empty.
     */
    private static void checkMetadataPrefix(String metadataPrefix)
            throws BadArgumentException {
        if (metadataPrefix == null || metadataPrefix.length() == 0) {
            throw new BadArgumentException("metadataPrefix must be specified");
        }
    }

    private int nonNegativeValue(Properties props, String name, boolean nonZero)
            throws ServerException {
        String v = props.getProperty(name);
        if (v == null)
            throw new ServerException("Required property missing: " + name);
        try {
            int val = Integer.parseInt(v);
            if (val < 0)
                throw new ServerException("Property value cannot be negative: "
                        + name);
            if (nonZero && val == 0) {
                throw new ServerException("Property value cannot be zero: "
                        + name);
            }
            return val;
        } catch (NumberFormatException e) {
            throw new ServerException("Bad integer '" + v
                    + "' specified for property: " + name);
        }
    }

    /**
     * Get the response for a GetRecord request.
     *
     * @param identifier     the item identifier.
     * @param metadataPrefix the format of the record (oai_dc, etc).
     * @throws BadArgumentException             if either of the required parameters are null.
     * @throws CannotDisseminateFormatException if the value of the metadataPrefix argument is not supported
     *                                          by the item identified by the value of the identifier
     *                                          argument.
     * @throws IdDoesNotExistException          if the value of the identifier argument is unknown or illegal
     *                                          in this repository.
     * @throws ServerException                  if a low-level (non-protocol) error occurred.
     */
    public ResponseData getRecord(String identifier, String metadataPrefix)
            throws
            ServerException {

        if (logger.isDebugEnabled()) {
            logger.debug("Entered getRecord(" + q(identifier) + ", "
                    + q(metadataPrefix) + ")");
        }

        try {

            checkIdentifier(identifier);
            checkMetadataPrefix(metadataPrefix);

            Writable content = m_cache.getRecordContent(identifier,
                    metadataPrefix);

            if (content == null) {
                checkItemExists(identifier);
                throw new CannotDisseminateFormatException(
                );
            } else {
                return new ResponseDataImpl(content);
            }
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Exiting getRecord(" + q(identifier) + ", "
                        + q(metadataPrefix) + ")");
            }
        }
    }

    /**
     * Throw an <code>IdDoesNotExistException<code> if the given item does
     * not exist in the cache.
     */
    private void checkItemExists(String identifier)
            throws ServerException {
        if (!m_cache.itemExists(identifier)) {
            throw new IdDoesNotExistException();
        }
    }

    /**
     * Get the response for an Identify request.
     *
     * @throws ServerException if a low-level (non-protocol) error occurred.
     */
    public ResponseData identify() throws ServerException {

        logger.debug("Entered identify()");

        try {
            return new ResponseDataImpl(
                    m_cache.getIdentifyContent());
        } finally {
            logger.debug("Exiting identify()");
        }
    }

    /**
     * Get the response for a ListIdentifiers request.
     *
     * @param from            optional UTC date specifying a lower bound for datestamp-based
     *                        selective harvesting.
     * @param until           optional UTC date specifying an upper bound for
     *                        datestamp-based selective harvesting.
     * @param metadataPrefix  specifies that headers should be returned only if the metadata
     *                        format matching the supplied metadataPrefix is available (or
     *                        has been deleted).
     * @param set             optional argument with a setSpec value, which specifies set
     *                        criteria for selective harvesting.
     * @param resumptionToken exclusive argument with a value that is the flow control token
     *                        returned by a previous ListIdentifiers request that issued an
     *                        incomplete list.
     * @throws BadArgumentException             if resumptionToken is specified with any other parameters, or
     *                                          if resumptionToken is unspecified and any required parameters
     *                                          are not.
     * @throws BadResumptionTokenException      if the value of the resumptionToken argument is invalid or
     *                                          expired.
     * @throws CannotDisseminateFormatException if the value of the metadataPrefix argument is not supported
     *                                          by the repository.
     * @throws NoRecordsMatchException          if the combination of the values of the from, until, and set
     *                                          arguments results in an empty list.
     * @throws NoSetHierarchyException          if set is specified and the repository does not support sets.
     * @throws ServerException                  if a low-level (non-protocol) error occurred.
     */
    public ResponseData listIdentifiers(String from, String until,
                                        String metadataPrefix, String set, String resumptionToken)
            throws
            ServerException {
        System.out.println("From " + from + " Until " + until);

        if (logger.isDebugEnabled()) {
            logger.debug("Entered listIdentifiers(" + q(from) + ", " + q(until)
                    + ", " + q(metadataPrefix) + ", " + q(set) + ", "
                    + q(resumptionToken) + ")");
        }

        try {
            return listRecords(from, until, metadataPrefix, set,
                    resumptionToken, true, m_incompleteIdentifierListSize);
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Exiting listIdentifiers(" + q(from) + ", "
                        + q(until) + ", " + q(metadataPrefix) + ", " + q(set)
                        + ", " + q(resumptionToken) + ")");
            }
        }
    }

    private ResponseData listRecords(String from, String until,
                                     String metadataPrefix, String set, String resumptionToken,
                                     boolean identifiersOnly, int incompleteListSize)
            throws
            ServerException {
        if (resumptionToken == null) {
            Date fromDate = null;
            Date untilDate = null;
            try {
                DateRange range = DateRange.getRangeInclIncl(from, until);
                untilDate = DateRange.iso8601ToDate(range.until);
                fromDate = DateRange.iso8601ToDate(range.from);
            } catch (DateRangeParseException e) {
                throw new BadArgumentException(e.getLocalizedMessage(), e);
            } catch (Exception e) {
                logger.debug(e.getMessage());
            }
            // checkGranularity(from, until);
            // checkFromUntil(fromDate, untilDate);
            checkMetadataPrefix(metadataPrefix);
            ListProvider<CachedContent> provider = new RecordListProvider(
                    m_cache, incompleteListSize, identifiersOnly, fromDate,
                    untilDate, metadataPrefix, set);
            return m_sessionManager.list(provider);
        } else {
            if (from != null || until != null || metadataPrefix != null
                    || set != null) {
                throw new BadArgumentException("the resumptionToken argument may only be specified by itself");
            }
            return m_sessionManager.getResponseData(resumptionToken);
        }
    }

    /**
     * Get the response for a ListMetadataFormats request.
     *
     * @param identifier an optional argument that specifies the unique identifier of
     *                   the item for which available metadata formats are being
     *                   requested. If this argument is omitted, then the response
     *                   includes all metadata formats supported by this repository.
     * @throws IdDoesNotExistException if the value of the identifier argument is unknown or illegal
     *                                 in this repository.
     * @throws ServerException         if a low-level (non-protocol) error occurred.
     */
    public ResponseData listMetadataFormats(String identifier)
            throws
            ServerException {

        if (logger.isDebugEnabled()) {
            logger.debug("Entered listMetadataFormats(" + q(identifier) + ")");
        }

        try {
            Writable content;
            content = m_cache.getMetadataFormatsContent(identifier);
            if (content == null && identifier != null) {
                checkItemExists(identifier);
                throw new NoMetadataFormatsException();
            }
            return new ResponseDataImpl(content);
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Exiting listMetadataFormats(" + q(identifier)
                        + ")");
            }
        }
    }

    // /////////////////////////////////////////////////////////////////////////

    /**
     * Get the response for a ListRecords request.
     *
     * @param from            optional UTC date specifying a lower bound for datestamp-based
     *                        selective harvesting.
     * @param until           optional UTC date specifying an upper bound for
     *                        datestamp-based selective harvesting.
     * @param metadataPrefix  specifies that records should be returned only if the metadata
     *                        format matching the supplied metadataPrefix is available (or
     *                        has been deleted).
     * @param set             optional argument with a setSpec value, which specifies set
     *                        criteria for selective harvesting.
     * @param resumptionToken exclusive argument with a value that is the flow control token
     *                        returned by a previous ListIdentifiers request that issued an
     *                        incomplete list.
     * @throws BadArgumentException             if resumptionToken is specified with any other parameters, or
     *                                          if resumptionToken is unspecified and any required parameters
     *                                          are not.
     * @throws BadResumptionTokenException      if the value of the resumptionToken argument is invalid or
     *                                          expired.
     * @throws CannotDisseminateFormatException if the value of the metadataPrefix argument is not supported
     *                                          by the repository.
     * @throws NoRecordsMatchException          if the combination of the values of the from, until, and set
     *                                          arguments results in an empty list.
     * @throws NoSetHierarchyException          if set is specified and the repository does not support sets.
     * @throws ServerException                  if a low-level (non-protocol) error occurred.
     */
    public ResponseData listRecords(String from, String until,
                                    String metadataPrefix, String set, String resumptionToken)
            throws
            ServerException {

        if (logger.isDebugEnabled()) {
            logger.debug("Entered listRecords(" + q(from) + ", " + q(until)
                    + ", " + q(metadataPrefix) + ", " + q(set) + ", "
                    + q(resumptionToken) + ")");
        }
        try {
            return listRecords(from, until, metadataPrefix, set,
                    resumptionToken, false, m_incompleteRecordListSize);
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Exiting listRecords(" + q(from) + ", " + q(until)
                        + ", " + q(metadataPrefix) + ", " + q(set) + ", "
                        + q(resumptionToken) + ")");
            }
        }
    }

    /**
     * Get the response for a ListSets request.
     *
     * @param resumptionToken exclusive argument with a value that is the flow control token
     *                        returned by a previous ListSets request that issued an
     *                        incomplete list.
     * @throws BadResumptionTokenException if the value of the resumptionToken argument is invalid or
     *                                     expired.
     * @throws NoSetHierarchyException     if the repository does not support sets.
     * @throws ServerException             if a low-level (non-protocol) error occurred.
     */
    public ResponseData listSets(String resumptionToken)
            throws
            ServerException {

        if (logger.isDebugEnabled()) {
            logger.debug("Entered listSets(" + q(resumptionToken) + ")");
        }
        try {
            if (resumptionToken == null) {
                ListProvider<SetInfo> provider = new SetListProvider(m_cache,
                        m_incompleteSetListSize);
                return m_sessionManager.list(provider);
            } else {
                return m_sessionManager.getResponseData(resumptionToken);
            }
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Exiting listSets(" + q(resumptionToken) + ")");
            }
        }
    }

    /**
     * Release any resources held by the session manager and the cache.
     */
    public void close() throws ServerException {
        m_sessionManager.close();
        m_cache.close();
    }

}



