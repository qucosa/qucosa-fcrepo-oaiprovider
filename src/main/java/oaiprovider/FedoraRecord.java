package oaiprovider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.Record;

/**
 * @author Edwin Shin
 * @author cwilper@cs.cornell.edu
 */
public class FedoraRecord
        implements Record {

    private static final Logger logger = LoggerFactory.getLogger(FedoraRecord.class);

    private final String m_itemID;

    private final String m_mdPrefix;

    private final String m_sourceInfo;

    public FedoraRecord(String itemID,
                        String mdPrefix,
                        String recordDiss,
                        String date,
                        boolean deleted,
                        String[] setSpecs,
                        String aboutDiss) {

        m_itemID = itemID;
        m_mdPrefix = mdPrefix;

        StringBuffer buf = new StringBuffer();
        buf.append(recordDiss);
        buf.append(" " + aboutDiss);
        buf.append(" " + deleted);
        buf.append(" " + date);
        for (String setSpec1 : setSpecs) {
            String setSpec = setSpec1.replace(' ', '_');
            buf.append(" " + setSpec);
        }
        m_sourceInfo = buf.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see proai.Record#getItemID()
     */
    public String getItemID() {
        return m_itemID;
    }

    public String getPrefix() {
        return m_mdPrefix;
    }

    public String getSourceInfo() {
        logger.debug("Returning source info line: " + m_sourceInfo);
        return m_sourceInfo;
    }
}
