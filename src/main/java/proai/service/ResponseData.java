package proai.service;

import proai.Writable;

/**
 * The data part of an OAI response.
 * <p/>
 * This may be complete in itself, as in the case of "Identify",
 * or it may be one part in a series of "incomplete list" responses.
 */
public interface ResponseData extends Writable {

    /**
     * Get the resumption token for the next part if this is one
     * in a series of incomplete list responses (and not the last part).
     */
    String getResumptionToken();

}
