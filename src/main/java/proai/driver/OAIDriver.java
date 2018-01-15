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

package proai.driver;

import java.io.PrintWriter;
import java.util.Date;
import java.util.Properties;

import proai.MetadataFormat;
import proai.Record;
import proai.SetInfo;
import proai.Writable;
import proai.error.RepositoryException;

/**
 * An interface to a repository.
 * <p/>
 * Note that an OAIDriver *must* implement a public no-arg constructor,
 * and will be initialized via a call to init(Properties).
 *
 * @author cwilper@cs.cornell.edu
 */
public interface OAIDriver extends Writable {

    /**
     * Initialize from properties.
     *
     * @param props the implementation-specific initialization properties.
     * @throws RepositoryException if required properties are missing/bad,
     *                             or initialization failed for any reason.
     */
    void init(Properties props) throws RepositoryException;

    /**
     * Returns application properties
     *
     * @return {@link Properties}
     */
    Properties getProps();

    /**
     * Write information about the repository to the given PrintWriter.
     * <p/>
     * <p/>
     * This will be a well-formed XML chunk beginning with an
     * <code>Identify</code> element, as described in
     * <a href="http://www.openarchives.org/OAI/openarchivesprotocol.html#Identify">section
     * 4.2 of the OAI-PMH 2.0 specification</a>.
     * <p/>
     *
     * @throws RepositoryException if there is a problem reading from the repository.
     */
    @Override
    void write(PrintWriter out) throws RepositoryException;

    /**
     * Get the latest date that something changed in the remote repository.
     * <p/>
     * <p>
     * If this is greater than the previously-aquired latestDate,
     * the formats, setInfos, and identity will be retrieved again,
     * and it will be used as the "until" date for the next record query.
     * </p>
     */
    Date getLatestDate() throws RepositoryException;

    /**
     * Get an iterator over a list of MetadataFormat objects representing
     * all OAI metadata formats currently supported by the repository.
     *
     * @see proai.MetadataFormat
     */
    RemoteIterator<? extends MetadataFormat> listMetadataFormats() throws RepositoryException;

    /**
     * Get an iterator over a list of SetInfo objects representing all
     * OAI sets currently supported by the repository.
     * <p/>
     * <p/>
     * The content will be a well-formed XML chunk beginning with a
     * <code>set</code> element, as described in
     * <a href="http://www.openarchives.org/OAI/openarchivesprotocol.html#ListSets">section
     * 4.6 of the OAI-PMH 2.0 specification</a>.
     * <p/>
     *
     * @see proai.SetInfo
     */
    RemoteIterator<? extends SetInfo> listSetInfo() throws RepositoryException;

    /**
     * Get an iterator of <code>Record</code> objects representing all records
     * in the format indicated by mdPrefix, which have changed in the given date
     * range.
     * <p/>
     * <p><strong>Regarding dates:</strong>
     * <em>If from is not null, the date is greater than (non-inclusive)
     * Until must be specified, and it is less than or equal to (inclusive).</em>
     *
     * @see proai.Record
     */
    RemoteIterator<? extends Record> listRecords(Date from,
                                                 Date until,
                                                 String mdPrefix) throws RepositoryException;

    /**
     * Write the XML of the record whose source info is given.
     * <p/>
     * SourceInfo MUST NOT contain newlines. Otherwise, the format is up to the
     * implementation.
     * <p/>
     * The Record implementation produces these strings, and the OAIDriver
     * implementation should know how to use them to produce the XML.
     * <p/>
     * The record must be a well-formed XML chunk beginning with a
     * <code>record</code> element, as described in
     * <a href="http://www.openarchives.org/OAI/openarchivesprotocol.html#GetRecord">section
     * 4.1 of the OAI-PMH 2.0 specification</a>.
     */
    void writeRecordXML(String itemID,
                        String mdPrefix,
                        String sourceInfo,
                        PrintWriter writer) throws RepositoryException;

}
