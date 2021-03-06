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

package oaiprovider;

import org.fcrepo.client.FedoraClient;
import proai.MetadataFormat;
import proai.SetInfo;
import proai.driver.RemoteIterator;

import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

/**
 * Interface for language-specific query handlers to the Fedora Resource Index.
 *
 * @author Edwin Shin
 */
public interface QueryFactory {

    void init(FedoraClient client,
              FedoraClient queryClient,
              Properties props);

    /**
     * Queries the Fedora Resource Index for the latest last-modified date of
     * all disseminations that act as metadata for the OAI provider.
     *
     * @param fedoraMetadataFormats the list of all FedoraMetadataFormats
     * @return date of the latest record
     */
    Date latestRecordDate(Iterator<? extends MetadataFormat> fedoraMetadataFormats);

    /**
     * @return a RemoteIterator of proai.SetInfo objects
     */
    RemoteIterator<SetInfo> listSetInfo(InvocationSpec setInfoSpec);

    /**
     * @param from  the date (inclusive) of the earliest record to return. Null
     *              indicates no lower bound.
     * @param until the date (inclusive). Null indicates no upper bound.
     * @return a RemoteIterator of proai.Record objects
     */
    RemoteIterator<FedoraRecord> listRecords(Date from,
                                             Date until,
                                             FedoraMetadataFormat format);
}
