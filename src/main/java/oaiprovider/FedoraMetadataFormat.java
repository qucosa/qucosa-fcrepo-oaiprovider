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

import proai.MetadataFormat;

/**
 * @author Edwin Shin
 */
public class FedoraMetadataFormat
        implements MetadataFormat {

    private final InvocationSpec m_aboutSpec;
    private final InvocationSpec m_mdSpec;
    private final String m_namespaceURI;
    private final String m_prefix;
    private final String m_schemaLocation;

    public FedoraMetadataFormat(String prefix,
                                String namespaceURI,
                                String schemaLocation,
                                InvocationSpec mdSpec,
                                InvocationSpec aboutSpec) {
        m_prefix = prefix;
        m_namespaceURI = namespaceURI;
        m_schemaLocation = schemaLocation;
        m_mdSpec = mdSpec;
        m_aboutSpec = aboutSpec;
    }

    public String getPrefix() {
        return m_prefix;
    }

    public String getNamespaceURI() {
        return m_namespaceURI;
    }

    public String getSchemaLocation() {
        return m_schemaLocation;
    }

    public InvocationSpec getMetadataSpec() {
        return m_mdSpec;
    }

    public InvocationSpec getAboutSpec() {
        return m_aboutSpec;
    }
}
