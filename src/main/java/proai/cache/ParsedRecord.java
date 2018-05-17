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

package proai.cache;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import proai.Record;
import proai.error.ServerException;
import proai.util.StreamUtil;

public class ParsedRecord extends DefaultHandler implements Record {

    private static final Logger log = LoggerFactory.getLogger(ParsedRecord.class);

    private final File m_file;
    private final SimpleDateFormat m_formatter1;
    private final SimpleDateFormat m_formatter2;
    private final String m_itemID;
    private final String m_prefix;
    private final List<String> m_setSpecs;
    private final String m_sourceInfo;
    private StringBuffer m_buf = null;
    private Date m_date;
    private boolean m_finishedParsing;
    private boolean m_inDatestamp;
    private boolean m_inSetSpec;

    public ParsedRecord(String itemID,
                        String prefix,
                        String sourceInfo,
                        File file) throws ServerException {
        m_itemID = itemID;
        m_prefix = prefix;
        m_sourceInfo = sourceInfo;
        m_file = file;
        m_date = new Date(0);
        m_setSpecs = new ArrayList<>();

        m_formatter1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        m_formatter2 = new SimpleDateFormat("yyyy-MM-dd");

        m_inDatestamp = false;
        m_inSetSpec = false;
        m_finishedParsing = false;

        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(false);
            SAXParser parser = spf.newSAXParser();
            parser.parse(new FileInputStream(file), this);
        } catch (Exception e) {
            if (log.isDebugEnabled() && file.exists()) {
                try {
                    String xml = StreamUtil.getString(new FileInputStream(file), "UTF-8");
                    log.debug("Error parsing record xml: #BEGIN-XML#" + xml + "#END-XML#");
                } catch (Exception ignored) {
                }
            }
            throw new ServerException("Error parsing record xml", e);
        }
    }

    @Override
    public void startElement(String uri,
                             String localName,
                             String qName,
                             Attributes a) {
        if (!m_finishedParsing) {
            if (qName.equals("datestamp")) {
                m_inDatestamp = true;
                m_buf = new StringBuffer();
            } else if (qName.equals("setSpec")) {
                m_inSetSpec = true;
                m_buf = new StringBuffer();
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (!m_finishedParsing) {
            switch (qName) {
                case "header":
                    m_finishedParsing = true;
                    break;
                case "datestamp": {
                    String s = m_buf.toString().trim();
                    try {
                        try {
                            m_date = m_formatter1.parse(s);
                        } catch (Exception e) {
                            m_date = m_formatter2.parse(s);
                        }
                    } catch (Exception e) {
                        throw new SAXException("Record datestamp is unparsable: " + s);
                    }
                    m_inDatestamp = false;
                    break;
                }
                case "setSpec": {
                    String s = m_buf.toString().trim();
                    // Infer memberships based on setSpec:syntax:stuff
                    String[] h = s.split(":");
                if (h.length > 2) {
                        StringBuilder b4 = new StringBuilder();
                        for (String aH : h) {
                            m_setSpecs.add(b4.toString() + aH);
                            b4.append(aH + ":");
                        }
                    } else {
                        m_setSpecs.add(m_buf.toString().trim());
                    }
                    m_inSetSpec = false;
                    break;
                }
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (!m_finishedParsing) {
            if (m_inDatestamp || m_inSetSpec) {
                m_buf.append(ch, start, length);
            }
        }
    }

    // From Record interface
    @Override
    public String getItemID() {
        return m_itemID;
    }

    // From Record interface
    @Override
    public String getPrefix() {
        return m_prefix;
    }

    // From Record interface
    // In this case, the sourceInfo is the path to the xml inside
    // the cache, relative to rcDisk's baseDir.
    @Override
    public String getSourceInfo() {
        return m_sourceInfo;
    }

    public Date getDate() {
        return m_date;
    }

    public List<String> getSetSpecs() {
        return m_setSpecs;
    }

    public boolean deleteFile() {
        return m_file.delete();
    }

}
