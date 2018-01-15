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

package proai.driver.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import proai.MetadataFormat;
import proai.Record;
import proai.SetInfo;
import proai.driver.OAIDriver;
import proai.driver.RemoteIterator;
import proai.error.RepositoryException;

/**
 * An simple OAIDriver for testing/demonstration purposes.
 * <p/>
 * The directory should contain the following files:
 * <p/>
 * identity.xml
 * records/
 * item1-oai_dc-2005-01-01T08-50-44.xml
 * sets/
 * abovetwo.xml
 * abovetwo-even.xml
 * abovetwo-odd.xml
 * prime.xml
 * formats/
 * oai_dc.txt
 * line1: ns
 * line2: loc
 */
public class OAIDriverImpl implements OAIDriver {

    private static final String BASE_DIR_PROPERTY = "proai.driver.simple.baseDir";

    private static final String IDENTITY_FILENAME = "identity.xml";
    private static final String RECORDS_DIRNAME = "records";
    private static final String SETS_DIRNAME = "sets";
    private static final String FORMATS_DIRNAME = "formats";
    private File m_formatsDir;
    private File m_identityFile;
    private File m_recordsDir;
    private File m_setsDir;
    private Properties props;

    public OAIDriverImpl() {
    }

    public OAIDriverImpl(File dir) throws RepositoryException {
        Properties props = new Properties();
        props.setProperty(BASE_DIR_PROPERTY, dir.getPath());
        init(props);
    }

    public static void writeFromFile(File file,
                                     PrintWriter out) throws RepositoryException {
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(file), "UTF-8"));
            String line = reader.readLine();
            while (line != null) {
                out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            throw new RepositoryException("Error reading from file: " + file.getPath(), e);
        }
    }

    @Override
    public void init(Properties props) throws RepositoryException {
        this.props = props;
        String baseDir = props.getProperty(BASE_DIR_PROPERTY);
        if (baseDir == null) {
            throw new RepositoryException("Required property is not set: "
                    + BASE_DIR_PROPERTY);
        }
        File dir = new File(baseDir);
        m_identityFile = new File(dir, IDENTITY_FILENAME);
        m_recordsDir = new File(dir, RECORDS_DIRNAME);
        m_setsDir = new File(dir, SETS_DIRNAME);
        m_formatsDir = new File(dir, FORMATS_DIRNAME);
        if (!dir.exists()) {
            throw new RepositoryException("Base directory does not exist: "
                    + dir.getPath());
        }
        if (!m_identityFile.exists()) {
            throw new RepositoryException("Identity file does not exist: "
                    + m_identityFile.getPath());
        }
        if (!m_recordsDir.exists()) {
            throw new RepositoryException("Records directory does not exist: "
                    + m_recordsDir.getPath());
        }
        if (!m_setsDir.exists()) {
            throw new RepositoryException("Sets directory does not exist: "
                    + m_setsDir.getPath());
        }
        if (!m_formatsDir.exists()) {
            throw new RepositoryException("Formats directory does not exist: "
                    + m_formatsDir.getPath());
        }
    }

    @Override
    public Properties getProps() {
        return this.props;
    }

    @Override
    public void write(PrintWriter out) throws RepositoryException {
        writeFromFile(m_identityFile, out);
    }

    @Override
    public Date getLatestDate() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");
        long latest = 0;
        String[] names = m_recordsDir.list();
        for (String name : names) {
            String[] temp = name.replaceFirst("-", " ")
                    .replaceFirst("-", " ")
                    .split(" ");
            if (temp.length == 3 && temp[2].contains(".")) {
                try {
                    long recDate = df.parse(temp[2].substring(0, temp[2].indexOf("."))).getTime();
                    if (recDate > latest) latest = recDate;
                } catch (Exception e) {
                    System.out.println("WARNING: Ignoring unparsable filename: "
                            + name);
                }
            }
        }
        return new Date(latest);
    }

    @Override
    public RemoteIterator<MetadataFormat> listMetadataFormats() {
        return new RemoteIteratorImpl<>(
                getMetadataFormatCollection().iterator());
    }

    @Override
    public RemoteIterator<SetInfo> listSetInfo() {
        return new RemoteIteratorImpl<>(
                getSetInfoCollection().iterator());
    }

    @Override
    public RemoteIterator<Record> listRecords(Date from,
                                              Date until,
                                              String mdPrefix) {
        return new RemoteIteratorImpl<>(getRecordCollection(from,
                until,
                mdPrefix).iterator());
    }

    // In this case, sourceInfo is the full path to the source file.
    @Override
    public void writeRecordXML(String itemID,
                               String mdPrefix,
                               String sourceInfo,
                               PrintWriter writer) throws RepositoryException {

        File file = new File(sourceInfo);
        writeFromFile(file, writer);
    }

    private Collection<Record> getRecordCollection(Date from,
                                                   Date until,
                                                   String mdPrefix) {
        List<Record> list = new ArrayList<>();
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH-mm-ss");
        String[] names = m_recordsDir.list();
        for (String name : names) {
            String[] temp = name.replaceFirst("-", " ")
                    .replaceFirst("-", " ")
                    .split(" ");
            if (temp.length == 3 && temp[2].contains(".")) {
                String[] parts = name.split("-");
                if (parts[1].equals(mdPrefix)) {
                    try {
                        long recDate = df.parse(temp[2].substring(0, temp[2].indexOf("."))).getTime();
                        if ((from == null || from.getTime() < recDate)
                                && (until.getTime() >= recDate)) {
                            String itemID = "oai:example.org:" + parts[0];
                            list.add(new RecordImpl(itemID,
                                    mdPrefix,
                                    new File(m_recordsDir,
                                            name)));
                        }
                    } catch (Exception e) {
                        System.out.println("WARNING: Ignoring unparsable filename: "
                                + name);
                    }
                }
            }
        }
        return list;
    }

    private Collection<SetInfo> getSetInfoCollection() {
        try {
            List<SetInfo> list = new ArrayList<>();
            String[] names = m_setsDir.list();
            for (String name : names) {
                if (name.endsWith(".xml")) {
                    String spec = name.split("\\.")[0].replaceAll("-", ":");
                    list.add(new SetInfoImpl(spec, new File(m_setsDir,
                            name)));
                }
            }
            return list;
        } catch (Exception e) {
            throw new RepositoryException("Error getting set information", e);
        }
    }

    private Collection<MetadataFormat> getMetadataFormatCollection() {
        try {
            List<MetadataFormat> list = new ArrayList<>();
            String[] names = m_formatsDir.list();
            for (String name : names) {
                if (name.endsWith(".txt")) {
                    String prefix = name.split("\\.")[0];
                    BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            new FileInputStream(new File(m_formatsDir, name)),
                                            "UTF-8"));
                    String uri = reader.readLine();
                    if (uri == null) {
                        throw new RepositoryException("Error reading first "
                                + "line of format file: " + name);
                    }
                    String loc = reader.readLine();
                    if (loc == null) {
                        throw new RepositoryException("Error reading second "
                                + "line of format file: " + name);
                    }
                    list.add(new MetadataFormatImpl(prefix, uri, loc));
                }
            }
            return list;
        } catch (Exception e) {
            throw new RepositoryException("Error getting metadata formats", e);
        }
    }
}
