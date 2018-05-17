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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import proai.Writable;
import proai.error.ServerException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * The file-based portion of the record cache.
 */
class RCDisk {

    private static final String PATH_DATE_PATTERN = "yyyy/MM/dd/HH/mm/ss.SSS.'UUID'.'xml'";

    private static final Logger logger = LoggerFactory.getLogger(RCDisk.class);

    private final File m_baseDir;

    public RCDisk(File baseDir) {
        m_baseDir = baseDir;
        if (!m_baseDir.exists()) {
            m_baseDir.mkdirs();
        }
    }

    /**
     * Get a new RCDiskWriter backed by a new file in the disk cache.
     */
    public RCDiskWriter getNewWriter() throws ServerException {
        String path = getNewPath();
        try {
            return new RCDiskWriter(m_baseDir, path);
        } catch (Exception e) {
            throw new ServerException("Error creating new cache file: " + path, e);
        }
    }

    /**
     * Get a new, unique path (relative to m_baseDir) for a file, based on
     * the current time.
     * <p/>
     * If the directory for the path does not yet exist, it will be created.
     */
    private String getNewPath() {
        DateFormat formatter = new SimpleDateFormat(PATH_DATE_PATTERN);
        long now = System.currentTimeMillis();
        String path = formatter.format(new Date(now)).replaceAll("UUID", UUID.randomUUID().toString());
        File dir = new File(m_baseDir, path.substring(0, 16));
        dir.mkdirs();
        return path;
    }

    /**
     * Write the content of the given <code>Writable</code> to a new file and
     * return the path of the file, relative to the disk cache base directory.
     */
    public String write(Writable writable) throws ServerException {
        String path = getNewPath();
        try {
            PrintWriter writer = new PrintWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(
                                    new File(m_baseDir, path)),
                            "UTF-8"));
            writable.write(writer);
            writer.close();
            return path;
        } catch (Exception e) {
            throw new ServerException("Error writing stream to file in cache: " + path, e);
        }
    }

    CachedContent getContent(String path) {
        if (path == null) return null;
        return new CachedContent(getFile(path));
    }

    CachedContent getContent(String path, boolean headersOnly) {
        if (path == null) return null;
        return new CachedContent(getFile(path), headersOnly);
    }

    File getFile(String path) {
        return new File(m_baseDir, path);
    }

    public void delete(String path) {
        new File(m_baseDir, path).delete();
    }

}
