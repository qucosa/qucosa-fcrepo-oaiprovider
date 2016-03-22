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
public class RCDisk {

    public static final String PATH_DATE_PATTERN = "yyyy/MM/dd/HH/mm/ss.SSS.'UUID'.'xml'";

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

    public CachedContent getContent(String path) {
        if (path == null) return null;
        return new CachedContent(getFile(path));
    }

    public File getFile(String path) {
        return new File(m_baseDir, path);
    }

    // Same as getContent, but re-writes the <datestamp> and optionally only returns the header
    public CachedContent getContent(String path, String dateStamp, boolean headerOnly) {
        return new CachedContent(getFile(path), dateStamp, headerOnly);
    }

    public void delete(String path) {
        new File(m_baseDir, path).delete();
    }

}
