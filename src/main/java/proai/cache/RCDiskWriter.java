package proai.cache;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class RCDiskWriter extends PrintWriter {

    private File m_file;
    private String m_path;

    public RCDiskWriter(File baseDir, String path) throws Exception {
        super(new FileOutputStream(new File(baseDir, path)));
        m_path = path;
        m_file = new File(baseDir, path);
    }

    public String getPath() {
        return m_path;
    }

    public File getFile() {
        return m_file;
    }

}
