package proai.driver.impl;

import proai.SetInfo;
import proai.error.RepositoryException;

import java.io.File;
import java.io.PrintWriter;

public class SetInfoImpl implements SetInfo {

    private final File m_file;
    private final String m_setSpec;

    public SetInfoImpl(String setSpec, File file) {
        m_setSpec = setSpec;
        m_file = file;
    }

    public String getSetSpec() {
        return m_setSpec;
    }

    public void write(PrintWriter out) throws RepositoryException {
        OAIDriverImpl.writeFromFile(m_file, out);
    }

}
