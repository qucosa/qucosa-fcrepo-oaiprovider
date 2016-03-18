package proai.driver.impl;

import proai.Record;

import java.io.File;

public class RecordImpl implements Record {

    private File m_file;
    private String m_itemID;
    private String m_prefix;

    public RecordImpl(String itemID, String prefix, File file) {
        m_itemID = itemID;
        m_prefix = prefix;
        m_file = file;
    }

    public String getItemID() {
        return m_itemID;
    }

    public String getPrefix() {
        return m_prefix;
    }

    public String getSourceInfo() {
        return m_file.getPath();
    }

}
