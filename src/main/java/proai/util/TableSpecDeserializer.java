package proai.util;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;

public class TableSpecDeserializer
        extends DefaultHandler {

    private final ArrayList<TableSpec> m_tableSpecList;
    private ArrayList<ColumnSpec> m_columnSpecList;
    private boolean m_column_autoIncrement;
    private boolean m_column_binary;
    private String m_column_default;
    private String m_column_foreignKey_columnName;
    private String m_column_foreignKey_foreignTableName;
    private String m_column_foreignKey_onDeleteAction;
    private String m_column_index;
    private String m_column_name;
    private boolean m_column_notNull;
    private String m_column_type;
    private boolean m_column_unique;
    private String m_table_name;
    private String m_table_primaryKey;
    private String m_table_type;

    protected TableSpecDeserializer() {
        m_tableSpecList = new ArrayList<>();
        m_columnSpecList = new ArrayList<>();
    }

    public List<TableSpec> getTableSpecs() {
        return m_tableSpecList;
    }

    public void startElement(String uri, String localName, String qName,
                             Attributes a) throws SAXException {
        if (localName.equals("table")) {
            m_table_name = a.getValue("name");
            if (m_table_name == null) {
                throw new SAXException("table element must have a name attribute");
            }
            m_table_primaryKey = a.getValue("primaryKey");
            m_table_type = a.getValue("type");
        } else if (localName.equals("column")) {
            m_column_name = a.getValue("name");
            if (m_column_name == null) {
                throw new SAXException("column element must have a name attribute");
            }
            m_column_type = a.getValue("type");
            if (m_column_type == null) {
                throw new SAXException("column element must have a type attribute");
            }
            if (a.getValue("binary") != null && a.getValue("binary").equalsIgnoreCase("true")) {
                m_column_binary = true;
            }
            m_column_autoIncrement = getBoolean(a, "autoIncrement");
            m_column_index = a.getValue("index");
            m_column_notNull = getBoolean(a, "notNull");
            m_column_unique = getBoolean(a, "unique");
            m_column_default = a.getValue("default");
            String f = a.getValue("foreignKey");
            if (f == null) {
                f = a.getValue("foriegnKey");
            }
            if (f != null) {
                int dotPos = f.indexOf(".");
                if (dotPos == -1) {
                    throw new SAXException("table.column not given in foreignKey attribute");
                }
                m_column_foreignKey_foreignTableName = f.substring(0, dotPos);
                int spacePos = f.indexOf(" ");
                if (spacePos == -1) {
                    m_column_foreignKey_columnName = f.substring(dotPos + 1);
                } else {
                    m_column_foreignKey_columnName = f.substring(dotPos + 1, spacePos);
                    m_column_foreignKey_onDeleteAction = f.substring(spacePos + 1);
                }
            }
        }
    }

    private boolean getBoolean(Attributes a, String name) {
        String v = a.getValue(name);
        if (v == null) {
            return false;
        }
        return (v.equalsIgnoreCase("true")) || (v.equalsIgnoreCase("yes"));
    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (localName.equals("table")) {
            m_tableSpecList.add(new TableSpec(m_table_name,
                    m_columnSpecList, m_table_primaryKey, m_table_type));
            m_table_name = null;
            m_table_primaryKey = null;
            m_table_type = null;
            m_columnSpecList = new ArrayList<>();
        } else if (localName.equals("column")) {
            m_columnSpecList.add(new ColumnSpec(m_column_name, m_column_type, m_column_binary,
                    m_column_default, m_column_autoIncrement, m_column_index,
                    m_column_unique, m_column_notNull,
                    m_column_foreignKey_foreignTableName,
                    m_column_foreignKey_columnName,
                    m_column_foreignKey_onDeleteAction));
            m_column_name = null;
            m_column_type = null;
            m_column_binary = false;
            m_column_default = null;
            m_column_autoIncrement = false;
            m_column_index = null;
            m_column_unique = false;
            m_column_notNull = false;
            m_column_foreignKey_foreignTableName = null;
            m_column_foreignKey_columnName = null;
            m_column_foreignKey_onDeleteAction = null;
        }
    }

}
