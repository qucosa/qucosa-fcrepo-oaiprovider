package proai.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OracleDDLConverter
        implements DDLConverter {

    public OracleDDLConverter() {
    }

    public boolean supportsTableType() {
        return true;
    }

    public List<String> getDDL(TableSpec spec) {
        ArrayList<String> l = new ArrayList<>();
        StringBuffer out = new StringBuffer();
        StringBuffer end = new StringBuffer();
        out.append("CREATE TABLE " + spec.getName() + " (\n");
        Iterator<ColumnSpec> csi = spec.columnSpecIterator();
        int csNum = 0;
        while (csi.hasNext()) {
            if (csNum > 0) {
                out.append(",\n");
            }
            csNum++;
            ColumnSpec cs = csi.next();
            out.append("  ");
            out.append(cs.getName());
            out.append(' ');
            if (cs.getType().toLowerCase().indexOf("int(") == 0) {
                // if precision was specified for int, use oracle's default int precision
                out.append("int");
            } else {
                if (cs.getType().toLowerCase().indexOf("smallint(") == 0) {
                    out.append("smallint");
                } else {
                    if (cs.getType().toLowerCase().equals("bigint")) {
                        out.append("NUMBER(20,0)");
                    } else if (cs.getType().toLowerCase().equals("text")) {
                        out.append("CLOB");
                    } else {
                        out.append(cs.getType());
                    }
                }
            }
            if (cs.isAutoIncremented()) {
                // oracle doesn't support auto-increment in a CREATE TABLE
                // ... but it can be done by creating the table,
                // creating a sequence, then creating a trigger that
                // inserts the sequence's next value for that column
                // upon insert.
                String createSeq = "CREATE SEQUENCE " +
                        spec.getName() +
                        "_S" +
                        csNum +
                        "\n" +
                        "  START WITH 1\n" +
                        "  INCREMENT BY 1\n" +
                        "  NOMAXVALUE";
                l.add(createSeq);
                String createTrig = "CREATE TRIGGER " +
                        spec.getName() +
                        "_T" +
                        csNum +
                        "\n" +
                        "  BEFORE INSERT ON " +
                        spec.getName() +
                        "\n  FOR EACH ROW" +
                        "\n  BEGIN" +
                        "\n    SELECT " +
                        spec.getName() +
                        "_S" +
                        csNum +
                        ".NEXTVAL INTO :NEW." +
                        cs.getName() +
                        " FROM DUAL;" +
                        "\n  END;";
                l.add(createTrig);
            }
            if (cs.getDefaultValue() != null) {
                out.append(" DEFAULT '");
                out.append(cs.getDefaultValue());
                out.append("'");
            }
            if (cs.isNotNull()) {
                out.append(" NOT NULL");
            }
            if (cs.isUnique()) {
                if (!end.toString().equals("")) {
                    end.append(",\n");
                }
                end.append("  UNIQUE ");
                end.append(" (");
                end.append(cs.getName());
                end.append(")");
            }
            if (cs.getForeignTableName() != null) {
                if (!end.toString().equals("")) {
                    end.append(",\n");
                }
                end.append("  FOREIGN KEY ");
                end.append(cs.getName());
                end.append(" (");
                end.append(cs.getName());
                end.append(") REFERENCES ");
                end.append(cs.getForeignTableName());
                end.append(" (");
                end.append(cs.getForeignColumnName());
                end.append(")");
                if (cs.getOnDeleteAction() != null) {
                    end.append(" ON DELETE ");
                    end.append(cs.getOnDeleteAction());
                }
            }
        }
        if (spec.getPrimaryColumnName() != null) {
            out.append(",\n  PRIMARY KEY (");
            out.append(spec.getPrimaryColumnName());
            out.append(")");
        }
        if (!end.toString().equals("")) {
            out.append(",\n");
            out.append(end);
        }
        out.append("\n");
        out.append(")");
        l.add(0, out.toString());
        return l;
    }

    public String getDropDDL(String command) {
        String[] parts = command.split(" ");
        String objectType = parts[1];
        String objectName = parts[2];
        return "DROP " + objectType + " " + objectName;
    }

}

