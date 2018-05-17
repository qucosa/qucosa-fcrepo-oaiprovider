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

package proai.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MySQLDDLConverter
        implements DDLConverter {

    public MySQLDDLConverter() {
    }

    public List<String> getDDL(TableSpec spec) {
        StringBuilder out = new StringBuilder();
        StringBuilder end = new StringBuilder();
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
            if (cs.getType().equalsIgnoreCase("text")) {
                if (cs.getBinary()) {
                    out.append("blob");
                } else {
                    out.append(cs.getType());
                }
            } else {
                out.append(cs.getType());
                if (cs.getType().toLowerCase().startsWith("varchar")) {
                    if (cs.getBinary()) {
                        out.append(" BINARY");
                    }
                }
            }
            if (cs.isNotNull()) {
                out.append(" NOT NULL");
            }
            if (cs.isAutoIncremented()) {
                out.append(" auto_increment");
            }
            if (cs.getDefaultValue() != null) {
                out.append(" default '");
                out.append(cs.getDefaultValue());
                out.append("'");
            }
            if (cs.isUnique()) {
                if (!end.toString().equals("")) {
                    end.append(",\n");
                }
                end.append("  UNIQUE KEY ");
                end.append(cs.getName());
                end.append(" (");
                end.append(cs.getName());
                end.append(")");
            }
            if (cs.getIndexName() != null) {
                if (!end.toString().equals("")) {
                    end.append(",\n");
                }
                end.append("  KEY ");
                end.append(cs.getIndexName());
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
        if (spec.getType() != null) {
            out.append(" ENGINE=" + spec.getType());
        }
        ArrayList<String> l = new ArrayList<>();
        l.add(out.toString());
        return l;
    }

    public String getDropDDL(String command) {
        String[] parts = command.split(" ");
        String tableName = parts[2];
        return "DROP TABLE " + tableName;
    }

}

