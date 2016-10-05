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

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class DBUtil {

    /**
     * Get a long string, which could be a TEXT or CLOB type.
     * (CLOBs require special handling -- this method normalizes the reading of them)
     */
    public static String getLongString(ResultSet rs, int pos) throws SQLException {
        String s = rs.getString(pos);
        if (s != null) {
            // It's a String-based datatype, so just return it.
            return s;
        } else {
            // It may be a CLOB.  If so, return the contents as a String.
            try {
                Clob c = rs.getClob(pos);
                return c.getSubString(1, (int) c.length());
            } catch (Throwable th) {
                th.printStackTrace();
                return null;
            }
        }
    }

    public static String quotedString(String in,
                                      boolean backslashIsEscape) {
        StringBuilder out = new StringBuilder();
        out.append('\'');
        if (in == null) {
            out.append("null");
        } else {
            for (int i = 0; i < in.length(); i++) {
                char c = in.charAt(i);
                if (c == '\'') {
                    out.append("''");
                } else if (backslashIsEscape && c == '\\') {
                    out.append("\\\\");
                } else {
                    out.append(c);
                }
            }
        }
        out.append('\'');
        return out.toString();
    }
}
