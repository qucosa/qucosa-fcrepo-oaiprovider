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

import proai.Writable;
import proai.error.ServerException;

import java.io.*;

public class CachedContentAggregate implements Writable {

    private final RecordCache m_cache;
    private final File m_listFile;
    private final String m_verb;

    /**
     * Expects a file of the following form.
     * <p/>
     * line 0 - n : path [dateString if record or header]
     * line n+1   : "end" or "end resumptionToken cursor"
     * <p/>
     * Note that if dateString isn't given for a line in the file, it will
     * be written as-is.
     */
    public CachedContentAggregate(File listFile,
                                  String verb,
                                  RecordCache cache) {
        m_listFile = listFile;
        m_verb = verb;
        m_cache = cache;
    }

    public void write(PrintWriter out) throws ServerException {
        BufferedReader lineReader = null;
        try {
            boolean headersOnly = m_verb.equals("ListIdentifiers");
            out.println("<" + m_verb + ">");
            lineReader = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(m_listFile), "UTF-8"));
            String line = lineReader.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                if (line.startsWith("end")) {
                    if (parts.length == 3) {
                        // it's resumable so write the resumptionToken w/cursor
                        out.println("<resumptionToken cursor=\"" + parts[2] + "\">" + parts[1] + "</resumptionToken>");
                    } else if (parts.length == 2) {
                        // it's the last part so write an empty resumptionToken w/cursor
                        out.println("<resumptionToken cursor=\"" + parts[1] + "\"/>");
                    }
                    line = null;
                } else {
                    new CachedContent(m_cache.getFile(parts[0]), headersOnly).write(out);
                    line = lineReader.readLine();
                }
            }
            out.println("</" + m_verb + ">");
        } catch (Exception e) {
            throw new ServerException("Error writing cached content aggregate", e);
        } finally {
            if (lineReader != null) try {
                lineReader.close();
            } catch (Exception ignored) {
            }
        }
    }

}
