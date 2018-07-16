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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class CachedContent implements Writable {

    final private File m_file;
    final private boolean m_headerOnly;
    final private String m_string;

    CachedContent(File file) {
        m_file = file;
        m_headerOnly = false;
        m_string = null;
    }

    CachedContent(File file, boolean headerOnly) {
        m_file = file;
        m_headerOnly = headerOnly;
        m_string = null;
    }

    CachedContent(String content) {
        m_file = null;
        m_headerOnly = false;
        m_string = content;
    }

    public void write(PrintWriter out) throws ServerException {
        if (m_file != null) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(m_file), "UTF-8"));
                String line = reader.readLine();
                while (line != null) {
                    if (!m_headerOnly || !line.contains("<record>")) {
                        out.println(line);
                    }
                    if (m_headerOnly && line.contains("</header>")) {
                        break;
                    }
                    line = reader.readLine();
                }
            } catch (Exception e) {
                throw new ServerException("Error reading from file: " + m_file.getPath(), e);
            } finally {
                if (reader != null) try {
                    reader.close();
                } catch (Exception ignored) {
                }
            }
        } else {
            out.println(m_string);
        }
    }

}
