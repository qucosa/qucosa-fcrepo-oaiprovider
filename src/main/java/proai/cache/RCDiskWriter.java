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
