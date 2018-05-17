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

import java.io.*;

class QueueIterator {

    private final File _tempFile;
    private QueueItem _next;
    private BufferedReader _reader;

    public QueueIterator(File tempFile) throws IOException {
        _tempFile = tempFile;
        _reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(_tempFile), "UTF-8"));
        _next = getNext();
    }

    private static QueueItem parseLine(String line) throws IOException {
        String[] parts = line.split(" ");
        if (parts.length > 4) {
            try {
                StringBuffer sourceInfo = new StringBuffer();
                for (int i = 4; i < parts.length; i++) {
                    if (i > 4) sourceInfo.append(' ');
                    sourceInfo.append(parts[i]);
                }
                return new QueueItem(Integer.parseInt(parts[0]),
                        parts[1],
                        parts[2],
                        sourceInfo.toString(),
                        parts[3].charAt(0));
            } catch (Throwable th) {
                throw new IOException("Error parsing next line: " + th.getClass().getName());
            }
        } else {
            throw new IOException("Error parsing next line: expected at least "
                    + "5 values, but got " + parts.length);
        }
    }

    private QueueItem getNext() throws IOException {
        if (_reader == null) return null; // if already closed
        String line = _reader.readLine();
        if (line == null) { // was last line
            close();
            return null;
        } else {
            if (line.length() == 0) { // skip empty lines
                return getNext();
            } else {
                return parseLine(line);
            }
        }
    }

    public boolean hasNext() {
        return (_next != null);
    }

    public QueueItem next() throws IOException {
        if (_next == null) return null;
        QueueItem prev = _next;
        _next = getNext();
        return prev;
    }

    public void close() {
        if (_reader != null) {
            try {
                _reader.close();
            } catch (Exception ignored) {
            }
            _tempFile.delete();
            _reader = null;
        }
    }

}
