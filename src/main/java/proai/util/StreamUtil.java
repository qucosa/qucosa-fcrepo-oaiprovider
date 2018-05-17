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

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Static methods for working with streams (and strings).
 */
public abstract class StreamUtil {

    private static final int STREAM_BUFFER_SIZE = 4096;

    /**
     * Pipe the input stream directly to the output stream.
     */
    public static void pipe(InputStream inStream,
                            OutputStream outStream) throws IOException {
        try {
            byte[] buf = new byte[STREAM_BUFFER_SIZE];
            int len;
            while ((len = inStream.read(buf)) > 0) {
                outStream.write(buf, 0, len);
            }
        } finally {
            inStream.close();
        }
    }

    /**
     * Read the given stream into a String, assuming the given character
     * encoding.
     */
    public static String getString(InputStream in,
                                   String encoding) throws IOException {
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in, encoding));
            StringBuffer out = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                out.append(line + "\n");
                line = reader.readLine();
            }
            return out.toString();
        } finally {
            in.close();
        }
    }

    public static String xmlEncode(String in) {
        StringBuffer out = new StringBuffer();
        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);
            if (c == '<') {
                out.append("&gt;");
            } else if (c == '>') {
                out.append("&lt;");
            } else if (c == '&') {
                out.append("&amp;");
            } else if (c == '"') {
                out.append("&quot;");
            } else if (c == '\'') {
                out.append("&apos;");
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    public static String nowUTCString() {
        try {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            return formatter.format(nowUTC());
        } catch (Exception e) { // won't happen
            return null;
        }
    }

    public static Date nowUTC() {
        return convertLocalDateToUTCDate(new Date());
    }

    private static Date convertLocalDateToUTCDate(Date localDate) {
        // figure out the time zone offset of this machine (in millisecs)
        Calendar cal = Calendar.getInstance();
        int tzOffset = cal.get(Calendar.ZONE_OFFSET);
        // ...and account for daylight savings time, if applicable
        TimeZone tz = cal.getTimeZone();
        if (tz.inDaylightTime(localDate)) {
            tzOffset += cal.get(Calendar.DST_OFFSET);
        }
        // now we have UTF offset in millisecs... so subtract it from
        // localDate.millisecs
        // and return a new Date object.
        Date UTCDate = new Date();
        UTCDate.setTime(localDate.getTime() - tzOffset);
        return UTCDate;
    }

}
