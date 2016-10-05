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

package oaiprovider;

import org.fcrepo.client.FedoraClient;
import org.fcrepo.common.PID;
import org.fcrepo.server.utilities.StreamUtility;
import proai.SetInfo;
import proai.error.RepositoryException;

import java.io.*;

/**
 * SetInfo impl that includes setDescription elements for setDiss dissemination,
 * if provided + available.
 */
public class FedoraSetInfo
        implements SetInfo {

    private final FedoraClient m_fedora;
    private final InvocationSpec m_setDiss;
    private final String m_setName;
    private final PID m_setPID;
    private final String m_setSpec;

    // if setDiss is null, descriptions don't exist, which is ok
    public FedoraSetInfo(FedoraClient fedora,
                         String setObjectPID,
                         String setSpec,
                         String setName,
                         String setDiss,
                         String setDissInfo) {
        m_fedora = fedora;
        m_setPID = PID.getInstance(setObjectPID);
        m_setSpec = setSpec.replace(' ', '_');
        m_setName = setName;
        if (setDissInfo != null) {
            m_setDiss = InvocationSpec.getInstance(setDiss);
        } else {
            m_setDiss = null;
        }
    }

    public String getSetSpec() {
        return m_setSpec;
    }

    public void write(PrintWriter out) throws RepositoryException {
        out.println("<set>");
        out.println("  <setSpec>" + m_setSpec + "</setSpec>");
        out
                .println("  <setName>" + StreamUtility.enc(m_setName)
                        + "</setName>");
        writeDescriptions(out);
        out.println("</set>");
    }

    private void writeDescriptions(PrintWriter out) throws RepositoryException {
        if (m_setDiss == null) return;
        InputStream in = null;
        try {
            in = m_setDiss.invoke(m_fedora, m_setPID);
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in));
            StringBuffer buf = new StringBuffer();
            String line = reader.readLine();
            while (line != null) {
                buf.append(line + "\n");
                line = reader.readLine();
            }
            String xml = buf.toString();
            int i = xml.indexOf("<setDescriptions>");
            if (i == -1)
                throw new RepositoryException("Bad set description xml: opening <setDescriptions> not found");
            xml = xml.substring(i + 17);
            i = xml.indexOf("</setDescriptions>");
            if (i == -1)
                throw new RepositoryException("Bad set description xml: closing </setDescrptions> not found");
            out.print(xml.substring(0, i));
        } catch (IOException e) {
            throw new RepositoryException("IO error reading " + m_setDiss, e);
        } finally {
            if (in != null) try {
                in.close();
            } catch (IOException ignored) {
            }
        }
    }

}
