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

package proai;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import proai.error.ProtocolException;
import proai.error.ServerException;
import proai.service.Responder;
import proai.service.ResponseData;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Ignore("faulty integration test")
public class ResponderTestIT {

    // TODO Debug flag, no need for such in unit tests, remove it
    private static final boolean m_print = false;
    private static Responder m_responder;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.load(ResponderTestIT.class.getResourceAsStream("/proai.properties"));

        File baseDir = new File(ResponderTestIT.class.getResource("/").getFile());
        File schemaDir = new File(baseDir, "schemaCache");
        File cacheDir = new File(baseDir, "cache");
        File sessionDir = new File(baseDir, "sessions");

        properties.setProperty("proai.driver.simple.baseDir", baseDir.getAbsolutePath());
        properties.setProperty("proai.cacheBaseDir", cacheDir.getAbsolutePath());
        properties.setProperty("proai.schemaDir", schemaDir.getAbsolutePath());
        properties.setProperty("proai.sessionBaseDir", sessionDir.getAbsolutePath());

        m_responder = new Responder(properties);
        waitForResponderSetup();
    }

    private static void waitForResponderSetup() throws InterruptedException {
        // TODO Waiting is a unit test antipattern, remove it
        System.out.println("Allowing 10 seconds for initial update.");
        Thread.sleep(10000);
    }

    @AfterClass
    public static void tearDown() {
        m_responder.close();
    }

    @Test
    @Ignore("Items are missing due to faulty database setup")
    public void testAll() throws Exception {
        System.out.println("Running testAll()...");
        doListMetadataFormatsTest(null);
        doListMetadataFormatsTest("oai:example.org:item1");
        doListMetadataFormatsTest("oai:example.org:item4");

        doListSetsTest();

        doListRecordsTest(null, null, "oai_dc", null);

        doListRecordsTest(null, null, "test_format", null);
        doListRecordsTest("2005-01-01T08:53:00Z", null, "test_format", "prime");
        doListRecordsTest(null, "2005-01-01T08:53:00Z", "oai_dc", "prime");

        doListIdentifiersTest(null, null, "oai_dc", null);
        doListIdentifiersTest(null, null, "test_format", null);
        doListIdentifiersTest("2005-01-01T08:53:00Z", null, "test_format", "prime");
        doListIdentifiersTest(null, "2005-01-01T08:53:00Z", "oai_dc", "prime");
    }

    @Test
    @Ignore("Items are missing due to faulty database setup")
    public void testGetGoodOaiDcRecords() throws Exception {
        System.out.println("Running testGetGoodRecords()...");
        doGetGoodRecord("oai:example.org:item1", "oai_dc");
        doGetGoodRecord("oai:example.org:item2", "oai_dc");
        doGetGoodRecord("oai:example.org:item3", "oai_dc");
        doGetGoodRecord("oai:example.org:item4", "oai_dc");
        doGetGoodRecord("oai:example.org:item5", "oai_dc");
    }

    @Test
    @Ignore("Items are missing due to faulty database setup")
    public void testGetGoodTestFormatRecords() throws Exception {
        doGetGoodRecord("oai:example.org:item1", "test_format");
        doGetGoodRecord("oai:example.org:item2", "test_format");
        doGetGoodRecord("oai:example.org:item3", "test_format");
    }

    private void doGetGoodRecord(String item, String prefix) throws Exception {
        try {
            tryGetRecord(item, prefix);
            System.out.println("doGetGoodRecord success for " + item + "/" + prefix);
        } catch (Exception e) {
            fail(String.format(
                    "Failed to get %s record of item: %s (Error message was: %s)",
                    prefix, item, e.getMessage()));
        }
    }

    private void tryGetRecord(String item, String prefix) {
        ResponseData data = m_responder.getRecord(item, prefix);
        if (m_print) printResult("getRecord(" + item + ", " + prefix + ")", data);
    }

    private void printResult(String call, ResponseData data) {
        StringWriter writer = new StringWriter();
        data.write(new PrintWriter(writer, true));
        System.out.println("Result of " + call);
        System.out.println(writer.toString());
    }

    @Test
    public void testGetBadRecords() throws Exception {
        System.out.println("Running testGetBadRecords()...");
        doGetBadRecord("oai:example.org:item1", "nonexisting_format");
        doGetBadRecord("oai:example.org:nonexisting_item", "oai_dc");
        doGetBadRecord("oai:example.org:nonexisting_item", "nonexisting_format");
    }

    private void doGetBadRecord(String item, String prefix) throws Exception {
        try {
            tryGetRecord(item, prefix);
            assertEquals("Expected failure to get " + prefix
                    + " record for item: " + item, true, false);
        } catch (ServerException e) {
            System.out.println("doGetBadRecord success for " + item + "/" + prefix + " : " + e.getMessage());
        }
    }

    @Test
    public void testIdentify() throws Exception {
        ResponseData data = m_responder.identify();
        if (m_print) printResult("identify()", data);
    }

    private void doListMetadataFormatsTest(String item) {
        ResponseData data = m_responder.listMetadataFormats(item);
        if (m_print) printResult("listMetadataFormats(" + item + ")", data);
    }

    private void doListSetsTest() {
        ResponseData data = m_responder.listSets(null);
        if (m_print) printResult("listSets(null)", data);
        String token = data.getResumptionToken();
        while (token != null) {
            data = m_responder.listSets(token);
            if (m_print) printResult("listSets(" + token + ")", data);
            data = m_responder.listSets(token);
            if (m_print) printResult("listSets(" + token + ")", data);
            token = data.getResumptionToken();
        }
    }

    private void doListRecordsTest(String from, String until, String prefix, String set) throws Exception {
        doListRecordsOrIdentifiersTest(from, until, prefix, set, false);
    }

    private void doListIdentifiersTest(String from, String until, String prefix, String set) throws Exception {
        doListRecordsOrIdentifiersTest(from, until, prefix, set, true);
    }

    private void doListRecordsOrIdentifiersTest(String from,
                                                String until,
                                                String prefix,
                                                String set,
                                                boolean identifiers) {
        ResponseData data = null;
        String which = null;
        try {
            if (identifiers) {
                which = "Identifiers";
                data = m_responder.listIdentifiers(from, until, prefix, new String[]{set}, null);
            } else {
                which = "Records";
                data = m_responder.listRecords(from, until, prefix, new String[]{set}, null);
            }
            if (m_print)
                printResult("list" + which + "(" + from + ", " + until + ", " + prefix + ", " + set + ", null)", data);
            String token = data.getResumptionToken();
            while (token != null) {
                if (identifiers) {
                    data = m_responder.listIdentifiers(null, null, null, null, token);
                } else {
                    data = m_responder.listRecords(null, null, null, null, token);
                }
                if (m_print) printResult("list" + which + "(null, null, null, null, " + token + ")", data);
                token = data.getResumptionToken();
            }
        } catch (ProtocolException e) {
            System.out.println("For from = " + from + ", until = " + until + ", prefix = " + prefix + ", set = " + set + ", List" + which + " got protocol exception: " + e.getMessage());
        }
    }

}
