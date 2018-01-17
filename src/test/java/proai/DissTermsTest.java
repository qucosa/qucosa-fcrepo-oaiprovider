package proai;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Assert;
import org.junit.Test;

import oaiprovider.mappings.DissTerms.Term;
import oaiprovider.mappings.DissTerms.XmlNamspace;
import proai.driver.daos.json.DissTermsDaoJson;

import java.io.IOException;

public class DissTermsTest {
    private DissTermsDaoJson dissTermsData;

    public DissTermsTest() throws IOException {
        dissTermsData = new DissTermsDaoJson(getClass().getResourceAsStream("/config/dissemination-config.json"));
    }

    @Test
    public void xmlNamespacesTest() {
        Assert.assertNotNull(dissTermsData.getSetXmlNamespaces());
        assertThat(dissTermsData.getSetXmlNamespaces(), not(IsEmptyCollection.empty()));
    }

    @Test
    public void getNamespaceTest() {
        XmlNamspace namspace = dissTermsData.getXmlNamespace("xMetaDiss");
        Assert.assertEquals("xMetaDiss", namspace.getPrefix());
        Assert.assertEquals("http://www.d-nb.de/standards/xmetadissplus/", namspace.getUrl());
    }

    @Test
    public void getTermTest() {
        Term term = dissTermsData.getTerm("xDDC", "xmetadissplus");
        Assert.assertEquals("xmetadissplus", term.getName());
        Assert.assertEquals("//xMetaDiss:xMetaDiss/dc:subject[@xsi:type='dcterms:DDC' and .='$val']",
                term.getTerm());
    }
}
