package proai;

import org.junit.Assert;
import org.junit.Test;

import oaiprovider.mappings.DissTerms.Term;
import oaiprovider.mappings.DissTerms.XmlNamspace;
import proai.driver.daos.json.DissTermsDaoJson;

public class DissTermsTest {
    private DissTermsDaoJson dissTermsData = new DissTermsDaoJson();

    @Test
    public void xmlNamespacesTest() {
        dissTermsData.getSetXmlNamespaces();
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
