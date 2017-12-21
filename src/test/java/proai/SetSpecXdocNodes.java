package proai;

import oaiprovider.driver.FedoraOAIDriver;
import oaiprovider.mappings.DisseminationTerms;
import oaiprovider.mappings.DisseminationTerms.Term;
import org.custommonkey.xmlunit.XMLUnit;
import org.custommonkey.xmlunit.exceptions.XpathException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import proai.driver.impl.DisseminationTermsImpl;
import proai.driver.impl.SetSpecImpl;
import proai.xmlutils.Namespaces;
import proai.xmlutils.SimpleNamespaceContext;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import static java.io.FileDescriptor.out;
import static org.custommonkey.xmlunit.XMLAssert.assertXpathExists;

public class SetSpecXdocNodes {
    static {
        XMLUnit.setXpathNamespaceContext(new org.custommonkey.xmlunit.SimpleNamespaceContext(Namespaces.getPrefixUriMap()));
    }

    private DocumentBuilderFactory builderFactory;

    private DisseminationTermsImpl disseminationTermsImpl = new DisseminationTermsImpl();
    List<DisseminationTerms> dissTerms = disseminationTermsImpl.getDissTerms();

    private String confFile = "config/dissemination-terms.json";

    @Before
    public void setup() {
        builderFactory = DocumentBuilderFactory.newInstance();
        builderFactory.setNamespaceAware(true);
    }

    @Test
    public void findDissTerms() throws XpathException, ParserConfigurationException, IOException, SAXException {
        String nameDissKey = "xmetadissplus";
        String dissType = "xDDC";
        File dissFile = new File(getClass().getClassLoader().getResource("disseminations/dcc:330.xml").getPath());
        File conf = new File(getClass().getClassLoader().getResource(confFile).getPath());
        Assert.assertTrue(conf.exists());

        for (DisseminationTerms dissTermObj : dissTerms) {
            if (dissTermObj.getDiss().equals(dissType)) {

                if (dissTermObj.getTerms() != null && dissTermObj.getTerms().size() > 0) {

                    for (int j = 0; j < dissTermObj.getTerms().size(); j++) {
                        Term termObj = dissTermObj.getTerms().get(j);

                        if (termObj.getName().equals(nameDissKey)) {
                                Document document = builderFactory.newDocumentBuilder().parse(dissFile);

                                XPathFactory pathFactory = XPathFactory.newInstance();
                                XPath xPath = pathFactory.newInstance().newXPath();

                                SimpleNamespaceContext namespaces = new SimpleNamespaceContext(Namespaces.getPrefixUriMap());
                                xPath.setNamespaceContext(namespaces);

                                assertXpathExists(termObj.getTerm().replace("$val", "050"), document);
                        }
                    }
                }

                break;
            }
        }
    }

    @Test
    public void findDcDccDissTerms() throws XpathException, ParserConfigurationException, IOException, SAXException {
        String nameDissKey = "oai_dc";
        String dissType = "xDDC";
        File dissFile = new File(getClass().getClassLoader().getResource("disseminations/dc_50795.xml").getPath());
        File conf = new File(getClass().getClassLoader().getResource(confFile).getPath());
        Assert.assertTrue(conf.exists());

        List<DisseminationTerms> dissTerms = disseminationTermsImpl.getDissTerms();

        for (int i = 0; i < dissTerms.size(); i++) {
            DisseminationTerms dissTermObj = dissTerms.get(i);

            if (dissTermObj.getDiss().equals(dissType)) {

                if (dissTermObj.getTerms() != null && dissTermObj.getTerms().size() > 0) {

                    for (int j = 0; j < dissTermObj.getTerms().size(); j++) {
                        Term termObj = dissTermObj.getTerms().get(j);

                        if (termObj.getName().equals(nameDissKey)) {
                                Document document = builderFactory.newDocumentBuilder().parse(dissFile);

                                XPathFactory pathFactory = XPathFactory.newInstance();
                                XPath xPath = pathFactory.newInstance().newXPath();

                                SimpleNamespaceContext namespaces = new SimpleNamespaceContext(Namespaces.getPrefixUriMap());
                                xPath.setNamespaceContext(namespaces);

                                assertXpathExists(termObj.getTerm().replace("$val", "004"), document);
                        }
                    }
                }

                break;
            }
        }
    }
}
