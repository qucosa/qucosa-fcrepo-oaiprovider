package proai;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import oaiprovider.mappings.DisseminationTerms;
import oaiprovider.mappings.DisseminationTerms.Term;
import proai.driver.impl.DisseminationTermsImpl;
import proai.driver.impl.SetSpecImpl;

public class SetSpecXdocNodes {
    private SetSpecImpl setSpecMerge = new SetSpecImpl();

    private DisseminationTermsImpl disseminationTermsImpl = new DisseminationTermsImpl();

    private File dissFile = new File(getClass().getClassLoader().getResource("disseminations/dcc:330.xml").getPath());

    private String confFile = "config/dissemination-terms.json";

    private String nameDissKey = "xMetaDissPlusDissemination";

    private String dissType = "xDDC";

    @Test
    public void findDissTerms() {
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
                            DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
                            builderFactory.setNamespaceAware(true);

                            try {
                                DocumentBuilder builder = builderFactory.newDocumentBuilder();
                                Document document = builder.parse(dissFile);

                                XPathFactory pathFactory = XPathFactory.newInstance();
                                XPath xPath = pathFactory.newInstance().newXPath();

                                xPath.setNamespaceContext(new NamespaceContext() {

                                    @Override
                                    public String getNamespaceURI(String prefix) {
                                        switch (prefix) {
                                        case "xMetaDiss":
                                            return "http://www.d-nb.de/standards/xmetadissplus/";
                                        case "dc":
                                            return "http://purl.org/dc/elements/1.1/";
                                        case "dcterms":
                                            return "http://purl.org/dc/terms/";
                                        case "xsi":
                                            return "http://www.w3.org/2001/XMLSchema-instance";
                                        default:
                                            return null;
                                        }
                                    }

                                    @Override
                                    public String getPrefix(String namespaceURI) {
                                        return null;
                                    }

                                    @Override
                                    public Iterator getPrefixes(String namespaceURI) {
                                        return null;
                                    }

                                });

                                Node node = (Node) xPath.compile(termObj.getTerm().replace("$val", "050"))
                                        .evaluate(document, XPathConstants.NODE);

                                Assert.assertNotNull(node);
                                Assert.assertEquals("050", node.getTextContent());
                            } catch (ParserConfigurationException e1) {
                                e1.printStackTrace();
                            } catch (SAXException | IOException e1) {
                                e1.printStackTrace();
                            } catch (XPathExpressionException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

                break;
            }
        }
    }
}
