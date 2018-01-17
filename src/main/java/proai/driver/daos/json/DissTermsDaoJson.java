package proai.driver.daos.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import oaiprovider.mappings.DissTerms;
import oaiprovider.mappings.DissTerms.DissTerm;
import oaiprovider.mappings.DissTerms.Term;
import oaiprovider.mappings.DissTerms.XmlNamspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DissTermsDaoJson {
    private static final Logger logger = LoggerFactory.getLogger(DissTermsDaoJson.class);

    private DissTerms dissTerms;

    public DissTermsDaoJson(InputStream disseminationConfiguration) throws IOException {
        ObjectMapper om = new ObjectMapper();
        dissTerms = om.readValue(disseminationConfiguration, DissTerms.class);
    }

    public Map<String, String> getMapXmlNamespaces() {
        Map<String, String> map = new HashMap<>();

        for (XmlNamspace namspace : xmlNamespaces()) {
            map.put(namspace.getPrefix(), namspace.getUrl());
        }

        return map;
    }

    public Set<XmlNamspace> getSetXmlNamespaces() {
        return xmlNamespaces();
    }

    public XmlNamspace getXmlNamespace(String prefix) {
        XmlNamspace xmlNamspace = null;

        for (XmlNamspace namespace : xmlNamespaces()) {

            if (namespace.getPrefix().equals(prefix)) {
                xmlNamspace = namespace;
            }
        }

        return xmlNamspace;
    }

    public Term getTerm(String diss, String name) {
        HashSet<DissTerm> dissTerms = (HashSet<DissTerm>) this.dissTerms.getDissTerms();
        Term term = null;

        for (DissTerm dt : dissTerms) {

            if (!dt.getDiss().equals(diss)) {
                logger.debug(diss + " is does not exists in dissemination-config.");
                continue;
            }

            if (dt.getTerms().isEmpty()) {
                logger.debug(diss + " has no terms config.");
                continue;
            }

            for (Term t : dt.getTerms()) {

                if (!t.getName().equals(name)) {
                    logger.debug("The term name " + name + " is not available in dissemination " + diss);
                    continue;
                }

                term = t;
            }

            if (term != null) {
                break;
            }
        }

        return term;
    }

    private Set<XmlNamspace> xmlNamespaces() {
        HashSet<XmlNamspace> xmlNamespaces = (HashSet<XmlNamspace>) dissTerms.getXmlnamespacees();
        return xmlNamespaces;
    }
}
