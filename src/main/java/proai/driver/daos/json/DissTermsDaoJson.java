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

    /**
     *
     * @param diss First part of predicate in list-set-conf.json (f.e. "xDocType")
     * @param name Metadata-prefix (terms -> name in dissemination-config.json)
     * @return Term Term object (metadata-prefix + XPath-Expression)
     */
    public Term getTerm(String diss, String name) {
        HashSet<DissTerm> dissTerms = (HashSet<DissTerm>) this.dissTerms.getDissTerms();
        Term term = null;
        boolean dissExists = false;
        boolean termExists = false;

        for (DissTerm dt : dissTerms) {

            if (dt.getDiss().equals(diss)) {
                dissExists = true;

                if (!dt.getTerms().isEmpty()) {

                    for (Term t : dt.getTerms()) {

                        if (t.getName().equals(name)) {
                            termExists = true;
                            term = t;
                            break;
                        }
                    }
                }
            }
        }
        if (!dissExists) {
            logger.debug(diss + " does not exist in dissemination-config.");
        } else if (!termExists) {
            logger.debug("The term name " + name + " is not available in dissemination " + diss);
        }

        return term;
    }

    private Set<XmlNamspace> xmlNamespaces() {
        return dissTerms.getXmlnamespacees();
    }
}
