package proai.driver.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import oaiprovider.mappings.DissTerms;
import oaiprovider.mappings.DissTerms.DissTerm;
import oaiprovider.mappings.DissTerms.Term;
import oaiprovider.mappings.DissTerms.XmlNamspace;

public class DissTermsImpl {
    private static final Logger logger = LoggerFactory.getLogger(DissTermsImpl.class);

    private ObjectMapper om = new ObjectMapper();

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
        HashSet<DissTerm> dissTerms = (HashSet<DissTerm>) dissTerms().getDissTerms();
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
        HashSet<XmlNamspace> xmlNamespaces = (HashSet<XmlNamspace>) dissTerms().getXmlnamespacees();
        return xmlNamespaces;
    }

    private DissTerms dissTerms() {
        File file = new File(getClass().getClassLoader().getResource("config/dissemination-config.json").getPath());
        DissTerms dissTerms = null;

        try {
            dissTerms = om.readValue(Files.readAllBytes(Paths.get(file.getAbsolutePath())), DissTerms.class);
            return dissTerms;
        } catch (IOException e) {
            e.printStackTrace();
            logger.debug("dissemination-conf parse failed.");
            return dissTerms;
        }
    }
}
