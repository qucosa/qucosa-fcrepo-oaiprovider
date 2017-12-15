package proai.driver.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import oaiprovider.mappings.DisseminationTerms;

/**
 * Load dissemination term configs.
 *
 * @author dseelig
 *
 */
public class DisseminationTermsImpl {
    private ObjectMapper om = new ObjectMapper();

    public List<DisseminationTerms> getDissTerms() {
        List<DisseminationTerms> dissTerms = null;
        File file = new File(getClass().getClassLoader().getResource("config/dissemination-terms.json").getPath());

        try {
            dissTerms = om.readValue(file,
                    om.getTypeFactory().constructCollectionType(List.class, DisseminationTerms.class));
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dissTerms;
    }
}
