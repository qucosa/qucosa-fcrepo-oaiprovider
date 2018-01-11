/*
 * Copyright (C) 2017 Saxon State and University Library Dresden (SLUB)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package de.qucosa.xmlutils;

import org.jdom2.Namespace;

import java.util.HashMap;
import java.util.Map;

public final class Namespaces {

    private static final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    private static final Namespace MEXT = Namespace.getNamespace("mext", "http://slub-dresden.de/mets");
    private static final Namespace MODS = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    private static final Namespace XLIN = Namespace.getNamespace("xlin", "http://www.w3.org/1999/xlink");
    private static final Namespace SLUB = Namespace.getNamespace("slub", "http://slub-dresden.de/");

    private static final Namespace XMETADISSPLUS = Namespace.getNamespace("xMetaDiss", "http://www.d-nb.de/standards/xmetadissplus/");
    private static final Namespace DC = Namespace.getNamespace("dc", "http://purl.org/dc/elements/1.1/");
    private static final Namespace DCTERMS = Namespace.getNamespace("dcterms", "http://purl.org/dc/terms/");
    private static final Namespace XSI = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
    private static final Namespace DINI = Namespace.getNamespace("dini", "http://www.d-nb.de/standards/xmetadissplus/type/");

    private Namespaces() {
    }

    static public Map<String, String> getPrefixUriMap() {
        return new HashMap<String, String>() {{
            put(METS.getPrefix(), METS.getURI());
            put(MEXT.getPrefix(), MEXT.getURI());
            put(MODS.getPrefix(), MODS.getURI());
            put(SLUB.getPrefix(), SLUB.getURI());
            put(XLIN.getPrefix(), XLIN.getURI());

            put(XMETADISSPLUS.getPrefix(), XMETADISSPLUS.getURI());
            put(DC.getPrefix(), DC.getURI());
            put(DCTERMS.getPrefix(), DCTERMS.getURI());
            put(XSI.getPrefix(), XSI.getURI());
            put(DINI.getPrefix(), DINI.getURI());
        }};
    }

}
