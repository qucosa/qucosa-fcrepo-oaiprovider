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

    public static final Namespace METS = Namespace.getNamespace("mets", "http://www.loc.gov/METS/");
    public static final Namespace MEXT = Namespace.getNamespace("mext", "http://slub-dresden.de/mets");
    public static final Namespace MODS = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
    public static final Namespace SLUB = Namespace.getNamespace("slub", "http://slub-dresden.de/");
    public static final Namespace XLIN = Namespace.getNamespace("xlin", "http://www.w3.org/1999/xlink");

    private Namespaces() {
    }

    static public Map<String, String> getPrefixUriMap() {
        return new HashMap<String, String>() {{
            put(METS.getPrefix(), METS.getURI());
            put(MEXT.getPrefix(), MEXT.getURI());
            put(MODS.getPrefix(), MODS.getURI());
            put(SLUB.getPrefix(), SLUB.getURI());
            put(XLIN.getPrefix(), XLIN.getURI());
        }};
    }

}
