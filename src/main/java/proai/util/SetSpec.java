/*
 * Copyright 2016 Saxon State and University Library Dresden (SLUB)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proai.util;

import proai.SetInfo;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public abstract class SetSpec {

    public static boolean hasParents(String spec) {

        return spec != null && spec.contains(":");
    }

    public static String parentOf(String spec) {
        int boundary = spec.lastIndexOf(':');
        if (boundary > 0) {
            return spec.substring(0, boundary);
        } else {
            return null;
        }
    }

    public static boolean isValid(String spec) {
        return spec
                .matches("([A-Za-z0-9_!'$\\(\\)\\+\\-\\.\\*])+(:[A-Za-z0-9_!'$\\(\\)\\+\\-\\.\\*]+)*");
    }

    public static Set<String> allSetsFor(String spec) {
        Set<String> ancestors = new HashSet<>();

        ancestors.add(spec);
        if (hasParents(spec)) {
            String parent = parentOf(spec);
            if (parent != null) {
                ancestors.add(parent);
                ancestors.addAll(SetSpec.allSetsFor(parent));
            }
        }

        return ancestors;
    }

    public static SetInfo defaultInfoFor(final String setSpec) {
        return new SetInfo() {

            public void write(PrintWriter w) {
                w.println("<set>");
                w.println("  <setSpec>" + setSpec + "</setSpec>");
                w.println("  <setName>" + setSpec + "</setName>");
                w.println("</set>");
            }

            public String getSetSpec() {
                return setSpec;
            }
        };
    }
}
