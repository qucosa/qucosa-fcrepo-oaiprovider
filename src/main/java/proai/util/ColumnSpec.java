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

class ColumnSpec {

    private final boolean m_binary;
    private final String m_defaultValue;
    private final String m_foreignColumnName;
    private final String m_foreignTableName;
    private final String m_indexName;
    private final boolean m_isAutoIncremented;
    private final boolean m_isNotNull;
    private final boolean m_isUnique;
    private final String m_name;
    private final String m_onDeleteAction;
    private final String m_type;

    public ColumnSpec(String name, String type, boolean binary, String defaultValue,
                      boolean isAutoIncremented, String indexName, boolean isUnique,
                      boolean isNotNull, String foreignTableName, String foreignColumnName,
                      String onDeleteAction) {
        m_name = name;
        m_type = type;
        m_binary = binary;
        m_defaultValue = defaultValue;
        m_isAutoIncremented = isAutoIncremented;
        m_indexName = indexName;
        m_isUnique = isUnique;
        m_isNotNull = isNotNull;
        m_foreignTableName = foreignTableName;
        m_foreignColumnName = foreignColumnName;
        m_onDeleteAction = onDeleteAction;
    }

    public String getName() {
        return m_name;
    }

    public boolean getBinary() {
        return m_binary;
    }

    public String getType() {
        return m_type;
    }

    public String getForeignTableName() {
        return m_foreignTableName;
    }

    public String getForeignColumnName() {
        return m_foreignColumnName;
    }

    public String getOnDeleteAction() {
        return m_onDeleteAction;
    }

    public boolean isUnique() {
        return m_isUnique;
    }

    public boolean isNotNull() {
        return m_isNotNull;
    }

    public String getIndexName() {
        return m_indexName;
    }

    public boolean isAutoIncremented() {
        return m_isAutoIncremented;
    }

    public String getDefaultValue() {
        return m_defaultValue;
    }

}
