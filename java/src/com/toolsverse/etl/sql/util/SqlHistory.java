/*
 * SqlHistory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.util;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import com.toolsverse.util.IndexArrayList;
import com.toolsverse.util.IndexList;
import com.toolsverse.util.LimitedMap;
import com.toolsverse.util.TypedKeyValue;

/**
 * The data structure used to capture the history of the executed sql statements along with bind variables.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlHistory implements Serializable
{
    
    /** The data. */
    private LimitedMap<TypedKeyValue<String, TypedKeyValue<String, String>>, Map<String, Object>> _data;
    
    /**
     * Instantiates SqlHistory.
     *
     * @param size the size of the history
     */
    public SqlHistory(int size)
    {
        _data = new LimitedMap<TypedKeyValue<String, TypedKeyValue<String, String>>, Map<String, Object>>(
                size);
    }
    
    /**
     * Gets the map of bind variables (name\value) for the given sql, type and node name, 
     *
     * @param sql the sql
     * @param type the driver class name usually used as a type
     * @param nodeName the node name
     * @return the map of bind variables
     */
    public Map<String, Object> getParams(String sql, String type,
            String nodeName)
    {
        TypedKeyValue<String, String> keyValue = new TypedKeyValue<String, String>(
                type, nodeName);
        
        Map<String, Object> properties = _data
                .get(new TypedKeyValue<String, TypedKeyValue<String, String>>(
                        sql, keyValue));
        
        if (properties == null)
            properties = new LinkedHashMap<String, Object>();
        
        return properties;
    }
    
    /**
     * Gets the sql history. The sql history is an object of the IndexList type. Each element consist of sql (key) and name (value). 
     *
     * @see com.toolsverse.util.IndexList
     *
     * @return the sql history
     */
    public IndexList<TypedKeyValue<String, String>> getSqlHistory()
    {
        IndexList<TypedKeyValue<String, String>> sqlHistory = new IndexArrayList<TypedKeyValue<String, String>>();
        
        if (_data != null)
            for (TypedKeyValue<String, TypedKeyValue<String, String>> key : _data
                    .keySet())
            {
                sqlHistory.add(new TypedKeyValue<String, String>(key.getKey(),
                        key.getValue().getValue()));
            }
        
        return sqlHistory;
    }
    
    /**
     * Associates bind variables (properties) with the sql for the given type and node name.
     *
     * @param sql the sql
     * @param type the driver class name usually used as a type
     * @param properties the bind variables
     * @param nodeName the node name
     */
    public void setParams(String sql, String type,
            Map<String, Object> properties, String nodeName)
    {
        TypedKeyValue<String, String> keyValue = new TypedKeyValue<String, String>(
                type, nodeName);
        
        TypedKeyValue<String, TypedKeyValue<String, String>> key = new TypedKeyValue<String, TypedKeyValue<String, String>>(
                sql, keyValue);
        
        if (_data.containsKey(key))
            _data.remove(key);
        
        _data.put(key, properties);
    }
    
}
