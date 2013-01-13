/*
 * StorageProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage;

import java.util.Map;

import com.toolsverse.util.TypedKeyValue;

/**
 * The interface for all classes that used to get\set\remove properties  Possible implementations are: local storage provider (map), 
 * HTML5 storage provider, etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface StorageProvider
{
    
    /**
     * The Class StorageObject. The key is a scope and the value is a value. The properties stored in the storage as name\StorageObject pairs.
     * Possible scopes are: GLOBAL, LOCAL, SESSION.
     */
    public class StorageObject extends TypedKeyValue<String, Object>
    {
        
        /**
         * Instantiates a new storage object.
         *
         * @param value the value
         */
        public StorageObject(Object value)
        {
            super(GLOBAL, value);
        }
        
        /**
         * Instantiates a new storage object.
         *
         * @param type the type
         * @param value the value
         */
        public StorageObject(String type, Object value)
        {
            super(type, value);
        }
        
        @Override
        public boolean equals(Object compareTo)
        {
            if (!(compareTo instanceof StorageObject))
                return false;
            
            if (!getKey().equals((((StorageObject)compareTo).getKey())))
                return false;
            
            String str1 = toString();
            
            String str2 = compareTo.toString();
            
            return (str1 == null && str2 == null)
                    || (str1 != null && str1.equals(str2));
        }
        
        /**
         * Checks if scope is global.
         *
         * @return true, if is global
         */
        public boolean isGlobal()
        {
            return GLOBAL.equalsIgnoreCase(getKey());
        }
        
        /**
         * Checks if scope is local.
         *
         * @return true, if is local
         */
        public boolean isLocal()
        {
            return LOCAL.equalsIgnoreCase(getKey());
        }
        
        /**
         * Checks if scope is session.
         *
         * @return true, if is session
         */
        public boolean isSession()
        {
            return SESSION.equalsIgnoreCase(getKey());
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.util.TypedKeyValue#toString()
         */
        @Override
        public String toString()
        {
            return getValue() != null ? getValue().toString() : null;
        }
    }
    
    /** The GLOBAL scope. */
    static final String GLOBAL = "global";
    
    /** The LOCAL scope. */
    static final String LOCAL = "local";
    
    /** The SESSION scope. */
    static final String SESSION = "session";
    
    /**
     * Gets the properties.
     *
     * @return the properties
     */
    Map<String, StorageObject> getProperties();
    
    /**
     * Gets the property.
     *
     * @param key the key
     * @return the property
     */
    Object getProperty(String key);
    
    /**
     * Gets the property.
     *
     * @param key the key
     * @param def the def
     * @return the property
     */
    Object getProperty(String key, Object def);
    
    /**
     * Removes the property.
     *
     * @param key the key
     * @return the object
     */
    Object removeProperty(String key);
    
    /**
     * Sets the property.
     *
     * @param key the key
     * @param value the value
     * @return the object
     */
    Object setProperty(String key, StorageObject value);
    
}