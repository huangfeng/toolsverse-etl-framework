/*
 * AliasReader.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.aliasreader;

import java.util.List;

import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.util.KeyValue;

/**
 * Class which can read database aliases from the external sources must implements this interface. Example: Oracle tnsnames.ora reader. 
 * 
 * @author Maksym Sherbinin
 * @version 1.0
 * @since 3.0
 */

public interface AliasReader extends ExtensionModule
{
    /**
     * Gets the aliases.
     *
     * @return the aliases
     */
    List<KeyValue> getAliases();
    
    /**
     * Gets the name.
     * 
     * @return the name
     */
    String getName();
    
}
