/*
 * FieldsRepository.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;
import java.util.List;

/**
 * The class which implements FieldsRepository interface collects information
 * about database types.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface FieldsRepository extends Serializable
{
    
    /**
     * Gets the list of field definitions by given key and type. Key is a jdbc
     * driver class name and type is one of the <code>java.sql.Types</code>.
     * 
     * @param key
     *            the key
     * @param type
     *            the type
     * @return the list of field definitions
     */
    List<FieldDef> getFieldDef(String key, int type);
}
