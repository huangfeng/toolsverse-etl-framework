/*
 * ParserType.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.lang;

/**
 * This class used to configure instance of the code highlighter for the specific language such as HTML, XML, Java, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ParserType
{
    
    private String _name;
    
    private String _configStr;
    
    /**
     * Instantiates a new ParserType
     *
     * @param name the name
     * @param configStr the configuration string
     */
    public ParserType(String name, String configStr)
    {
        _name = name;
        _configStr = configStr;
    }
    
    /**
     * Gets the configuration string.
     *
     * @return the configuration string.
     */
    public String getConfigStr()
    {
        return _configStr;
    }
    
    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return _name;
    }
}
