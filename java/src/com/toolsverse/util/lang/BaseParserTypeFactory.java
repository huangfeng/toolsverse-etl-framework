/*
 * BaseParserTypeFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.lang;

import java.util.Map;
import java.util.TreeMap;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.Utils;

/**
 * The abstract implementation of the ParserTypeFactory interface. Provides some useful common methods and constants. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class BaseParserTypeFactory implements ParserTypeFactory
{
    
    /** The map where keys are file extensions and values are languages */
    private static Map<String, String> EXT_TO_LANG = new TreeMap<String, String>(
            String.CASE_INSENSITIVE_ORDER);
    
    static
    {
        EXT_TO_LANG.put("txt", NONE_LANG);
        EXT_TO_LANG.put("sh", NONE_LANG);
        EXT_TO_LANG.put("bat", NONE_LANG);
        EXT_TO_LANG.put("cmd", NONE_LANG);
        EXT_TO_LANG.put("properties", NONE_LANG);
        EXT_TO_LANG.put("log", NONE_LANG);
        EXT_TO_LANG.put("cpp", CSHARP_LANG);
        EXT_TO_LANG.put("c", CSHARP_LANG);
        EXT_TO_LANG.put("xml", XML_LANG);
        EXT_TO_LANG.put("xsd", XML_LANG);
        EXT_TO_LANG.put("xsl", XML_LANG);
        EXT_TO_LANG.put("xslt", XML_LANG);
        EXT_TO_LANG.put("html", HTML_LANG);
        EXT_TO_LANG.put("htm", HTML_LANG);
        EXT_TO_LANG.put("xhtml", XML_HTML_LANG);
        EXT_TO_LANG.put("js", JAVA_SCRIPT_LANG);
        EXT_TO_LANG.put("php", PHP_LANG);
        EXT_TO_LANG.put("css", CSS_LANG);
        EXT_TO_LANG.put("sql", SQL_LANG);
        EXT_TO_LANG.put("java", JAVA_LANG);
        EXT_TO_LANG.put("cs", CSHARP_LANG);
        EXT_TO_LANG.put("py", PYTHON_LANG);
        EXT_TO_LANG.put("lua", LUA_LANG);
        EXT_TO_LANG.put("rb", RUBY_LANG);
        EXT_TO_LANG.put("ruby", RUBY_LANG);
        EXT_TO_LANG.put("sq", SPARQL_LANG);
        EXT_TO_LANG.put("diff", DIFF_LANG);
        EXT_TO_LANG.put("dif", DIFF_LANG);
        EXT_TO_LANG.put("xql", XQUERY_LANG);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.util.lang.ParserTypeFactory#getLangByExt(java.lang.String)
     */
    public String getLangByExt(String ext)
    {
        return getLangByExt(ext, NONE_LANG);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.util.lang.ParserTypeFactory#getLangByExt(java.lang.String,
     * java.lang.String)
     */
    public String getLangByExt(String ext, String defLang)
    {
        String lang = SystemConfig.instance().getSystemProperty(ext, null);
        
        if (!Utils.isNothing(lang))
            return lang;
        
        lang = EXT_TO_LANG.get(ext);
        
        if (!Utils.isNothing(lang))
            return lang;
        
        return defLang;
    }
}
