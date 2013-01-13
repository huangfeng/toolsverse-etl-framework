/*
 * ParserTypeFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.lang;

import java.util.Map;

/**
 * A parser type factory creates a <code>ParserType</code> object based on the language and returns language based on file extension.
 * The implementation of this interface is used by code editors to instantiate code highlighter for the specific language such as HTML, XML, Java, etc.
 *
 * @see com.toolsverse.util.lang.ParserType
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ParserTypeFactory
{
    
    /** NONE. */
    static final String NONE_LANG = "None";
    
    /** JAVA SCRIPT. */
    static final String JAVA_SCRIPT_LANG = "JavaScript";
    
    /** XML. */
    static final String XML_LANG = "XML";
    
    /** HTML. */
    static final String HTML_LANG = "HTML";
    
    /** Mix of XML and HTML */
    static final String XML_HTML_LANG = "XML and HTML";
    
    /** Mix of HTML. */
    static final String HTML_MIXED_LANG = "HTML Mixed";
    
    /** PHP. */
    static final String PHP_LANG = "PHP";
    
    /** Mix of HTML and PHP. */
    static final String HTML_PHP_LANG = "HTML and PHP";
    
    /** CSS. */
    static final String CSS_LANG = "CSS";
    
    /** SPARQL. */
    static final String SPARQL_LANG = "SPARQL";
    
    /** PYTHON. */
    static final String PYTHON_LANG = "Python";
    
    /** LUA. */
    static final String LUA_LANG = "Lua";
    
    /** RUBY. */
    static final String RUBY_LANG = "Ruby";
    
    /** SQL. */
    static final String SQL_LANG = "SQL";
    
    /** DIFF. */
    static final String DIFF_LANG = "diff";
    
    /** C#. */
    static final String CSHARP_LANG = "C#";
    
    /** JAVA. */
    static final String JAVA_LANG = "Java";
    
    /** XQUERY. */
    static final String XQUERY_LANG = "XQuery";
    
    /** C. */
    static final String C_LANG = "C";
    
    /** C++. */
    static final String CPLUS_LANG = "C++";
    
    /** DELPHI. */
    static final String DELPHI_LANG = "Delphi";
    
    /** ASM86_. */
    static final String ASM86_LANG = "Asm86";
    
    /** FORTRAN. */
    static final String FORTRAN_LANG = "Fortran";
    
    /** The GROOVY. */
    static final String GROOVY_LANG = "Groovy";
    
    /** JSP. */
    static final String JSP_LANG = "Jsp";
    
    /** MAKEFILE. */
    static final String MAKEFILE_LANG = "Makefile";
    
    /** PERL. */
    static final String PERL_LANG = "Perl";
    
    /** PROPS. */
    static final String PROPS_LANG = "Properties";
    
    /** SAS. */
    static final String SAS_LANG = "SAS";
    
    /** TCL. */
    static final String TCL_LANG = "TCL";
    
    /** SHELL. */
    static final String SHELL_LANG = "Shell";
    
    /** CMD. */
    static final String CMD_LANG = "Cmd";
    
    /**
     * Gets the language by file extension. If there is no matching language returns <code>NONE_LANG</code>. 
     *
     * @param ext the file extension
     * @return the language
     */
    String getLangByExt(String ext);
    
    /**
     * Gets the language by file extension.
     *
     * @param ext the file extension
     * @param defLang the value returned if there is no language matching file extension  
     * @return the language
     */
    String getLangByExt(String ext, String defLang);
    
    /**
     * Gets the ParserType by language. 
     *
     * @param lang the langguage
     * @return the ParserType
     */
    ParserType getParserType(String lang);
    
    /**
     * Gets the map of the parser types. The keys are languages. 
     *
     * @return the map of the parser types
     */
    Map<String, ParserType> getParserTypes();
}
