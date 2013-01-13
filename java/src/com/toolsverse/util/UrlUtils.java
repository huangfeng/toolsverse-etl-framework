/*
 * UrlUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The general URL manipulation utilities.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class UrlUtils
{
    /** WEB-INF directory name */
    public static final String WEB_INF = "WEB-INF";
    
    /** PATH token */
    public static final String PATH_TOKEN = "path:";
    
    /** FILE token */
    public static final String FILE_TOKEN = "file:";
    
    /**
     * Decodes a <code>application/x-www-form-urlencoded</code> string using UTF-8 
     * encoding scheme.
     *
     * @param url the <code>String</code> to decode
     * @return the newly decoded <code>String</code>
     * @exception  UnsupportedEncodingException
     *             If character encoding needs to be consulted, but
     *             named character encoding is not supported
     * @see URLEncoder#encode(java.lang.String, java.lang.String)
     */
    public static String decodeUrl(String url)
    {
        if (url == null)
            return null;
        
        try
        {
            return URLDecoder.decode(url, "UTF-8");
        }
        catch (UnsupportedEncodingException ex)
        {
            return url.replaceAll("%20", " ");
        }
    }
    
    /**
     * Gets the file name from the string containing url and file name using token as a delimiter.<br><br>
     * 
     * <b>Example:</b><br>
     * 
     * input: http://localhost:8080/something/{file:test}<br>
     * token: file:<br>
     * return: test<br>
     *
     * @param token the token
     * @param input the input
     * @return the file name
     */
    public static String getFileName(String token, String input)
    {
        Matcher m = Pattern.compile(getUrlRegex(token)).matcher(input);
        
        if (!m.find())
            return "";
        
        String[] groups = m.group().split(getUrlSplitRegex(token), -1);
        
        if (groups.length != 2)
            return "";
        
        return groups[1].split("\\}", -1)[0];
    }
    
    /**
     * Gets the url from the string containing url and file name.<br><br>
     * 
     * <b>Example:</b><br>
     * 
     * input: http://localhost:8080/something/{file:test}<br>
     * token: file:<br>
     * return: http://localhost:8080/something<br>
     *
     * @param token the token
     * @param input the input
     * @return the url
     */
    public static String getUrl(String token, String input)
    {
        return input.replaceAll(getUrlRegex(token), getFileName(token, input)
                .replaceAll("\\\\", "/"));
    }
    
    /**
     * Gets regular expression for url.
     *
     * @param token the token
     * @return the regular expression for url
     */
    private static String getUrlRegex(String token)
    {
        return "\\{" + token + "[^\\}]*\\}";
    }
    
    /**
     * Gets regular expression used to split string on url and token.
     *
     * @param token the token
     * @return the regular expression used to split string on url and token
     */
    private static String getUrlSplitRegex(String token)
    {
        return "\\{" + token;
    }
    
    /**
     * Gets the url token.
     *
     * @param token the token
     * @return the url token
     */
    public static String getUrlToken(String token)
    {
        return "{" + token + "}";
    }
    
    /**
     * Checks if input string contains specific token.
     *
     * @param token the token
     * @param input the input
     * @return true, if input string contains specific token
     */
    public static boolean hasUrlToken(String token, String input)
    {
        return input != null && token != null
                && input.toLowerCase().indexOf("{" + token) > 0;
    }
    
    /**
     * Sets the value for the token in the input string.<br><br>
     * 
     * <b>Example:</b><br>
     * 
     * token: file:<br>
     * input: http://localhost:8080/something/{file:}<br>
     * value: test2
     * return: http://localhost:8080/something/{file:test2}<br>
     *
     * @param token the token
     * @param input the input
     * @param value the value
     * @return the string created by setting value for the token in the input string.
     */
    public static String setUrl(String token, String input, String value)
    {
        String ret = input.replaceAll(getUrlRegex(token),
                "{" + token + value.replaceAll("\\\\", "/") + "}");
        
        return ret;
    }
    
    /**
     * Gets path from the string representing url.<br><br>
     * 
     * <b>Example:</b><br>
     * 
     * url: file:/C:/Tomcat 5.5/webapps/developer/WEB-INF/lib/toolsverse-core.jar!/com/toolsverse/config/SystemConfig.class<br>
     * return: C:/Tomcat 5.5/webapps/developer/WEB-INF
     *
     * @param url the url
     * @return the path
     */
    public static String urlToPath(String url)
    {
        if (url == null)
            return null;
        
        int index = url.indexOf(WEB_INF);
        
        if (index < 0)
            return null;
        
        String path = url.substring(0, index + WEB_INF.length());
        
        if (path.indexOf("file:/") == 0)
            path = path.substring("file:/".length());
        
        return path;
        
    }
    
}
