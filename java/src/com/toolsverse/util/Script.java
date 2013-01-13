/*
 * Script.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * This is a wrapper for the Java scripting engine.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class Script implements ObjectStorage, Serializable
{
    /**
     * Converts any script to javascript.
     * 
     * @param code the script 
     * @return the javascript
     */
    public static String any2Js(String code)
    {
        return any2Js(code, null);
    }
    
    /**
     * Converts any script to javascript.
     *
     * @param code the script
     * @param importStr the import string
     * @return the javascript
     */
    public static String any2Js(String code, String importStr)
    {
        if (Utils.isNothing(importStr))
            importStr = Utils.makeString(importStr);
        else
        {
            importStr = importStr.trim();
            
            if (';' != importStr.charAt(importStr.length() - 1))
                importStr = importStr + ";";
        }
        
        if (Utils.isNothing(code))
            return importStr + "var value;value=true;";
        
        code = code.trim();
        
        if (';' != code.charAt(code.length() - 1))
            code = code + ";";
        
        if (code.toLowerCase().indexOf("var value") != 0)
            code = importStr + "var value;" + "value=" + code;
        
        return code;
    }
    
    /**
     * Returns list of potential variables from the code.
     * 
     * @param code the code
     * @return the map of potential variables
     */
    public static LinkedHashMap<String, String> getVariables(String code)
    {
        LinkedHashMap<String, String> vars = new LinkedHashMap<String, String>();
        
        if (Utils.isNothing(code))
            return vars;
        
        Pattern pattern = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*",
                Pattern.CASE_INSENSITIVE);
        
        Matcher matcher = pattern.matcher(code);
        while (matcher.find())
        {
            String var = matcher.group(0);
            
            if (Utils.isNothing(var) || vars.containsKey(var))
                continue;
            
            vars.put(var, var);
        }
        
        pattern = Pattern.compile("(?<=\")(?:\\\\.|[^\"\\\\])*(?=\")",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(code);
        while (matcher.find())
        {
            String var = matcher.group(0);
            
            if (Utils.isNothing(var) || vars.containsKey(var))
                continue;
            
            vars.put(var, var);
        }
        
        return vars;
    }
    
    /**
     * Parses the functions.
     * 
     * <pre>
     * Example: sum(abc) + avg(xyz) --> com.toolsverse.function.Stat.sum(abc, context) + com.toolsverse.function.Stat.avg(xyz, context)
     * </pre>
     *
     * @param code the code
     * @param patterns the patterns
     * @return the the code with function calls replaced on patterns from the function patterns map
     */
    public static String parseFunctions(String code,
            final Map<String, String> patterns)
    {
        if (patterns == null || patterns.isEmpty() || Utils.isNothing(code))
            return code;
        
        for (final String name : patterns.keySet())
        {
            final String pattern = "(?i)" + name
                    + "\\(([ ,.a-zA-Z0-9\\?\\*\"']+)\\)";
            
            final String patt = patterns.get(name);
            
            final String fnc = patt.substring(0, patt.lastIndexOf("(")).trim();
            
            final String ps = patt.substring(patt.lastIndexOf("(") + 1,
                    patt.lastIndexOf(")"));
            
            final String[] fParamsPattern = !Utils.isNothing(ps) ? ps
                    .split(",") : null;
            
            code = new Rewriter(pattern)
            {
                @Override
                public String replacement()
                {
                    String params = group(1);
                    
                    String[] fParams = params.split(",", -1);
                    
                    String newParams = "";
                    
                    if (fParamsPattern != null)
                        for (int i = 0; i < fParamsPattern.length; i++)
                        {
                            String fParamPattern = fParamsPattern[i].trim();
                            
                            if (fParamPattern.charAt(0) == '?')
                            {
                                String paramPrefix = fParamPattern.length() == 1 ? ""
                                        : fParamPattern.substring(1);
                                
                                String parName = "";
                                
                                if (i < fParams.length)
                                    parName = fParams[i].trim();
                                else
                                    parName = paramPrefix
                                            + fParams[fParams.length - 1]
                                                    .trim();
                                
                                newParams = Utils.concat(newParams, parName,
                                        ",");
                            }
                            else
                                newParams = Utils.concat(newParams,
                                        fParamPattern, ",");
                        }
                    
                    return fnc + "(" + newParams + ")";
                }
            }.rewrite(code);
        }
        
        return code;
    }
    
    /**
     * Replaces SQL like boolean operations. 
     * 
     * <pre>
     * or  -> ||
     * and -> &&
     * not ->  !
     * ==  ->  =  
     * </pre>
     * 
     * @param code the code
     * @return translated code
     * 
     */
    public static String sql2Java(String code)
    {
        if (Utils.isNothing(code))
            return code;
        
        code = code.replaceAll("(?i)\\band\\b", "&&")
                .replaceAll("(?i)\\bor\\b", "||")
                .replaceAll("(?i)\\bnot\\b", "!").replaceAll("\\<>", "!=");
        
        return new Rewriter("\\=")
        {
            @Override
            public String replacement()
            {
                char last = buffer.charAt(buffer.length() - 1);
                
                if ('!' == last || '<' == last || '>' == last || '=' == last)
                    return "=";
                
                return "==";
            }
        }.rewrite(code);
    }
    
    /** The storage. */
    private Map<String, Object> _storage;
    
    /**
     * Instantiates new Script object.
     */
    public Script()
    {
        _storage = new ConcurrentHashMap<String, Object>();
    }
    
    /**
     * Clears the storage.
     */
    public void clear()
    {
        _storage.clear();
    }
    
    /**
     * Compiles script if it is possible.
     * 
     * @param objectStorage the storage for the compiled scripts
     * @param name the name of the script
     * @param code the script
     * @param lang the script language name
     * @return true if script was compiled 
     * @throws Exception in case of any error
     */
    public boolean compile(ObjectStorage objectStorage, String name,
            String code, String lang)
        throws Exception
    {
        if (objectStorage == null)
            objectStorage = this;
        
        CompiledScript script = (CompiledScript)objectStorage.getValue(name);
        
        if (script == null)
        {
            objectStorage.setValue("code" + name, code);
            
            ScriptEngine engine = (ScriptEngine)objectStorage
                    .getValue(ScriptEngine.class.getName() + lang);
            
            if (engine == null)
            {
                ScriptEngineManager manager = new ScriptEngineManager();
                
                engine = manager.getEngineByName(lang);
                
                objectStorage.setValue(ScriptEngine.class.getName() + lang,
                        engine);
            }
            
            if (engine instanceof Compilable)
            {
                Compilable compilingEngine = (Compilable)engine;
                
                script = compilingEngine.compile(code);
                
                objectStorage.setValue(name, script);
                
                return true;
            }
        }
        else
            return true;
        
        return false;
    }
    
    /**
     * Evaluates script. 
     * 
     * @param objectStorage the storage for the compiled scripts
     * @param bindings the bindings  
     * @param name the name of the script
     * @param lang the script language name 
     * 
     * @return the object returned by evaluated script 
     * 
     * @throws ScriptException in case of any error
     */
    public Object eval(ObjectStorage objectStorage, Bindings bindings,
            String name, String lang)
        throws ScriptException
    {
        if (objectStorage == null)
            objectStorage = this;
        
        CompiledScript script = (CompiledScript)objectStorage.getValue(name);
        
        if (script == null)
        {
            ScriptEngine engine = (ScriptEngine)objectStorage
                    .getValue(ScriptEngine.class.getName() + lang);
            
            String code = (String)objectStorage.getValue("code" + name);
            
            return engine.eval(code, bindings);
        }
        else
            return script.eval(bindings);
    }
    
    /**
     * Returns an uninitialized <code>Bindings</code> for the script engine defined by <code>lang</code>. 
     * 
     * @param objectStorage the object storage
     * @param lang the script language
     * @return bindings
     */
    public Bindings getBindings(ObjectStorage objectStorage, String lang)
    {
        if (objectStorage == null)
            objectStorage = this;
        
        ScriptEngine engine = (ScriptEngine)objectStorage
                .getValue(ScriptEngine.class.getName() + lang);
        
        if (engine == null)
            return null;
        
        return engine.createBindings();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getString(java.lang.String)
     */
    public String getString(String key)
    {
        Object value = _storage.get(key);
        
        return value != null ? value.toString() : null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getValue(java.lang.String)
     */
    public Object getValue(String key)
    {
        return _storage.get(key);
    }
    
    /**
     * Checks if code has been compiled.
     *
     * @param objectStorage the object storage
     * @param name the name
     * @return true, if successful
     */
    public boolean hasCompiledCode(ObjectStorage objectStorage, String name)
    {
        if (objectStorage == null)
            objectStorage = this;
        
        CompiledScript script = (CompiledScript)objectStorage.getValue(name);
        
        return script != null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#setValue(java.lang.String,
     * java.lang.Object)
     */
    public void setValue(String key, Object value)
    {
        if (key == null)
            return;
        
        _storage.put(key, value);
    }
}
