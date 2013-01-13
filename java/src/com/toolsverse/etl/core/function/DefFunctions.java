/*
 * DefFunctions.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.function;

import java.util.HashMap;
import java.util.Map;

import javax.script.Bindings;

import com.toolsverse.cache.Cache;
import com.toolsverse.cache.CacheProvider;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Function;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.engine.Destination;
import com.toolsverse.etl.core.engine.LoadFunctionContext;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Script;
import com.toolsverse.util.Utils;

/**
 * The default library of functions. There could be database specific overrides
 * such as OracleFunctions, MsSqlFunctions, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DefFunctions implements Function
{
    
    /** The DEFAULT_FUNCTION. */
    public static final String DEFAULT_FUNCTION = "getValue";
    
    /** The SCRIPT_FUNCTION. */
    public static final String SCRIPT_FUNCTION = "script";
    
    /** The functions executed before "load" code. */
    private static final String[] BEFORE_FUNCTIONS = {"getPk", "getSeq",
            "getFk", DEFAULT_FUNCTION, "getVariableValue", SCRIPT_FUNCTION,
            "getGlobalVarValue", "assignVar"};
    
    /** The functions executed for each row. */
    private static final String[] RUNTIME_FUNCTIONS = BEFORE_FUNCTIONS;
    
    /** The CURRENT KEY. */
    private static final String CURRENT_KEY = "CURRENT_KEY";
    
    /** The START ATTRIBUTE. */
    private static final String START_ATTR = "start";
    
    /** The START ATTRIBUTE. */
    private static final String SCENARIO_VARIABLE_ATTR = "global";
    
    /**
     * Assigns value to variable.
     * 
     * @param context
     *            the context
     * @return the string
     * @throws Exception
     *             in case of any error
     */
    public String assignVar(LoadFunctionContext context)
        throws Exception
    {
        return assignVar(context, getVariableValue(context));
    }
    
    /**
     * Assigns value to variable.
     * 
     * @param context
     *            the context
     * @param value
     *            the value
     * @return the string
     * @throws Exception
     *             the exception
     */
    public String assignVar(LoadFunctionContext context, String value)
        throws Exception
    {
        return value != null ? value : "NULL";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.Function#getAfterFunctions()
     */
    public String[] getAfterFunctions()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.Function#getBeforeFunctions()
     */
    public String[] getBeforeFunctions()
    {
        return BEFORE_FUNCTIONS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.Function#getExcludeFunctions()
     */
    public String[] getExcludeFunctions()
    {
        return null;
    }
    
    /**
     * Gets the new value for the field from the corresponding "primary key" field.
     * 
     * @param context
     *            the context
     * @return the foreign key
     * @throws Exception
     *             in case of any error
     */
    public String getFk(LoadFunctionContext context)
        throws Exception
    {
        Variable var = context.getVariable();
        
        CacheProvider<String, Object> cacheProvider = var
                .getLinkedCacheProvider();
        
        if (cacheProvider == null)
            return null;
        
        if (context.getScope() == Variable.EXECUTE_BEFORE)
            return "getFk(" + getValue(context) + ")";
        else if (context.getScope() == Variable.EXECUTE_RUNTIME)
        {
            Map<Object, String> keys = getKeys(var.getEvalName(), cacheProvider);
            
            String key = keys.get(context.getCurrentValue());
            
            if (key == null)
                key = "NULL";
            
            return key;
        }
        else
            return "";
    }
    
    /**
     * Gets the global variable value.
     * 
     * @param context
     *            the context
     * @return the global variable value
     * @throws Exception
     *             in case of any error
     */
    public String getGlobalVarValue(LoadFunctionContext context)
        throws Exception
    {
        Destination dest = context.getDestination();
        Variable var = context.getVariable();
        
        String name = var.getAttrValue(SCENARIO_VARIABLE_ATTR);
        
        if (Utils.isNothing(name))
            name = var.getParam();
        
        if (dest == null || dest.getScenarioVariables() == null
                || dest.getScenarioVariables().size() == 0
                || Utils.isNothing(name))
            return "";
        else
        {
            Variable gvar = dest.getScenarioVariables().get(name);
            
            return gvar != null ? gvar.getValue() : "";
        }
    }
    
    /**
     * Gets the keys.
     *
     * @param key the key
     * @param cacheProvider the cache provider
     * @return the keys
     * @throws Exception in case of any error
     */
    @SuppressWarnings("unchecked")
    private Map<Object, String> getKeys(String key,
            CacheProvider<String, Object> cacheProvider)
        throws Exception
    {
        Cache<String, Object> cache = cacheProvider.getCache();
        
        if (cache == null)
            return null;
        
        Map<Object, String> keys = (Map<Object, String>)cache.get(key);
        
        if (keys == null)
            keys = (Map<Object, String>)cache.put(key,
                    new HashMap<Object, String>());
        
        return keys;
        
    }
    
    /**
     * Gets the new value for the field using in-memory map or other mechanism, for example db sequence.
     * 
     * @param context
     *            the context
     * @return the primary key
     * @throws Exception
     *             in case of any error
     */
    public String getPk(LoadFunctionContext context)
        throws Exception
    {
        Destination dest = context.getDestination();
        Variable var = context.getVariable();
        
        if (dest == null)
            return "";
        
        if (context.getScope() == Variable.EXECUTE_BEFORE)
            return "getPk(" + getValue(context) + ")";
        else if (context.getScope() == Variable.EXECUTE_RUNTIME)
        {
            Map<Object, String> keys = getKeys(var.getName(), dest);
            
            String start = var.getAttrValue(START_ATTR);
            
            boolean hasStart = !Utils.isNothing(start);
            
            if (!hasStart)
                start = var.getParam();
            
            String key = null;
            if (Utils.isNothing(start))
            {
                key = (String)dest.getCache().get(var.getName() + CURRENT_KEY);
                
                if (key == null)
                    key = "0";
                
                key = String.valueOf(Long.parseLong(key) + 1);
            }
            else
            {
                key = String.valueOf(Long.parseLong(start));
                
                if (hasStart)
                    var.clearAttr(START_ATTR);
                else
                    var.setParam(null);
            }
            
            dest.getCache().put(var.getName() + CURRENT_KEY, key);
            
            keys.put(context.getCurrentValue(), key);
            
            return key;
        }
        else
            return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.common.Function#getRuntimeFunctions()
     */
    public String[] getRuntimeFunctions()
    {
        return RUNTIME_FUNCTIONS;
    }
    
    /**
     * Gets the new value for the field using sequence. Synonym for getPk.
     *
     * @param context the context
     * @return the pk
     * @throws Exception the exception in case of any error
     */
    public String getSeq(LoadFunctionContext context)
        throws Exception
    {
        return getPk(context);
    }
    
    /**
     * Gets the sql.
     * 
     * @param sql
     *            the sql
     * @return the sql
     */
    protected String getSql(String sql)
    {
        if (sql == null)
            return "";
        
        sql = sql.trim();
        if (sql.lastIndexOf(";") == sql.length() - 1)
            sql = sql.substring(0, sql.length() - 1);
        
        return sql;
    }
    
    /**
     * Gets the current field value.
     * 
     * @param context
     *            the context
     * @return the current field value
     * @throws Exception
     *             in case of any error
     */
    public String getValue(LoadFunctionContext context)
        throws Exception
    {
        DataSet dataSet = context.getDestination().getDataSet();
        Variable var = context.getVariable();
        String fieldName = var.getName();
        DataSetRecord currentRow = context.getCurrentRecord();
        String value = "NULL";
        
        FieldDef fieldDef = dataSet.getFieldDef(fieldName);
        
        int fieldType = fieldDef.getSqlDataType();
        Object fieldValue = dataSet.getFieldValue(currentRow, fieldName);
        
        if (fieldValue != null)
            value = dataSet.getDriver().convertValueForStorage(fieldValue,
                    fieldType, context.isFromCursor());
        
        return value;
    }
    
    /**
     * Gets the variable value from the current field value.
     * 
     * @param context
     *            the context
     * @return the variable value
     * @throws Exception
     *             in case of any error
     */
    public String getVariableValue(LoadFunctionContext context)
        throws Exception
    {
        DataSet dataSet = context.getDestination().getDataSet();
        Variable var = context.getVariable();
        String varName = var.getEvalName();
        String value = "NULL";
        
        FieldDef fieldDef = dataSet.getFieldDef(varName);
        
        if (fieldDef == null)
            return null;
        
        Object oldValue = dataSet.getFieldValue(context.getCurrentRecord(),
                varName);
        try
        {
            value = dataSet.getDriver().convertValueForStorage(oldValue,
                    fieldDef.getSqlDataType(), context.isFromCursor());
        }
        catch (Exception ex)
        {
            if (oldValue == null)
                value = "NULL";
            else
                throw ex;
        }
        
        return value;
    }
    
    /**
     * Gets the sql associated with the variable.
     * 
     * @param var
     *            the variable
     * @param driver
     *            the driver
     * @return the sql associated with the variable.
     */
    protected String getVarSql(ListHashMap<String, Variable> variables,
            Variable var, Driver driver)
    {
        if (variables == null || variables.size() == 0)
            return var.getCode();
        
        String ret = var.getCode();
        
        for (Variable variable : variables.getList())
        {
            ret = Utils.findAndReplace(ret, "{" + variable.getName() + "}",
                    driver.getVarName(variable.getName()), true);
        }
        
        return ret;
    }
    
    /**
     * Gets the sql associated with the variable.
     * 
     * @param set
     *            the "set" token
     * @param varName
     *            the variable name
     * @param sql
     *            the sql
     * @param pattern
     *            the pattern
     * @return the sql associated with the variable
     */
    protected String getVarSql(String set, String varName, String sql,
            String pattern)
    {
        sql = getSql(sql);
        
        if (sql.toLowerCase().indexOf((set + varName).toLowerCase()) >= 0)
            return sql;
        else
            return set + varName + pattern + sql;
    }
    
    /**
     * Executes script (typically JavaScript).
     * 
     * @param context
     *            the context
     * @return the string
     * @throws Exception
     *             in case of any error
     */
    public String script(LoadFunctionContext context)
        throws Exception
    {
        Variable var = context.getVariable();
        
        DataSet dataSet = context.getDestination().getDataSet();
        
        if (!Utils.isNothing(var.getCode()))
        {
            String code = getVarSql(context.getDestination().getVariables(),
                    var, dataSet.getDriver());
            
            if (Variable.DEFAULT_LANG.equalsIgnoreCase(var.getLang()))
            {
                String varValue = "";
                
                if (!context.isFromCursor())
                    varValue = assignVar(context);
                
                return varValue + "  " + code + "\n";
            }
            else
            {
                DataSetRecord currentRow = context.getCurrentRecord();
                String fieldName = var.getName();
                Object fieldValue = dataSet
                        .getFieldValue(currentRow, fieldName);
                
                Script script = new Script();
                
                script.compile(context.getConfig(), context.getDestination()
                        .getName() + var.getName(), code, var.getLang());
                
                Bindings bindings = script.getBindings(context.getConfig(),
                        var.getLang());
                
                bindings.put("fieldValue", fieldValue);
                bindings.put("variable", var);
                bindings.put("currentRow", currentRow);
                bindings.put("dataSet", dataSet);
                bindings.put("etlConfig", context.getConfig());
                bindings.put("destination", context.getDestination());
                bindings.put("row", context.getRow());
                
                Object ret = script.eval(context.getConfig(), bindings, context
                        .getDestination().getName() + var.getName(),
                        var.getLang());
                
                if (ret != null)
                    return assignVar(context, ret.toString());
                else
                    return getValue(context);
            }
        }
        else
            return "\n";
    }
    
}
