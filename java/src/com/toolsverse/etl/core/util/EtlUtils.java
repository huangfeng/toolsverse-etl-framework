/*
 * EtlUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Bindings;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConditionalExecution;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Script;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The collection of static methods used by ETL framework.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public final class EtlUtils
{
    
    /**
     * Creates a new variable and adds it to the given map.
     * 
     * @param vars
     *            the map of variables
     * @param name
     *            the name
     * @param defValue
     *            the default value
     * @param replace
     *            the "replace" flag. If true will replace existing variable
     *            with the same name.
     * @return the variable
     */
    public static Variable addVar(ListHashMap<String, Variable> vars,
            String name, String defValue, boolean replace)
    {
        Variable var = vars.get(name);
        if (var != null && replace)
            var.setValue(defValue);
        else if (var == null)
        {
            var = new Variable();
            var.setName(name);
            var.setValue(defValue);
            vars.put(var.getName(), var);
        }
        
        return var;
    }
    
    /**
     * Checks condition.
     * 
     * @param config
     *            the config
     * @param name
     *            the name
     * @param variables
     *            the variables
     * @param conditionalExecution
     *            the instance of the conditional execution class. Usually it is
     *            either Source, Destination or Scenario
     * @return true, if successful
     * @throws Exception
     *             in case of any error
     */
    public static boolean checkCondition(EtlConfig config, String name,
            ListHashMap<String, Variable> variables,
            ConditionalExecution conditionalExecution)
        throws Exception
    {
        if (Utils.isNothing(conditionalExecution.getConditionCode()))
            return true;
        
        if (Variable.DEFAULT_LANG.equalsIgnoreCase(conditionalExecution
                .getConditionLang()))
        {
            
            Connection con = null;
            
            if (Utils.isNothing(conditionalExecution
                    .getConditionConnectionName()))
                con = config.getConnectionFactory().getConnection(
                        conditionalExecution.getDefaultConnectionName());
            else
                con = config.getConnectionFactory().getConnection(
                        conditionalExecution.getConditionConnectionName());
            
            if (con == null)
            {
                Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                        EtlResource.CHECK_CONDITION_MSG.getValue(),
                        new String[] {name}));
                
                return false;
            }
            
            PreparedStatement st = null;
            ResultSet rs = null;
            Savepoint svp = null;
            
            try
            {
                st = con.prepareStatement(EtlUtils.mergeSqlWithVars(config,
                        conditionalExecution.getConditionCode(), null,
                        variables, null));
                
                svp = SqlUtils.getSavepoint(con);
                
                rs = st.executeQuery();
                
                if (rs.next())
                    return true;
            }
            catch (Exception ex)
            {
                if (svp != null)
                    con.rollback(svp);
                
                Logger.log(Logger.SEVERE, config,
                        Resource.ERROR_GENERAL.getValue(), ex);
            }
            finally
            {
                SqlUtils.cleanUpSQLData(st, rs, config);
            }
            
            Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                    EtlResource.CHECK_CONDITION_MSG.getValue(),
                    new String[] {name}));
            
            return false;
        }
        else
        {
            Script script = new Script();
            
            script.compile(config, name + "cond",
                    conditionalExecution.getConditionCode(),
                    conditionalExecution.getConditionLang());
            
            Bindings bindings = script.getBindings(config,
                    conditionalExecution.getConditionLang());
            
            bindings.put("variables", variables);
            bindings.put("etlConfig", config);
            bindings.put("name", name);
            bindings.put("self", config);
            
            Object ret = script.eval(config, bindings, name + "cond",
                    conditionalExecution.getConditionLang());
            
            if (ret instanceof Boolean)
                return (Boolean)ret;
            else if (ret instanceof String)
                return Utils.str2Boolean(ret.toString(), true);
            
            return false;
        }
    }
    
    /**
     * Gets the database id by parsing url.
     * 
     * @param alias
     *            the alias
     * @param delim
     *            the delimiter
     * @return the database id
     */
    public static String getDbId(Alias alias, String delim)
    {
        String server = Utils.getParam(alias.getParams(), Driver.DB_PARAM);
        
        if (!Utils.isNothing(server))
            return server;
        
        String url = alias.getUrl();
        
        if (Utils.isNothing(url))
            return url;
        
        String[] params = url.split(delim);
        
        return params[params.length - 1];
    }
    
    /**
     * Gets the file name. If original fileName has no extension adds ".xml".
     * 
     * @param fileName
     *            the file name
     * @return the file name
     */
    public static String getFileName(String fileName)
    {
        if (Utils.isNothing(fileName))
            return "unknown.xml";
        
        String path = FilenameUtils.getFullPath(fileName);
        
        String ext = FilenameUtils.getExtension(fileName);
        
        if (Utils.isNothing(ext))
        {
            if (Utils.isNothing(path))
                return fileName + ".xml";
            else
                return FilenameUtils.getBaseName(fileName) + ".xml";
        }
        
        if (Utils.isNothing(path))
            return fileName;
        else
            return FilenameUtils.getBaseName(fileName) + ".xml";
    }
    
    /**
     * Gets the pattern name. Example: test -> {test}
     * 
     * @param name
     *            the name
     * @return the pattern name
     */
    public static String getPatternName(String name)
    {
        if (Utils.isNothing(name))
            return null;
        
        return "{" + name + "}";
    }
    
    /**
     * Gets the scenario file name.
     * 
     * @param folder
     *            the folder
     * @param fileName
     *            the file name
     * @return the scenario file name
     */
    public static String getScenarioFileName(String folder, String fileName)
    {
        String path = FilenameUtils.getFullPath(fileName);
        
        String ext = FilenameUtils.getExtension(fileName);
        
        if (Utils.isNothing(ext))
        {
            if (Utils.isNothing(path))
                return folder + fileName + ".xml";
            else
                return fileName + ".xml";
        }
        
        if (Utils.isNothing(path))
            return folder + fileName;
        else
            return fileName;
    }
    
    /**
     * Gets the scenario name.
     * 
     * <p>
     * 
     * <pre>
     * a/b/c.txt --&gt; c                                                                                                                   
     * a.txt     --&gt; a                                                                                                                   
     * a/b/c     --&gt; c                                                                                                                   
     * a/b/c/    --&gt; &quot;&quot;
     * </pre>
     * 
     * @param name
     *            the name
     * @return the scenario name
     */
    public static String getScenarioName(String name)
    {
        return FilenameUtils.getBaseName(name);
    }
    
    /**
     * Gets the sql file name.
     * 
     * @param name
     *            the name
     * @param index
     *            the index of the file
     * @return the sql file name
     */
    public static String getSqlFileName(String name, int index)
    {
        if (Utils.isNothing(SystemConfig.instance().getScriptsFolder())
                || Utils.isNothing(name))
            return null;
        
        if (index < 0)
            return SystemConfig.instance().getScriptsFolder() + name + ".sql";
        else
            return SystemConfig.instance().getScriptsFolder() + name + "_"
                    + index + ".sql";
        
    }
    
    /**
     * Gets the variable value.
     * 
     * @param vars
     *            the variables
     * @param name
     *            the variable name
     * @param defValue
     *            the default value
     * @return the variable value
     */
    public static String getVarValue(ListHashMap<String, Variable> vars,
            String name, String defValue)
    {
        if (vars == null)
            return defValue;
        
        Variable var = vars.get(name);
        if (var != null && var.getValue() != null
                && var.getValue().length() > 0)
            return var.getValue();
        else
            return defValue;
    }
    
    /**
     * Converts "insert" sql to "update".
     * 
     * @param sql
     *            the sql
     * @param keyField
     *            the key field(s)
     * @return the update sql
     */
    public static String insert2Update(String sql, String keyField)
    {
        if (Utils.isNothing(sql) || Utils.isNothing(keyField))
            return null;
        
        String[] strs = Utils.getTokens(sql, '\n', 0);
        String str = null;
        String updateSql = "";
        String fieldName = null;
        int fieldIndex = 0;
        
        List<String> result = new ArrayList<String>();
        List<String> keyLines = new ArrayList<String>();
        List<String> keyFiledValues = new ArrayList<String>();
        
        String[] fields = Utils.getTokens(keyField, ',', 0);
        
        for (int i = 0; i < strs.length; i++)
        {
            str = strs[i];
            
            if (Utils.isNothing(str))
                continue;
            
            if (i == 0)
                str = str.replaceFirst("insert into ", "update ").replaceFirst(
                        "(".replaceAll("([^a-zA-z0-9])", "\\\\$1"), " set ");
            else if (str.indexOf(") values (") >= 0)
            {
                str = null;
                fieldIndex++;
            }
            else if (str.equals(")"))
                str = null;
            else if (fieldIndex == 0)
            {
                str = str.trim().replaceFirst(",", "");
            }
            else if (fieldIndex > 0)
            {
                str = str.trim();
                
                if (fieldIndex >= result.size())
                    break;
                
                fieldName = result.get(fieldIndex);
                
                boolean isKey = Utils.belongsTo(fields, fieldName);
                
                if (isKey)
                {
                    if (str.endsWith(","))
                        str = str.substring(0, str.length() - 1);
                    
                    keyFiledValues.add(str);
                    
                    str = fieldName;
                    
                    keyLines.add(fieldName + "=" + str);
                }
                
                result.set(fieldIndex, fieldName + "=" + str);
                str = null;
                fieldIndex++;
            }
            
            if (str != null)
                result.add(str);
        }
        
        for (int i = 0; i < result.size(); i++)
            if (!keyLines.contains(result.get(i)))
                updateSql = updateSql + result.get(i) + "\n";
        
        String whereSql = "where ";
        
        for (int i = 0; i < fields.length; i++)
        {
            if (i > 0)
                whereSql = whereSql + " and ";
            
            whereSql = whereSql + fields[i] + "=" + keyFiledValues.get(i);
        }
        
        updateSql = updateSql + whereSql;
        
        return updateSql;
    }
    
    /**
     * Checks if extract needs to be performed during execution of the etl
     * scenario.
     * 
     * @param action
     *            The action associated with the scenario.
     * 
     * 
     * @return true, if extract needs to be performed during execution of the
     *         etl scenario.
     */
    public static boolean isExtract(int action)
    {
        return action == EtlConfig.EXTRACT || action == EtlConfig.EXTRACT_LOAD;
    }
    
    /**
     * Checks if extract and load needs to be performed during execution of the
     * etl scenario.
     * 
     * @param action
     *            The action associated with the scenario.
     * 
     * 
     * @return true, if extract and load needs to be performed during execution
     *         of the etl scenario.
     */
    public static boolean isExtractLoad(int action)
    {
        return action == EtlConfig.EXTRACT_LOAD;
    }
    
    /**
     * Checks if load needs to be performed during execution of the etl
     * scenario.
     * 
     * @param action
     *            The action associated with the scenario.
     * 
     * 
     * @return true, if load needs to be performed during execution of the etl
     *         scenario.
     */
    public static boolean isLoad(int action)
    {
        return action == EtlConfig.LOAD || action == EtlConfig.EXTRACT_LOAD;
    }
    
    /**
     * Merges sql with variables.
     * 
     * @param cacheProvider
     *            the cache provider
     * @param sql
     *            the sql
     * @param using
     *            the comma delimited string with the names of bind variables.
     * @param variables
     *            the variables
     * @param bindVars
     *            the bind variables
     * @return the sql
     */
    public static String mergeSqlWithVars(
            CacheProvider<String, Object> cacheProvider, String sql,
            String using, ListHashMap<String, Variable> variables,
            Map<String, Object> bindVars)
    {
        String[] usingVars = null;
        
        if (!Utils.isNothing(using))
            usingVars = using.split(",");
        
        if (variables != null && variables.size() > 0)
        {
            String key = null;
            
            boolean isBind = bindVars != null && usingVars != null
                    && usingVars.length > 0;
            
            for (int i = 0; i < variables.size(); i++)
            {
                Variable var = variables.get(i);
                String value = var.getValue();
                
                if (cacheProvider.getCache() != null)
                    key = (String)cacheProvider.getCache().get(var.getName());
                
                if (!Utils.isNothing(key))
                    value = key;
                
                if (value != null && !"null".equalsIgnoreCase(value))
                {
                    if (isBind && var.getName() != null)
                    {
                        if (SqlUtils.addBindVar(usingVars, var.getName(),
                                value, bindVars))
                            sql = Utils.findAndReplace(sql, "{" + var.getName()
                                    + "}", "?", true);
                        else
                            sql = Utils.findAndReplace(sql, "{" + var.getName()
                                    + "}", value, true);
                    }
                    else
                        sql = Utils.findAndReplace(sql, "{" + var.getName()
                                + "}", value, true);
                }
                else
                {
                    if (isBind && var.getName() != null)
                    {
                        if (SqlUtils.addBindVar(usingVars, var.getName(),
                                value, bindVars))
                            sql = Utils.findAndReplace(sql, "{" + var.getName()
                                    + "}", "?", true);
                        else
                            sql = Utils.findAndReplace(sql, "{" + var.getName()
                                    + "}", "null", true);
                    }
                    else
                        sql = Utils.findAndReplace(sql, "{" + var.getName()
                                + "}", "null", true);
                }
            }
        }
        
        return sql;
    }
    
    /**
     * Merges sql with variables.
     *
     * @param sql the sql
     * @param vars the variables
     * @param driver the driver
     * @return the string
     */
    public static String mergeSqlWithVars(String sql,
            ListHashMap<String, Variable> vars, Driver driver)
    {
        if (Utils.isNothing(sql) || vars == null || vars.size() == 0
                || driver == null)
            return sql;
        
        for (Variable var : vars.getList())
        {
            String find = getPatternName(var.getName());
            
            sql = Utils.findAndReplace(sql, find,
                    driver.getVarName(var.getName()), true);
        }
        
        return sql;
    }
    
    /**
     * Merges string with parameters.
     * 
     * @param input
     *            the input
     * @param params
     *            the parameters
     * @return the string
     */
    public static String mergeStringWithParams(String input, String[] params)
    {
        if (input == null || params == null || params.length == 0)
            return input;
        
        for (int i = 0; i < params.length; i++)
        {
            if (params[i] == null)
                continue;
            
            input = input.replaceAll("%" + (i + 1), params[i]);
        }
        
        return input;
    }
    
    /**
     * Parses exception. Returns line number in the sql which contains an error.
     * 
     * @param exception
     *            the exception
     * @param code
     *            the sql
     * @param errorLinePattern
     *            the error line pattern
     * @return the line number in the sql which contains an error
     */
    public static int parseException(Exception exception, String code,
            String errorLinePattern)
    {
        String message = exception != null ? exception.getMessage() : null;
        
        if (Utils.isNothing(code) || Utils.isNothing(errorLinePattern)
                || Utils.isNothing(message))
            return -1;
        
        try
        {
            if (message.indexOf(errorLinePattern) >= 0)
            {
                int pos = message.indexOf(errorLinePattern)
                        + errorLinePattern.length();
                
                String errorStr = message.substring(pos).trim();
                
                if (!Utils.isNothing(errorStr))
                {
                    Pattern pattern = Pattern.compile("\\d+");
                    
                    Matcher m = pattern.matcher(errorStr);
                    
                    if (m.find())
                    {
                        String lineStr = m.group();
                        
                        if (!Utils.isNothing(lineStr))
                            return Integer.parseInt(lineStr);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            return -1;
        }
        
        return -1;
    }
    
    /**
     * Substitute variable values in {} brackets.
     *
     * @param variables the variables
     */
    public static void substituteVars(ListHashMap<String, Variable> variables)
    {
        if (variables == null || variables.size() == 0)
            return;
        
        for (int i = 0; i < variables.size(); i++)
        {
            Variable var = variables.get(i);
            
            String value = var.getValue();
            
            String original = ("{" + var.getName() + "}").toLowerCase();
            
            for (int j = i + 1; j < variables.size(); j++)
            {
                Variable toVar = variables.get(j);
                
                String toValue = toVar.getValue();
                String toSql = toVar.getCode();
                
                if (toValue != null
                        && toValue.toLowerCase().indexOf(original) >= 0)
                {
                    toValue = Utils.findAndReplace(toValue, original,
                            Utils.makeString(value), true);
                    
                    toVar.setValue(toValue);
                }
                
                if (toSql != null && toSql.toLowerCase().indexOf(original) >= 0)
                {
                    toSql = Utils.findAndReplace(toSql, original,
                            Utils.makeString(value), true);
                    
                    toVar.setCode(toSql);
                }
            }
        }
    }
    
}