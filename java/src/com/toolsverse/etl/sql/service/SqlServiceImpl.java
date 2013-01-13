/*
 * SqlServiceImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.metadata.GenericMetadataExtractor;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.SessionConnectionProvider;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.Resource;
import com.toolsverse.storage.StorageManager;
import com.toolsverse.storage.StorageProvider;
import com.toolsverse.storage.StorageProvider.StorageObject;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation for the SqlService interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlServiceImpl implements SqlService
{
    
    // keys used by object storge
    
    /** The current statement. */
    private static final String CURRENT_ST_KEY = "currentst";
    
    /** The current result set. */
    private static final String CURRENT_RS_KEY = "currentrs";
    
    /** The current external process. */
    private static final String CURRENT_PROCESS_KEY = "currentprocess";
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#cancel(com.toolsverse.etl.sql
     * .connection.SessionConnectionProvider)
     */
    public void cancel(SessionConnectionProvider<Alias> provider)
    {
        provider = getConnectionProvider(provider);
        
        final Statement statement = getCurrentStatement();
        final ResultSet resultSet = getCurrentResultSet();
        final Process process = getCurrentProcess();
        
        try
        {
            
            if (process != null)
                process.destroy();
            
            if (statement != null)
            {
                Thread cancelThread = new Thread(new Runnable()
                {
                    public void run()
                    {
                        try
                        {
                            statement.cancel();
                        }
                        catch (Exception ex)
                        {
                            Logger.log(Logger.INFO, this,
                                    Resource.ERROR_GENERAL.getValue(), ex);
                        }
                        finally
                        {
                            SqlUtils.cleanUpSQLData(statement, resultSet, this);
                        }
                        
                    }
                });
                
                cancelThread.start();
                
            }
        }
        finally
        {
            clear();
        }
    }
    
    /**
     * Sets current statement, external process, and result set to null.
     */
    public void clear()
    {
        setCurrentStatement(null);
        setCurrentResultSet(null);
        setCurrentProcess(null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#commitTransaction(com.toolsverse
     * .etl.sql.connection.SessionConnectionProvider,
     * com.toolsverse.etl.common.Alias, boolean)
     */
    public void commitTransaction(SessionConnectionProvider<Alias> provider,
            Alias params, boolean closeOnCommit)
        throws Exception
    {
        provider = getConnectionProvider(provider);
        
        Connection con = provider.getConnection(params, false);
        
        if (con != null)
        {
            con.commit();
            
            if (closeOnCommit)
                releaseConnection(provider, params);
        }
    }
    
    /**
     * Executes prepared statement.
     * 
     * @param st
     *            the prepared statement
     * @return the type of the sql (select, script, or dml)
     * @throws Exception
     *             in case of any error
     */
    private int execute(PreparedStatement st)
        throws Exception
    {
        if (st.execute())
            return SELECT_TYPE;
        else if (st instanceof CallableStatement)
            return EXECUTE_SCRIPT_TYPE;
        else
            return DML_TYPE;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#executeExplainPlan(com.toolsverse
     * .etl.sql.service.SqlRequest)
     */
    public SqlResult executeExplainPlan(SqlRequest sqlRequest)
        throws Exception
    {
        String result = "";
        Connection con = null;
        
        String sql = null;
        
        String[] stmts = sqlRequest.getParser().split(
                sqlRequest.getParser().removeChars(sqlRequest.getSql()));
        
        for (int i = 0; i < stmts.length; i++)
        {
            sql = stmts[i];
            
            if (!Utils.isNothing(sql))
                break;
        }
        
        if (Utils.isNothing(sql))
            return null;
        
        sql = sqlRequest.getDriver().getSqlForExplainPlan(sql,
                sqlRequest.getParser());
        
        LinkedHashMap<String, DataSet> models = new LinkedHashMap<String, DataSet>();
        
        SessionConnectionProvider<Alias> provider = getConnectionProvider(sqlRequest);
        
        Savepoint svp = null;
        
        try
        {
            con = provider.getConnection(sqlRequest
                    .getConnectionParamsProvider().getConnectionParams());
            
            if (sqlRequest.getDriver().requiresRollbackAfterSqlError())
                svp = SqlUtils.getSavepoint(con);
            
            Object res = sqlRequest.getDriver().getExplainPlan(
                    sqlRequest,
                    con,
                    sqlRequest.getConnectionParamsProvider()
                            .getConnectionParams(), sql);
            
            if (res instanceof String)
                result = (String)res;
            else if (res instanceof DataSet)
                models.put("Explain Plan", (DataSet)res);
            else if (res != null)
                result = res.toString();
            
        }
        catch (Exception ex)
        {
            if (svp != null)
                con.rollback(svp);
            
            result = updateResult(EXCEPTION_TYPE, result, ex.toString());
            
            SqlResult sqlResult = new SqlResult(result, null);
            
            sqlResult.setException(ex);
            
            return sqlResult;
        }
        finally
        {
            if (sqlRequest.isClose() && con != null)
                releaseConnection(provider, sqlRequest
                        .getConnectionParamsProvider().getConnectionParams());
        }
        
        return new SqlResult(models.size() == 0 ? result : null,
                models.size() != 0 ? models : null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#executeExternal(com.toolsverse
     * .etl.sql.service.SqlRequest)
     */
    public SqlResult executeExternal(SqlRequest sqlRequest)
        throws Exception
    {
        String result = "";
        
        SystemConfig systemConfig = SystemConfig.instance();
        
        String dataFolder = systemConfig.getScriptsFolder();
        
        if (Utils.isNothing(dataFolder))
            return null;
        
        dataFolder = FileUtils.getUnixFolderName(dataFolder);
        
        String cmdExt = Utils.getShellExt(systemConfig
                .getSystemProperty(SystemConfig.SHELL_EXT_PROP));
        
        String sqlFile = dataFolder + Utils.getUUIDName() + "_script.sql";
        String cmdFileName = dataFolder + Utils.getUUIDName() + "_script"
                + cmdExt;
        
        String sqlToRun = sqlRequest.getSql();
        
        if (sqlRequest.hasParams())
            sqlToRun = sqlRequest.getParser().setBindVariables(sqlToRun,
                    sqlRequest.getProperties());
        
        File file = new File(sqlFile);
        file.delete();
        
        final File cmdFile = new File(cmdFileName);
        cmdFile.delete();
        
        Alias alias = sqlRequest.getConnectionParamsProvider()
                .getConnectionParams();
        
        String cmd = sqlRequest.getDriver().getCmdForExternalTool(sqlRequest,
                alias, sqlFile);
        
        if (Utils.isNothing(cmd))
            return null;
        
        try
        {
            Writer output = null;
            try
            {
                output = new BufferedWriter(new FileWriter(file));
                
                output.write(sqlRequest.getDriver().getSqlForExternalTool(
                        alias, sqlToRun, sqlRequest.getParser()));
            }
            finally
            {
                if (output != null)
                    output.close();
                output = null;
            }
            
            try
            {
                output = new BufferedWriter(new FileWriter(cmdFile));
                output.write(cmd);
            }
            finally
            {
                if (output != null)
                    output.close();
            }
            
            BufferedReader outputReader = null;
            
            try
            {
                try
                {
                    Process process = Utils.execScript(cmdFileName);
                    
                    setCurrentProcess(process);
                    
                    outputReader = new BufferedReader(new InputStreamReader(
                            process.getInputStream()));
                    
                    String currentLine = null;
                    
                    while ((currentLine = outputReader.readLine()) != null)
                    {
                        if (Utils.isNothing(currentLine)
                                || currentLine.indexOf(sqlFile) > 0)
                            continue;
                        
                        result = result + currentLine + "\n";
                    }
                    
                    if (Utils.isNothing(result))
                    {
                        BufferedReader error = null;
                        
                        try
                        {
                            error = new BufferedReader(new InputStreamReader(
                                    process.getErrorStream()));
                            
                            while ((currentLine = error.readLine()) != null)
                            {
                                if (Utils.isNothing(currentLine)
                                        || currentLine.indexOf(sqlFile) > 0)
                                    continue;
                                
                                result = result + currentLine + "\n";
                            }
                            
                        }
                        finally
                        {
                            if (error != null)
                                error.close();
                            
                        }
                    }
                    
                    if (process != null)
                        process.waitFor();
                }
                finally
                {
                    file.delete();
                    
                    cmdFile.delete();
                }
            }
            finally
            {
                if (outputReader != null)
                    outputReader.close();
            }
            
        }
        catch (Exception ex)
        {
            result = updateResult(EXCEPTION_TYPE, result, ex.toString());
            
            SqlResult sqlResult = new SqlResult(result, null);
            
            sqlResult.setException(ex);
            
            return sqlResult;
        }
        
        return new SqlResult(result, null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#executeScript(com.toolsverse
     * .etl.sql.service.SqlRequest)
     */
    public SqlResult executeScript(SqlRequest sqlRequest)
        throws Exception
    {
        Map<String, List<Integer>> paramMap = new HashMap<String, List<Integer>>();
        
        List<String> outpurParams = new ArrayList<String>();
        
        String sqlToRun = toSql(sqlRequest.getSql(), paramMap, outpurParams,
                sqlRequest);
        
        CallableStatement st = null;
        ResultSet rs = null;
        Connection con = null;
        
        String result = "";
        LinkedHashMap<String, DataSet> models = new LinkedHashMap<String, DataSet>();
        int type = NOTHING_TYPE;
        boolean executed = false;
        
        SessionConnectionProvider<Alias> provider = getConnectionProvider(sqlRequest);
        
        Savepoint svp = null;
        
        try
        {
            con = provider.getConnection(sqlRequest
                    .getConnectionParamsProvider().getConnectionParams());
            
            rs = null;
            
            st = con.prepareCall(sqlToRun);
            
            if (sqlRequest.getMaxRows() >= 0)
                st.setMaxRows(sqlRequest.getMaxRows());
            
            setCurrentStatement(st);
            
            // sql has bind variables
            if (paramMap.size() > 0)
                sqlRequest.getParser().setBindVariables(st,
                        sqlRequest.getProperties(), paramMap, outpurParams,
                        sqlRequest.getDriver());
            
            if (sqlRequest.getDriver().requiresRollbackAfterSqlError())
                svp = SqlUtils.getSavepoint(con);
            
            type = execute(st);
            
            executed = true;
            
            if (type == SELECT_TYPE)
            {
                boolean hasMore = true;
                int ind = 1;
                
                while (hasMore)
                {
                    rs = st.getResultSet();
                    
                    setCurrentResultSet(rs);
                    
                    DataSet dataSet = new DataSet();
                    
                    SqlUtils.populateDataSet(dataSet, sqlRequest.getDriver(),
                            rs, null, false, false);
                    
                    models.put(
                            EtlResource.DATASET_MSG.getValue() + " " + ind++,
                            dataSet);
                    
                    rs = null;
                    
                    hasMore = st.getMoreResults();
                }
            }
            else
            {
                SQLWarning warning = st.getWarnings();
                while (warning != null)
                {
                    result = result + warning.getLocalizedMessage();
                    warning = warning.getNextWarning();
                }
                
                result = updateResult(type, result, "");
                
                if (outpurParams != null && outpurParams.size() > 0
                        && paramMap != null && paramMap.size() > 0)
                    for (int i = 0; i < outpurParams.size(); i++)
                    {
                        String name = outpurParams.get(i);
                        
                        Variable var = sqlRequest.getParser().getVariable(name,
                                sqlRequest.getDriver());
                        
                        if (var == null)
                            continue;
                        
                        List<Integer> indexes = paramMap.get(name);
                        
                        if (indexes == null || indexes.size() == 0)
                            continue;
                        
                        for (int j = 0; j < indexes.size(); j++)
                        {
                            int index = ((Number)indexes.get(j)).intValue();
                            
                            Object value = st.getObject(index);
                            
                            if (value == null)
                                result = updateResult(OUTPUT_PARAM_TYPE,
                                        result, var.getName() + " = null");
                            else if (value instanceof ResultSet)
                            {
                                rs = (ResultSet)value;
                                
                                setCurrentResultSet(rs);
                                
                                DataSet dataSet = new DataSet();
                                
                                SqlUtils.populateDataSet(dataSet,
                                        sqlRequest.getDriver(), rs, null,
                                        false, false);
                                
                                models.put(var.getName(), dataSet);
                                
                                rs = null;
                            }
                            else
                                result = updateResult(OUTPUT_PARAM_TYPE,
                                        result, var.getName() + " = " + value);
                        }
                        
                    }
            }
            
            if (sqlRequest.isCommit() && executed && con != null)
                try
                {
                    con.commit();
                }
                catch (Exception e)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_GENERAL.getValue(), e);
                }
            
        }
        catch (Exception ex)
        {
            if (sqlRequest.isCommit() && executed && con != null)
                try
                {
                    con.rollback();
                }
                catch (Exception e)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_GENERAL.getValue(), e);
                }
            else if (svp != null)
            {
                con.rollback(svp);
            }
            
            result = updateResult(EXCEPTION_TYPE, result, ex.toString());
            
            SqlResult sqlResult = new SqlResult(result, null, false, executed);
            
            sqlResult.setException(ex);
            
            return sqlResult;
        }
        finally
        {
            SqlUtils.cleanUpSQLData(st, null, this);
            
            if (sqlRequest.isClose() && con != null)
                releaseConnection(provider, sqlRequest
                        .getConnectionParamsProvider().getConnectionParams());
        }
        
        return new SqlResult(result, models, executed, executed);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#executeScript(com.toolsverse
     * .etl.sql.service.SqlRequest)
     */
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#executeSql(com.toolsverse.etl
     * .sql.service.SqlRequest)
     */
    public SqlResult executeSql(SqlRequest sqlRequest)
        throws Exception
    {
        if (sqlRequest.getSql() == null)
            return null;
        
        String result = "";
        LinkedHashMap<String, DataSet> models = new LinkedHashMap<String, DataSet>();
        
        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        
        String[] stmts = sqlRequest.getParser().split(sqlRequest.getSql());
        
        int type = NOTHING_TYPE;
        int index = 1;
        
        boolean executed = false;
        
        SessionConnectionProvider<Alias> provider = getConnectionProvider(sqlRequest);
        
        Savepoint svp = null;
        
        try
        {
            con = provider.getConnection(sqlRequest
                    .getConnectionParamsProvider().getConnectionParams());
            
            for (int i = 0; i < stmts.length; i++)
            {
                rs = null;
                
                try
                {
                    Map<String, List<Integer>> paramMap = new HashMap<String, List<Integer>>();
                    
                    String sqlToRun = toSql(stmts[i], paramMap, null,
                            sqlRequest);
                    
                    if (Utils.isNothing(sqlToRun))
                        continue;
                    
                    st = con.prepareStatement(sqlToRun);
                    
                    if (sqlRequest.getMaxRows() >= 0)
                        st.setMaxRows(sqlRequest.getMaxRows());
                    
                    setCurrentStatement(st);
                    
                    // sql has bind variables
                    if (paramMap.size() > 0)
                        sqlRequest.getParser().setBindVariables(st,
                                sqlRequest.getProperties(), paramMap, null,
                                sqlRequest.getDriver());
                    
                    svp = null;
                    
                    if (sqlRequest.getDriver().requiresRollbackAfterSqlError())
                        svp = SqlUtils.getSavepoint(con);
                    
                    type = execute(st);
                    
                    executed = true;
                    
                    if (type == SELECT_TYPE)
                    {
                        rs = st.getResultSet();
                        
                        setCurrentResultSet(rs);
                        
                        DataSet dataSet = new DataSet();
                        
                        SqlUtils.populateDataSet(
                                dataSet,
                                sqlRequest.getDriver(),
                                rs,
                                null,
                                false,
                                false,
                                null,
                                null,
                                sqlRequest.getMaxRows() >= 0 ? sqlRequest
                                        .getMaxRows() : -1);
                        
                        models.put(EtlResource.DATASET_MSG.getValue() + index,
                                dataSet);
                        
                        rs = null;
                        
                        index++;
                    }
                    else
                        result = updateResult(type, result,
                                String.valueOf(st.getUpdateCount()));
                }
                finally
                {
                    SqlUtils.cleanUpSQLData(st, rs, this);
                }
            }
            
            if (sqlRequest.isCommit() && executed && con != null)
                try
                {
                    con.commit();
                }
                catch (Exception e)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_GENERAL.getValue(), e);
                }
            
        }
        catch (Exception ex)
        {
            if (sqlRequest.isCommit() && executed && con != null)
                try
                {
                    con.rollback();
                }
                catch (Exception e)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_GENERAL.getValue(), e);
                }
            else if (svp != null)
                con.rollback(svp);
            
            result = updateResult(EXCEPTION_TYPE, result, ex.toString());
            
            SqlResult sqlResult = new SqlResult(result, null, false, executed);
            
            sqlResult.setException(ex);
            
            return sqlResult;
        }
        finally
        {
            if (sqlRequest.isClose() && con != null)
                releaseConnection(provider, sqlRequest
                        .getConnectionParamsProvider().getConnectionParams());
        }
        
        return new SqlResult(result, models, executed, executed);
    }
    
    /**
     * Gets the connection provider.
     * 
     * @param provider
     *            the connection provider
     * @return the connection provider
     */
    @SuppressWarnings("unchecked")
    private SessionConnectionProvider<Alias> getConnectionProvider(
            SessionConnectionProvider<Alias> provider)
    {
        if (provider == null)
            provider = (SessionConnectionProvider<Alias>)ObjectFactory
                    .instance().get(SessionConnectionProvider.class.getName(),
                            true);
        
        return provider;
    }
    
    /**
     * Gets the connection provider.
     * 
     * @param sqlRequest
     *            the sql request
     * @return the connection provider
     */
    private SessionConnectionProvider<Alias> getConnectionProvider(
            SqlRequest sqlRequest)
    {
        return getConnectionProvider(sqlRequest.getProvider());
    }
    
    /**
     * Gets the current process.
     * 
     * @return the current process
     */
    private Process getCurrentProcess()
    {
        Object value = StorageManager.instance().getProperty(
                CURRENT_PROCESS_KEY);
        
        if (value instanceof Process)
            return (Process)value;
        else
            return null;
    }
    
    /**
     * Gets the current result set.
     * 
     * @return the current result set
     */
    private ResultSet getCurrentResultSet()
    {
        Object value = StorageManager.instance().getProperty(CURRENT_RS_KEY);
        
        if (value instanceof ResultSet)
            return (ResultSet)value;
        else
            return null;
    }
    
    /**
     * Gets the current statement.
     * 
     * @return the current statement
     */
    private Statement getCurrentStatement()
    {
        Object value = StorageManager.instance().getProperty(CURRENT_ST_KEY);
        
        if (value instanceof Statement)
            return (Statement)value;
        else
            return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#getMetaData(com.toolsverse.
     * etl.sql.service.SqlRequest)
     */
    public Map<String, FieldDef> getMetaData(SqlRequest sqlRequest)
        throws Exception
    {
        Connection con = null;
        SessionConnectionProvider<Alias> provider = getConnectionProvider(sqlRequest);
        
        try
        {
            con = provider.getConnection(sqlRequest
                    .getConnectionParamsProvider().getConnectionParams());
            
            GenericMetadataExtractor extractor = new GenericMetadataExtractor();
            
            return extractor.getMetaData(sqlRequest.getSql(), con,
                    sqlRequest.getDriver(), null, true, true);
            
        }
        finally
        {
            if (sqlRequest.isClose() && con != null)
                releaseConnection(provider, sqlRequest
                        .getConnectionParamsProvider().getConnectionParams());
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#releaseConnection(com.toolsverse
     * .etl.sql.connection.SessionConnectionProvider,
     * com.toolsverse.etl.common.Alias)
     */
    public void releaseConnection(
            final SessionConnectionProvider<Alias> provider, final Alias params)
        throws Exception
    {
        getConnectionProvider(provider).releaseConnection(params);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.service.SqlService#rollbackTransaction(com.toolsverse
     * .etl.sql.connection.SessionConnectionProvider,
     * com.toolsverse.etl.common.Alias, boolean)
     */
    public void rollbackTransaction(SessionConnectionProvider<Alias> provider,
            Alias params, boolean closeOnRollback)
        throws Exception
    {
        provider = getConnectionProvider(provider);
        
        Connection con = null;
        
        con = provider.getConnection(params, false);
        
        if (con != null)
        {
            con.rollback();
            
            if (closeOnRollback)
                releaseConnection(provider, params);
        }
    }
    
    /**
     * Sets the current process.
     * 
     * @param process
     *            the new current process
     */
    private void setCurrentProcess(Process process)
    {
        if (process == null)
            StorageManager.instance().removeProperty(CURRENT_PROCESS_KEY);
        
        StorageManager.instance().setProperty(CURRENT_PROCESS_KEY,
                new StorageObject(StorageProvider.SESSION, process));
    }
    
    /**
     * Sets the current result set.
     * 
     * @param resultSet
     *            the new current result set
     */
    private void setCurrentResultSet(ResultSet resultSet)
    {
        if (resultSet == null)
            StorageManager.instance().removeProperty(CURRENT_RS_KEY);
        
        StorageManager.instance().setProperty(CURRENT_RS_KEY,
                new StorageObject(StorageProvider.SESSION, resultSet));
    }
    
    /**
     * Sets the current statement.
     * 
     * @param statement
     *            the new current statement
     */
    private void setCurrentStatement(Statement statement)
    {
        if (statement == null)
            StorageManager.instance().removeProperty(CURRENT_ST_KEY);
        
        StorageManager.instance().setProperty(CURRENT_ST_KEY,
                new StorageObject(StorageProvider.SESSION, statement));
    }
    
    /**
     * Prepares sql for execution. Removes unused chars, sets parameters,
     * including output if needed, etc.
     * 
     * @param sqlToRun
     *            the sql to run
     * @param paramMap
     *            the parameters map
     * @param outputParams
     *            the output parameters
     * @param sqlRequest
     *            the sql request
     * @return the string
     */
    private String toSql(String sqlToRun, Map<String, List<Integer>> paramMap,
            List<String> outputParams, SqlRequest sqlRequest)
    {
        if (Utils.isNothing(sqlToRun))
            return null;
        
        if (sqlRequest.hasParams())
            sqlToRun = sqlRequest.getParser().parseParams(sqlToRun, null,
                    paramMap, outputParams, sqlRequest.getDriver());
        
        return sqlRequest.getParser().removeChars(sqlToRun);
    }
    
    /**
     * Updates result.
     * 
     * @param type
     *            the sql type
     * @param result
     *            the result
     * @param value
     *            the value
     * @return the string
     */
    private String updateResult(int type, String result, String value)
    {
        switch (type)
        {
            case NOTHING_TYPE:
                value = "";
                break;
            case DML_TYPE:
                value = "SQL executed. " + value + " row(s) affected";
                break;
            case UPDATE_TYPE:
                value = "Updated " + value + " row(s)";
                break;
            case INSERT_TYPE:
                value = "Inserted " + value + " row(s)";
                break;
            case DELETE_TYPE:
                value = "Deleted " + value + " row(s)";
                break;
            case MERGE_TYPE:
                value = "Merged " + value + " row(s)";
                break;
            case EXECUTE_TYPE:
                value = "Statement executed";
                break;
            case EXECUTE_SCRIPT_TYPE:
                value = "SQL script executed";
                break;
            case OUTPUT_PARAM_TYPE:
                value = "Output variable " + value;
                break;
            case EXCEPTION_TYPE:
                value = "Exception occured: " + value;
                break;
            case EXPLAIN_PLAN_TYPE:
                value = "Explain Plan: \n" + value;
                break;
            
            default:
                value = "Statement executed";
                break;
        }
        
        if (Utils.isNothing(result))
            result = value;
        else
            result = result + "\n" + value;
        
        return result;
    }
}
