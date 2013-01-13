/*
 * DefaultCodeGen.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.codegen;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.MergeHandler;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.CodeGen;
import com.toolsverse.etl.core.engine.Destination;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.core.engine.EtlUnit;
import com.toolsverse.etl.core.engine.LoadFunctionContext;
import com.toolsverse.etl.core.engine.Scenario;
import com.toolsverse.etl.core.engine.TaskExecutor;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.function.DefFunctions;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.DriverUnit;
import com.toolsverse.etl.metadata.JdbcMetadata;
import com.toolsverse.etl.metadata.Metadata;
import com.toolsverse.etl.metadata.MetadataExtractor;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.DummyConnectionFactory;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.ext.loader.SharedUnitLoader;
import com.toolsverse.ext.loader.UnitLoader;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the CodeGen interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DefaultCodeGen implements CodeGen, MergeHandler
{
    
    /** The variables. */
    private final Set<String> _variables;
    
    /** The global variables. */
    private final ListHashMap<String, String> _globalVariables;
    
    /** The declare script. */
    private String _declareScript;
    
    /** The declare procedure script. */
    private String _declareProcScript;
    
    /** The declare cursor variable script. */
    private String _declareCursorVarScript;
    
    /** The declare cursor script. */
    private String _declareCursorScript;
    
    /** The declare cursor end script. */
    private String _declareCursorEndScript;
    
    /** The assembled scripts. */
    private final List<TypedKeyValue<String, Integer>> _assembledScripts;
    
    /** The currently executing script. */
    private String _currentExecutingScript;
    
    /** The scripts to init. */
    private List<String> _scriptsToInit;
    
    /** The scripts to create. */
    private List<String> _scriptsToCreate;
    
    /** The scripts to clean up after. */
    private List<String> _scriptsToCleanUpAfter;
    
    /** The scripts to clean up before. */
    private List<String> _scriptsToCleanUpBefore;
    
    /** The scripts to clean on exception. */
    private List<String> _scriptsToCleanOnException;
    
    /** The current index. */
    private int _scrIndex;
    
    /** The current unit. */
    private EtlUnit _unit;
    
    /** The status. */
    private int _status;
    
    /**
     * Instantiates a new default code generator.
     */
    public DefaultCodeGen()
    {
        _variables = new HashSet<String>();
        _globalVariables = new ListHashMap<String, String>();
        _declareScript = "";
        _declareProcScript = "";
        _declareCursorVarScript = "";
        _declareCursorScript = "";
        _declareCursorEndScript = "";
        _currentExecutingScript = "";
        _scrIndex = 0;
        _scriptsToInit = null;
        _scriptsToCreate = null;
        _scriptsToCleanUpAfter = null;
        _scriptsToCleanUpBefore = null;
        _scriptsToCleanOnException = null;
        _unit = null;
        _assembledScripts = new ArrayList<TypedKeyValue<String, Integer>>();
        _status = CREATED_STATUS;
    }
    
    /**
     * Adds the script to clean on exception.
     * 
     * @param script
     *            the script
     */
    private void addScriptToCleanOnException(String script)
    {
        if (Utils.isNothing(script))
            return;
        
        if (_scriptsToCleanOnException == null)
            _scriptsToCleanOnException = new ArrayList<String>();
        else if (_scriptsToCleanOnException.contains(script))
            return;
        
        _scriptsToCleanOnException.add(script);
    }
    
    /**
     * Adds the script to clean up after.
     * 
     * @param script
     *            the script
     */
    private void addScriptToCleanUpAfter(String script)
    {
        if (Utils.isNothing(script))
            return;
        
        if (_scriptsToCleanUpAfter == null)
            _scriptsToCleanUpAfter = new ArrayList<String>();
        
        _scriptsToCleanUpAfter.add(script);
    }
    
    /**
     * Adds the script to clean up before.
     * 
     * @param script
     *            the script
     */
    private void addScriptToCleanUpBefore(String script)
    {
        if (Utils.isNothing(script))
            return;
        
        if (_scriptsToCleanUpBefore == null)
            _scriptsToCleanUpBefore = new ArrayList<String>();
        
        _scriptsToCleanUpBefore.add(script);
    }
    
    /**
     * Adds the script to create objects.
     * 
     * @param destination
     *            the destination
     * @param script
     *            the script
     */
    private void addScriptToCreate(Destination destination, String script)
    {
        if (destination.getDataSet() == null
                || destination.getDataSet().getDriver() == null
                || Utils.isNothing(script))
            return;
        
        Driver driver = destination.getDataSet().getDriver();
        
        if (_scriptsToCreate == null)
            _scriptsToCreate = new ArrayList<String>();
        
        _scriptsToCreate.add(script);
        
        addScriptToCleanUpBefore(driver.getDropSql(destination.getType(),
                destination.getObjectName()));
        
        addScriptToCleanUpAfter(driver.getDropSql(destination.getType(),
                destination.getObjectName()));
    }
    
    /**
     * Adds the script to init.
     * 
     * @param destination
     *            the destination
     * @param script
     *            the script
     * @param cleanUpOnException
     *            the "clean up on exception" flag
     * @param type
     *            the type
     */
    private void addScriptToInit(Destination destination, String script,
            boolean cleanUpOnException, String type)
    {
        if (destination.getDataSet() == null
                || destination.getDataSet().getDriver() == null
                || Utils.isNothing(script))
            return;
        
        Driver driver = destination.getDataSet().getDriver();
        
        if (_scriptsToInit == null)
            _scriptsToInit = new ArrayList<String>();
        
        _scriptsToInit.add(script);
        
        if (cleanUpOnException)
        {
            addScriptToCleanOnException(driver.getDropSql(type,
                    destination.getObjectName()));
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#assembleCode(com.toolsverse.etl
     * .core.config.EtlConfig, com.toolsverse.etl.core.engine.Scenario,
     * com.toolsverse.etl.driver.Driver, int, boolean)
     */
    public void assembleCode(EtlConfig config, Scenario scenario,
            Driver driver, int loadIndex, boolean silent)
        throws Exception
    {
        if (_unit.getWriter() != null)
            return;
        
        String name = getScriptName(scenario.getScriptName());
        
        if (!silent)
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.ASSEMPBLING_SCRIPT_MSG.getValue());
        
        int numberOfScripts = numberOfScripts();
        
        if (numberOfScripts == 0)
            return;
        
        String script = "";
        
        if (numberOfScripts == 1)
        {
            TypedKeyValue<String, Integer> currentScript = _assembledScripts
                    .get(0);
            
            String header = Utils.nvl(driver.getDeclare(), "\n")
                    + Utils.nvl(_declareScript, "\n")
                    + Utils.nvl(_declareProcScript, "\n")
                    + Utils.nvl(_declareCursorVarScript, "\n")
                    + Utils.nvl(_declareCursorScript, "\n")
                    + Utils.nvl(_declareCursorEndScript, "\n");
            
            if (loadIndex > 0)
                header = header + Utils.nvl(driver.getBeginSplited(), "\n");
            else
                header = header + Utils.nvl(driver.getBegin(), "\n");
            
            // global variables
            if (_globalVariables != null)
                for (int varIndex = 0; varIndex < _globalVariables.size(); varIndex++)
                    header = header + _globalVariables.get(varIndex) + "\n";
            
            script = header + currentScript.getKey() + "\n"
                    + Utils.nvl(driver.getEnd());
            
            if (scenario.getOnSave() == Scenario.ON_ACTION_SAVE)
                FileUtils
                        .saveTextFile(EtlUtils.getSqlFileName(name, 0), script);
            
            currentScript.setKey(script);
        }
        else
            for (int i = 0; i < numberOfScripts; i++)
            {
                Logger.log(Logger.INFO, EtlLogger.class,
                        EtlResource.ASSEMPBLING_SCRIPT_MSG.getValue() + " "
                                + name + "_" + String.valueOf(i) + " out of "
                                + numberOfScripts + "...");
                
                TypedKeyValue<String, Integer> currentScript = _assembledScripts
                        .get(i);
                
                String header = Utils.nvl(driver.getDeclare(), "\n")
                        + Utils.nvl(_declareScript, "\n")
                        + Utils.nvl(_declareProcScript, "\n")
                        + Utils.nvl(_declareCursorVarScript, "\n")
                        + Utils.nvl(_declareCursorScript, "\n")
                        + Utils.nvl(_declareCursorEndScript, "\n");
                
                if (i == 0 && loadIndex == 0)
                    header = header + Utils.nvl(driver.getBegin(), "\n");
                else
                    header = header + Utils.nvl(driver.getBeginSplited(), "\n");
                
                // global variables
                if (_globalVariables != null)
                    for (int varIndex = 0; varIndex < _globalVariables.size(); varIndex++)
                        header = header + _globalVariables.get(varIndex) + "\n";
                
                script = header + currentScript.getKey() + "\n"
                        + Utils.nvl(driver.getEndSplited());
                
                if (scenario.getOnSave() == Scenario.ON_ACTION_SAVE)
                    FileUtils.saveTextFile(EtlUtils.getSqlFileName(name, i),
                            script);
                
                currentScript.setKey(script);
            }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#cleanUp(com.toolsverse.etl.core
     * .config.EtlConfig, com.toolsverse.etl.core.engine.Scenario,
     * com.toolsverse.etl.driver.Driver, java.sql.Connection)
     */
    public void cleanUp(EtlConfig config, Scenario scenario, Driver driver,
            Connection conn)
        throws Exception
    {
        executeScript(config, conn, _scriptsToCleanUpAfter, true, driver,
                EtlResource.DELETE_SQL_SCRIPT_MSG.getValue(),
                getScriptName(scenario.getScriptName()), false, true, true);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#cleanUpOnException(com.toolsverse
     * .etl.core.engine.Scenario, com.toolsverse.etl.driver.Driver)
     */
    public void cleanUpOnException(EtlConfig config, Scenario scenario,
            Driver driver, Connection cleanUpConn)
        throws Exception
    {
        executeScript(config, cleanUpConn, _scriptsToCleanOnException, true,
                driver, EtlResource.DELETE_SQL_SCRIPT_MSG.getValue(),
                getScriptName(scenario.getScriptName()), false, true, true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#copy(com.toolsverse.etl.core.engine
     * .CodeGen)
     */
    public void copy(CodeGen codeGen)
    {
        if (codeGen.getScriptsToCleanOnException() != null)
            for (String value : codeGen.getScriptsToCleanOnException())
            {
                addScriptToCleanOnException(value);
            }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#execute(com.toolsverse.etl.core
     * .config.EtlConfig, com.toolsverse.etl.core.engine.Scenario,
     * com.toolsverse.etl.driver.Driver, boolean, java.sql.Connection,
     * java.sql.Connection, com.toolsverse.etl.core.engine.Destination)
     */
    public void execute(EtlConfig config, Scenario scenario, Driver driver,
            boolean silent, Connection conn, Connection cleanUpConn,
            Destination destination)
        throws Exception
    {
        _status = EXECUTED_STATUS;
        
        String script = null;
        CallableStatement callableStatement = null;
        
        if (_unit.getWriter() != null)
            return;
        
        try
        {
            executeScript(config, cleanUpConn, _scriptsToInit, false, driver,
                    EtlResource.EXECUTION_SQL_SCRIPT_MSG.getValue(),
                    EtlConfig.CREATE_SQL, true, false, true);
            
            executeScript(config, cleanUpConn, _scriptsToCleanUpBefore, true,
                    driver, EtlResource.EXECUTION_SQL_SCRIPT_MSG.getValue(),
                    EtlConfig.DROP_SQL, false, true, true);
            
            executeScript(config, cleanUpConn, _scriptsToCreate, false, driver,
                    EtlResource.EXECUTION_SQL_SCRIPT_MSG.getValue(),
                    EtlConfig.CREATE_SQL, true, true, true);
            
            for (int i = 0; i < _assembledScripts.size(); i++)
            {
                String sName = getScriptName(scenario.getScriptName()) + "_"
                        + String.valueOf(config.getNewAtomicLong());
                
                if (!silent)
                {
                    if (driver.supportsCallableStatement()
                            && !driver.supportsAnonymousBlocks())
                        Logger.log(Logger.INFO, EtlLogger.class,
                                EtlResource.COMPILE_SQL_SCRIPT_MSG.getValue()
                                        + sName + " out of "
                                        + numberOfScripts());
                    else
                        Logger.log(Logger.INFO, EtlLogger.class,
                                EtlResource.EXECUTION_SQL_SCRIPT_MSG.getValue()
                                        + sName + " out of "
                                        + numberOfScripts());
                }
                
                script = setCurrentCode(driver, sName, i);
                
                if (driver.supportsCallableStatement())
                {
                    String identName = null;
                    
                    try
                    {
                        if (!driver.supportsAnonymousBlocks())
                        {
                            identName = driver.getIdentifierName(sName,
                                    Driver.PROC_TYPE);
                            
                            executeCallable(cleanUpConn, driver.getDropSql(
                                    Destination.PROC_TYPE, identName), true,
                                    null);
                        }
                        
                        callableStatement = cleanUpConn.prepareCall(script);
                        
                        if (destination != null)
                            setLargeObjects(destination, driver,
                                    callableStatement, 0);
                        
                        callableStatement.execute();
                        
                        if (!driver.supportsAnonymousBlocks())
                        {
                            executeCallable(
                                    conn,
                                    driver.getCallSql(identName),
                                    false,
                                    !silent ? EtlResource.EXECUTION_SQL_SCRIPT_MSG
                                            .getValue()
                                            + sName
                                            + " out of "
                                            + numberOfScripts() : null);
                        }
                    }
                    finally
                    {
                        SqlUtils.cleanUpSQLData(callableStatement, null, this);
                        callableStatement = null;
                        
                        if (!driver.supportsAnonymousBlocks())
                            executeCallable(cleanUpConn, driver.getDropSql(
                                    Destination.PROC_TYPE, identName), true,
                                    null);
                    }
                }
                else
                    executeNotCallableDestCode(config, script, driver,
                            scenario, conn, silent);
            }
            
            if (scenario.isCommitEachBlock())
                conn.commit();
        }
        catch (Exception ex)
        {
            String fName = EtlUtils.getSqlFileName(
                    getScriptName(scenario.getScriptName()), _scrIndex);
            
            if (Scenario.ON_ACTION_SKIP == scenario.getOnSave())
                FileUtils.saveTextFile(fName, _currentExecutingScript);
            
            config.setLastExecutedFileName(fName);
            config.setLastExecutedCode(_currentExecutingScript);
            config.setErrorLine(EtlUtils.parseException(ex,
                    _currentExecutingScript, driver.getErrorLinePattern()));
            
            throw ex;
        }
    }
    
    /**
     * Executes callable statement.
     * 
     * @param conn
     *            the connection
     * @param script
     *            the script
     * @param ignoreException
     *            the "ignore exception" flag
     * @param message
     *            the message
     * @throws Exception
     *             in case of any error
     */
    private void executeCallable(Connection conn, String script,
            boolean ignoreException, String message)
        throws Exception
    {
        if (Utils.isNothing(script))
            return;
        
        CallableStatement callableStatement = null;
        
        String[] stmts = SqlUtils.parseSql(script);
        
        if (!Utils.isNothing(message))
            Logger.log(Logger.INFO, EtlLogger.class, message);
        
        for (int index = 0; index < stmts.length; index++)
        {
            String sql = stmts[index];
            try
            {
                callableStatement = conn.prepareCall(sql);
                
                callableStatement.executeUpdate();
                
                SqlUtils.cleanUpSQLData(callableStatement, null, null);
                callableStatement = null;
            }
            catch (Exception ex)
            {
                if (!ignoreException)
                    throw ex;
            }
            finally
            {
                SqlUtils.cleanUpSQLData(callableStatement, null, null);
            }
        }
        
    }
    
    /**
     * Executes sql for destination if driver doesn't support callable
     * statement.
     * 
     * @param config
     *            the etl config
     * @param script
     *            the script
     * @param driver
     *            the driver
     * @param scenario
     *            the scenario
     * @param conn
     *            the connection
     * @param silent
     *            the silent. If true all logging except for errors is disabled.
     * @throws Exception
     *             in case of any error
     */
    private void executeNotCallableDestCode(EtlConfig config, String script,
            Driver driver, Scenario scenario, Connection conn, boolean silent)
        throws Exception
    {
        if (_unit.getWriter() != null)
            return;
        
        String[] stmts = script.split(Utils.str2Regexp(driver
                .getOnExceptionEnd()));
        
        if (stmts == null)
            return;
        
        String[] localStmts = null;
        Destination dest = null;
        int endIndex = -1;
        PreparedStatement preparedStatement = null;
        int action;
        String sqlToRun = null;
        
        for (int index = 0; index < stmts.length; index++)
        {
            String sql = stmts[index];
            
            if (!Utils.isNothing(sql))
            {
                // parse exception handler if any
                TypedKeyValue<String, Integer> destInfo = driver
                        .getDestinationInfo(sql);
                
                String destName = destInfo.getKey();
                int row = destInfo.getValue();
                dest = scenario.getDestinations().get(destName);
                
                if (!Utils.isNothing(dest.getSql()))
                    localStmts = SqlUtils.parseSql(sql);
                else
                    localStmts = new String[] {sql};
                
                for (int ind = 0; ind < localStmts.length; ind++)
                {
                    sqlToRun = localStmts[ind];
                    
                    sqlToRun = parseRuntimeFunctions(config, dest, driver
                            .replaceOnException(sqlToRun).trim(), driver);
                    
                    endIndex = getNextIndex(dest, stmts, index);
                    if (endIndex >= 0 && endIndex < stmts.length)
                        stmts[endIndex] = Utils.findAndReplace(stmts[endIndex],
                                driver.getOnExceptionEnd(), "", true);
                    
                    if (!silent)
                        Logger.log(
                                Logger.INFO,
                                EtlLogger.class,
                                EtlResource.EXECUTION_SQL_SUB_SCRIPT_MSG
                                        .getValue()
                                        + getScriptName(scenario
                                                .getScriptName())
                                        + ": "
                                        + String.valueOf(index)
                                        + " out of "
                                        + stmts.length);
                    
                    if (!Utils.isNothing(sqlToRun))
                    {
                        String[] sts = SqlUtils.parseSql(sqlToRun);
                        
                        Savepoint svp = null;
                        
                        for (String willExecute : sts)
                        {
                            svp = null;
                            
                            try
                            {
                                preparedStatement = conn
                                        .prepareStatement(willExecute);
                                
                                setLargeObjects(dest, driver,
                                        preparedStatement, row);
                                
                                if (dest.isSavePoint())
                                    svp = SqlUtils.getSavepoint(conn);
                                
                                preparedStatement.executeUpdate();
                            }
                            catch (Exception ex)
                            {
                                if (svp != null)
                                    conn.rollback(svp);
                                
                                action = dest.handleException(conn,
                                        willExecute, ex, row, this);
                                
                                if (action == OnException.ON_EXCEPTION_CONTINUE)
                                    continue;
                                else if (action == OnException.ON_EXCEPTION_MERGE)
                                    continue;
                                else if (action == OnException.ON_EXCEPTION_IGNORE)
                                {
                                    index = endIndex - 1;
                                    endIndex = -1;
                                }
                                else
                                    throw ex;
                            }
                            finally
                            {
                                SqlUtils.cleanUpSQLData(preparedStatement,
                                        null, this);
                                preparedStatement = null;
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Executes sql scripts.
     * 
     * @param config
     *            the etl config
     * @param conn
     *            the connection
     * @param scripts
     *            the scripts to execute
     * @param ignoreException
     *            the ignore exception
     * @param driver
     *            the driver
     * @param message
     *            the message
     * @param name
     *            the name
     * @param saveError
     *            "the save error" flag. If true the sql which generated error
     *            will be saved in the data\errors folder
     * @param callable
     *            the "callable" flag. If true the sql is executed as a callable
     *            statement (anonymous sql block, stored procedure, etc)
     * @param clean
     *            the "clean" flag. If true the <code>scripts</code> will be
     *            cleared at the end
     * @throws Exception
     *             in case of any error
     */
    private void executeScript(EtlConfig config, Connection conn,
            List<String> scripts, boolean ignoreException, Driver driver,
            String message, String name, boolean saveError, boolean callable,
            boolean clean)
        throws Exception
    {
        if (scripts == null || scripts.size() == 0)
            return;
        
        CallableStatement callableStatement = null;
        
        PreparedStatement preparedStatement = null;
        
        try
        {
            for (int index = 0; index < scripts.size(); index++)
            {
                String sql = scripts.get(index);
                if (!Utils.isNothing(sql))
                    try
                    {
                        if (!Utils.isNothing(message))
                            Logger.log(Logger.INFO, EtlLogger.class, message
                                    + name + "_" + String.valueOf(index)
                                    + " out of " + scripts.size());
                        
                        if (callable && driver.supportsCallableStatement())
                        {
                            callableStatement = null;
                            Savepoint svp = null;
                            
                            try
                            {
                                callableStatement = conn.prepareCall(sql);
                                
                                // need it mostly for postgres
                                svp = ignoreException
                                        && driver
                                                .requiresRollbackAfterSqlError() ? SqlUtils
                                        .getSavepoint(conn) : null;
                                
                                callableStatement.executeUpdate();
                            }
                            catch (Exception ex)
                            {
                                if (svp != null)
                                    conn.rollback(svp);
                                
                                throw ex;
                            }
                            finally
                            {
                                SqlUtils.cleanUpSQLData(callableStatement,
                                        null, null);
                            }
                        }
                        else
                        {
                            String[] stmts = SqlUtils.parseSql(sql);
                            
                            for (String sqlToExecute : stmts)
                            {
                                preparedStatement = null;
                                
                                Savepoint svp = null;
                                
                                try
                                {
                                    preparedStatement = conn
                                            .prepareStatement(sqlToExecute);
                                    
                                    // need it mostly for postgres
                                    svp = ignoreException
                                            && driver
                                                    .requiresRollbackAfterSqlError() ? SqlUtils
                                            .getSavepoint(conn) : null;
                                    
                                    preparedStatement.execute();
                                }
                                catch (Exception ex)
                                {
                                    if (svp != null)
                                        conn.rollback(svp);
                                    
                                    throw ex;
                                }
                                finally
                                {
                                    SqlUtils.cleanUpSQLData(preparedStatement,
                                            null, null);
                                    
                                }
                            }
                        }
                        
                        callableStatement = null;
                        preparedStatement = null;
                    }
                    catch (Exception ex)
                    {
                        String fName = EtlUtils.getSqlFileName(name, index);
                        
                        if (saveError)
                        {
                            FileUtils.saveTextFile(fName, sql);
                        }
                        
                        if (!ignoreException)
                        {
                            config.setLastExecutedFileName(fName);
                            config.setLastExecutedCode(sql);
                            config.setErrorLine(EtlUtils.parseException(ex,
                                    sql, driver.getErrorLinePattern()));
                            
                            if (!saveError)
                                setCurrentCode(driver, message, index);
                            
                            throw ex;
                        }
                    }
                    finally
                    {
                        if (callable && driver.supportsCallableStatement())
                            SqlUtils.cleanUpSQLData(callableStatement, null,
                                    null);
                        else
                            SqlUtils.cleanUpSQLData(preparedStatement, null,
                                    null);
                    }
            }
        }
        finally
        {
            if (clean)
                scripts.clear();
        }
    }
    
    /**
     * Gets auto generated sql for destination and current data set row.
     * 
     * @param config
     *            the etl config
     * @param destination
     *            the destination
     * @param currentRow
     *            the current row
     * @param isFromCursor
     *            the is from cursor. If true the data are coming from the
     *            cursor
     * @param scenario
     *            the scenario
     * @param row
     *            the row number
     * @return the auto sql
     * @throws Exception
     *             in case of any error
     */
    private String getAutoSql(EtlConfig config, Destination destination,
            DataSetRecord currentRow, boolean isFromCursor, Scenario scenario,
            long row)
        throws Exception
    {
        String cursorStartSql = "";
        String cursorEndSql = "";
        String dropTableSql = "";
        
        DataSet dataSet = destination.getDataSet();
        Driver driver = dataSet.getDriver();
        
        if (isFromCursor)
        {
            Alias alias = (Alias)config
                    .getConnectionFactory()
                    .getConnectionParams(
                            Utils.isNothing(destination.getConnectionName()) ? EtlConfig.DEST_CONNECTION_NAME
                                    : destination.getConnectionName());
            
            String tableName = destination.getCursorTableName(driver);
            String sourceSql = !Utils.isNothing(destination.getCursorSql()) ? destination
                    .getCursorSql() : tableName;
            
            _declareCursorVarScript = driver.getDeclareCursorVarSql(
                    _declareCursorVarScript, dataSet,
                    alias != null ? alias.getJdbcDriverClass() : null, config,
                    _variables);
            _declareCursorScript = driver.getDeclareCursorSql(
                    _declareCursorScript,
                    !Utils.isNothing(tableName) ? tableName : destination
                            .getName(), sourceSql, dataSet);
            _declareCursorEndScript = driver.getDeclareCursorEndSql();
            
            cursorStartSql = driver.getCursorLoopStartSql(!Utils
                    .isNothing(tableName) ? tableName : destination.getName(),
                    sourceSql, dataSet);
            cursorEndSql = driver.getCursorLoopEndSql(!Utils
                    .isNothing(tableName) ? tableName : destination.getName(),
                    dataSet);
            
            currentRow = dataSet.getCursorRecord(driver);
            
            if (destination.getOnFinish() == Destination.ON_FINISH_DROP
                    && !Utils.isNothing(tableName))
            {
                String dropSql = driver.getDropSql(Destination.TABLE_TYPE,
                        tableName);
                
                if (!dropSql.endsWith(driver.getDelimiter() + "\n"))
                    dropTableSql = dropSql + driver.getDelimiter() + "\n";
                else
                    dropTableSql = dropSql;
                
                addScriptToCleanUpAfter(dropSql);
            }
        }
        
        if (currentRow == null)
            return null;
        
        TaskExecutor taskExecutor = new TaskExecutor();
        
        TaskResult taskResult = taskExecutor.executeInlineTasks(config,
                destination, scenario, row);
        
        if (taskResult != null)
        {
            if (taskResult.getResult() == TaskResult.TaskResultCode.REJECT
                    || taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                return "";
        }
        
        LoadFunctionContext context = new LoadFunctionContext();
        context.setCurrentRecord(currentRow);
        context.setDestination(destination);
        context.setFromCursor(isFromCursor);
        context.setRow(row);
        
        String sql = cursorStartSql
                + getAutoSql(config, destination, currentRow, context) + "\n"
                + cursorEndSql + dropTableSql;
        
        Utils.splitLines(_assembledScripts, sql.trim(), driver.getLinesLimit(),
                "\n");
        
        return sql;
    }
    
    /**
     * Gets auto generated sql for destination and current data set row.
     * 
     * @param config
     *            the config
     * @param destination
     *            the destination
     * @param currentRow
     *            the current row
     * @param context
     *            the context
     * @return the auto sql
     * @throws Exception
     *             in case of any error
     */
    private String getAutoSql(EtlConfig config, Destination destination,
            DataSetRecord currentRow, LoadFunctionContext context)
        throws Exception
    {
        DataSet dataSet;
        String prepSql = "";
        
        dataSet = destination.getDataSet();
        
        String tableName = dataSet.getDriver().getTableName(
                destination.getObjectName());
        
        Driver driver = destination.getDataSet().getDriver();
        
        boolean isCallable = driver.supportsCallableStatement();
        
        context.setScope(Variable.EXECUTE_BEFORE);
        context.setConfig(config);
        
        if (destination.getVariables() != null && isCallable)
            for (int i = 0; i < destination.getVariables().size(); i++)
            {
                Variable var = destination.getVariables().get(i);
                
                if (Utils.isNothing(var.getFunctionClassName()))
                    var.setFunctionClassName(driver.getDefaultFunctionClass());
                
                prepSql = prepSql + context.execute(var);
            }
        
        TypedKeyValue<List<String>, List<String>> fieldsAndValues = getFieldsAndValues(
                config, destination, currentRow, context, dataSet.getDriver());
        
        String autoSql = "";
        
        if (Destination.LOAD_INSERT.equalsIgnoreCase(destination
                .getLoadAction()))
            autoSql = dataSet.getDriver().getInsertStatement(fieldsAndValues,
                    tableName);
        else if (Destination.LOAD_UPDATE.equalsIgnoreCase(destination
                .getLoadAction()))
            autoSql = dataSet.getDriver().getUpdateStatement(fieldsAndValues,
                    tableName, destination.getLoadKey());
        else if (Destination.LOAD_MERGE.equalsIgnoreCase(destination
                .getLoadAction()))
            autoSql = dataSet.getDriver().getMergeStatement(fieldsAndValues,
                    tableName, destination.getLoadKey());
        else if (Destination.LOAD_DELETE.equalsIgnoreCase(destination
                .getLoadAction()))
            autoSql = dataSet.getDriver().getDeleteStatement(fieldsAndValues,
                    tableName, destination.getLoadKey());
        else
            autoSql = dataSet.getDriver().getInsertStatement(fieldsAndValues,
                    tableName);
        
        String condition = destination.getCondition();
        if (!Utils.isNothing(condition))
        {
            autoSql = dataSet.getDriver().getIf()
                    + EtlUtils.mergeSqlWithVars(condition,
                            destination.getVariables(), dataSet.getDriver())
                    + dataSet.getDriver().getIfBegin()
                    + getThenElse(autoSql, EtlUtils.mergeSqlWithVars(
                            destination.getThen(), destination.getVariables(),
                            dataSet.getDriver()), EtlUtils.mergeSqlWithVars(
                            destination.getElse(), destination.getVariables(),
                            dataSet.getDriver()), dataSet.getDriver())
                    + dataSet.getDriver().getIfEnd();
            
            if (!Utils.isNothing(destination.getAfter()))
                autoSql = autoSql
                        + "\n"
                        + EtlUtils
                                .mergeSqlWithVars(destination.getAfter(),
                                        destination.getVariables(),
                                        dataSet.getDriver()) + "\n";
        }
        
        autoSql = onException(destination, autoSql, context.getRow());
        
        String sql = prepSql + autoSql + "\n";
        
        context.setScope(Variable.EXECUTE_AFTER);
        context.setConfig(config);
        
        if (destination.getVariables() != null)
            for (int i = 0; i < destination.getVariables().size(); i++)
            {
                Variable var = destination.getVariables().get(i);
                
                if (Utils.isNothing(var.getFunctionClassName()))
                    var.setFunctionClassName(driver.getDefaultFunctionClass());
                
                sql = sql + context.execute(var);
            }
        
        return sql;
    }
    
    /**
     * Gets the fields and values for the current row. Used by code which
     * auto-generates insert/update/delete/merge sql statements.
     * 
     * @param config
     *            the config
     * @param destination
     *            the destination
     * @param currentRow
     *            the current row
     * @param context
     *            the context
     * @param driver
     *            the driver
     * @return the fields and values
     * @throws Exception
     *             in case of any error
     */
    
    private TypedKeyValue<List<String>, List<String>> getFieldsAndValues(
            EtlConfig config, Destination destination,
            DataSetRecord currentRow, LoadFunctionContext context, Driver driver)
        throws Exception
    {
        boolean isCallable = driver.supportsCallableStatement();
        
        boolean blobsAsParams = !isCallable
                || driver.supportsParamsInAnonymousBlocks();
        
        List<String> fields = new ArrayList<String>();
        
        List<String> values = new ArrayList<String>();
        
        DataSet dataSet = destination.getDataSet();
        
        int count = dataSet.getFieldCount();
        
        for (int i = 0; i < count; i++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(i);
            
            String fieldName = fieldDef.getName();
            
            int fieldType = fieldDef.getSqlDataType();
            Object fieldValue = dataSet.getFieldValue(currentRow, i);
            
            Variable var = destination.getVariable(fieldName);
            if (var != null && !var.isInclude())
            {
                continue;
            }
            
            if (var != null)
            {
                String label = var.getLabel();
                
                if (!Utils.isNothing(label))
                    fieldName = label;
                
                if (Utils.isNothing(var.getFunctionClassName()))
                    var.setFunctionClassName(driver.getDefaultFunctionClass());
            }
            
            if (destination.getMetaData() != null
                    && destination.getMetaData().size() > 0)
            {
                if (!destination.getMetaData().containsKey(fieldName))
                    continue;
            }
            
            fields.add(fieldName);
            
            if (var == null)
                values.add(SqlUtils.isLargeObject(fieldType) && blobsAsParams ? "?"
                        : dataSet.getDriver().convertValueForStorage(
                                fieldValue, fieldType, context.isFromCursor()));
            else
            {
                if (isCallable)
                {
                    if ((SqlUtils.isLargeObject(fieldType) && !dataSet
                            .getDriver().supportsBinaryInProc())
                            || var.isScopeSet(Variable.EXECUTE_RUNTIME))
                    {
                        try
                        {
                            context.setScope(Variable.EXECUTE_RUNTIME);
                            values.add(context.execute(var));
                        }
                        finally
                        {
                            context.setScope(Variable.EXECUTE_BEFORE);
                        }
                        
                    }
                    else
                        values.add(dataSet.getDriver()
                                .getVarName(var.getName()));
                }
                else
                    values.add(context.execute(var));
            }
            
        }
        
        return new TypedKeyValue<List<String>, List<String>>(fields, values);
    }
    
    /**
     * Gets the inline sql for destination and current data set row.
     * 
     * @param config
     *            the etl config
     * @param destination
     *            the destination
     * @param currentRow
     *            the current row
     * @param isFromCursor
     *            the is from cursor. If true the data re comming from the
     *            cursor
     * @param scenario
     *            the scenario
     * @param row
     *            the row number
     * @return the inline sql
     * @throws Exception
     *             in case of any error
     */
    private String getInLineSql(EtlConfig config, Destination destination,
            DataSetRecord currentRow, boolean isFromCursor, Scenario scenario,
            long row)
        throws Exception
    {
        String sql = destination.getSql();
        String prepSql = "";
        
        String cursorStartSql = "";
        String cursorEndSql = "";
        String dropTableSql = "";
        
        DataSet dataSet = destination.getDataSet();
        Driver driver = dataSet.getDriver();
        
        if (isFromCursor)
        {
            Alias alias = (Alias)config
                    .getConnectionFactory()
                    .getConnectionParams(
                            Utils.isNothing(destination.getConnectionName()) ? EtlConfig.DEST_CONNECTION_NAME
                                    : destination.getConnectionName());
            
            String tableName = destination.getCursorTableName(driver);
            String sourceSql = !Utils.isNothing(destination.getCursorSql()) ? destination
                    .getCursorSql() : tableName;
            
            _declareCursorVarScript = driver.getDeclareCursorVarSql(
                    _declareCursorVarScript, dataSet,
                    alias != null ? alias.getJdbcDriverClass() : null, config,
                    _variables);
            _declareCursorScript = driver.getDeclareCursorSql(
                    _declareCursorScript,
                    !Utils.isNothing(tableName) ? tableName : destination
                            .getName(), sourceSql, dataSet);
            _declareCursorEndScript = driver.getDeclareCursorEndSql();
            
            cursorStartSql = driver.getCursorLoopStartSql(!Utils
                    .isNothing(tableName) ? tableName : destination.getName(),
                    sourceSql, dataSet);
            cursorEndSql = driver.getCursorLoopEndSql(!Utils
                    .isNothing(tableName) ? tableName : destination.getName(),
                    dataSet);
            
            currentRow = dataSet.getCursorRecord(driver);
            
            if (destination.getOnFinish() == Destination.ON_FINISH_DROP
                    && !Utils.isNothing(tableName))
            {
                String dropSql = driver.getDropSql(Destination.TABLE_TYPE,
                        tableName);
                
                if (!dropSql.endsWith(driver.getDelimiter() + "\n"))
                    dropTableSql = dropSql + driver.getDelimiter() + "\n";
                else
                    dropTableSql = dropSql;
                
                addScriptToCleanUpAfter(dropSql);
            }
        }
        
        boolean isCallable = driver.supportsCallableStatement();
        
        if (!isCallable && currentRow == null && destination.isEmpty())
            return null;
        
        TaskExecutor taskExecutor = new TaskExecutor();
        
        TaskResult taskResult = taskExecutor.executeInlineTasks(config,
                destination, scenario, row);
        
        if (taskResult != null)
        {
            if (taskResult.getResult() == TaskResult.TaskResultCode.REJECT
                    || taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                return "";
        }
        
        LoadFunctionContext context = new LoadFunctionContext();
        context.setCurrentRecord(currentRow);
        context.setDestination(destination);
        context.setScope(Variable.EXECUTE_BEFORE);
        context.setFromCursor(isFromCursor);
        context.setRow(row);
        context.setConfig(config);
        
        if (destination.getVariables() != null)
            for (int i = 0; i < destination.getVariables().size(); i++)
            {
                Variable var = destination.getVariables().get(i);
                
                if (Utils.isNothing(var.getFunctionClassName()))
                    var.setFunctionClassName(driver.getDefaultFunctionClass());
                
                String varValue = context.execute(var);
                prepSql = prepSql + varValue;
                
                if (isCallable)
                    sql = Utils.findAndReplace(sql,
                            EtlUtils.getPatternName(var.getName()),
                            driver.getVarName(var.getName()), true);
                else
                    sql = Utils.findAndReplace(sql,
                            EtlUtils.getPatternName(var.getName()), varValue,
                            true);
            }
        
        sql = onException(destination, sql, row);
        
        context.setScope(Variable.EXECUTE_AFTER);
        context.setConfig(config);
        
        if (destination.getVariables() != null)
            for (int i = 0; i < destination.getVariables().size(); i++)
            {
                Variable var = destination.getVariables().get(i);
                
                if (Utils.isNothing(var.getFunctionClassName()))
                    var.setFunctionClassName(driver.getDefaultFunctionClass());
                
                sql = sql + context.execute(var);
            }
        
        if (isCallable)
            sql = cursorStartSql + prepSql + sql + "\n" + cursorEndSql
                    + dropTableSql;
        
        if (!destination.isProcOrFunc())
        {
            Utils.splitLines(_assembledScripts, sql.trim(),
                    driver.getLinesLimit(), "\n");
        }
        
        return sql;
    }
    
    /**
     * Gets the index of the the next statement if driver doesn't support
     * callable statement.
     * 
     * @param dest
     *            the destination
     * @param stmts
     *            the sql statements
     * @param index
     *            the current index
     * @return the next index
     */
    private int getNextIndex(Destination dest, String[] stmts, int index)
    {
        if (dest == null)
            return index + 1;
        
        String endStr = dest.getDataSet().getDriver().getOnExceptionEnd();
        
        if (Utils.isNothing(endStr))
            return index + 1;
        
        endStr = endStr.toUpperCase();
        
        for (int i = index + 1; i < stmts.length; i++)
        {
            String s = stmts[i];
            
            if (!Utils.isNothing(s) && s.toUpperCase().indexOf(endStr) >= 0)
                return i;
        }
        
        return index + 1;
    }
    
    /**
     * Gets the script name.
     * 
     * @param name
     *            the name
     * @return the script name
     */
    private String getScriptName(String name)
    {
        if (getUnit() == null)
            return name;
        
        return name + "_" + getUnit().getConnectionName();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#getScriptsToCleanOnException()
     */
    public List<String> getScriptsToCleanOnException()
    {
        return _scriptsToCleanOnException;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.CodeGen#getStatus()
     */
    public int getStatus()
    {
        return _status;
    }
    
    /**
     * Gets if\then\else sql.
     * 
     * @param sql
     *            the sql
     * @param thenS
     *            the "then" token
     * @param elseS
     *            the "else" token
     * @param driver
     *            the driver
     * @return the if\then\else sql
     */
    private String getThenElse(String sql, String thenS, String elseS,
            Driver driver)
    {
        if (!Utils.isNothing(thenS))
            sql = thenS + sql;
        
        if (!Utils.isNothing(elseS))
            sql = sql + driver.getIfElse() + elseS;
        
        return sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.CodeGen#getUnit()
     */
    public EtlUnit getUnit()
    {
        return _unit;
    }
    
    /**
     * Gets the number of scripts.
     * 
     * @return the number of scripts
     */
    private int numberOfScripts()
    {
        return _assembledScripts.size();
    }
    
    /**
     * Adds exception handler to the sql.
     * 
     * @param destination
     *            the destination
     * @param sql
     *            the sql
     * @param row
     *            the row number
     * @return the string
     */
    private String onException(Destination destination, String sql, long row)
    {
        Driver driver = destination.getDataSet().getDriver();
        int action = destination.getOnExceptionAction();
        
        if ((action & OnException.ON_EXCEPTION_RAISE) != action
                || !driver.supportsCallableStatement())
            sql = driver.getOnExceptionBegin(destination, row) + sql
                    + driver.getOnException(destination) + "\n"
                    + driver.getOnExceptionEnd() + "\n";
        
        return sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.MergeHandler#onMerge(com.toolsverse.etl.common
     * .OnException, java.sql.Connection, java.lang.String, java.lang.String,
     * long)
     */
    public void onMerge(OnException onException, Connection conn, String sql,
            String keyField, long row)
        throws Exception
    {
        Destination dest = (Destination)onException;
        
        if (dest == null || dest.getDataSet() == null || row < 0)
            throw new Exception(EtlResource.MERGE_EXCEPTION_MSG.getValue());
        
        PreparedStatement preparedStatement = null;
        
        try
        {
            sql = EtlUtils.insert2Update(sql, keyField);
            
            if (Utils.isNothing(sql))
                throw new Exception(EtlResource.MERGE_EXCEPTION_MSG.getValue());
            
            preparedStatement = conn.prepareStatement(sql);
            
            setLargeObjects(dest, dest.getDataSet().getDriver(),
                    preparedStatement, (int)row);
            
            preparedStatement.executeUpdate();
        }
        finally
        {
            SqlUtils.cleanUpSQLData(preparedStatement, null, null);
            preparedStatement = null;
        }
    }
    
    /**
     * Parses the runtime functions.
     *
     * @param config the etl config
     * @param dest the destination
     * @param sql the sql
     * @param driver the driver
     * @return the string
     * @throws Exception in case of any error
     */
    private String parseRuntimeFunctions(EtlConfig config, Destination dest,
            String sql, Driver driver)
        throws Exception
    {
        if (dest == null || dest.getVariables() == null
                || dest.getVariables().size() == 0)
            return sql;
        
        LoadFunctionContext context = new LoadFunctionContext();
        context.setDestination(dest);
        context.setScope(Variable.EXECUTE_RUNTIME);
        context.setConfig(config);
        
        int pos = 0;
        
        for (int i = 0; i < dest.getVariables().size(); i++)
        {
            Variable var = dest.getVariables().get(i);
            
            if (Utils.isNothing(var.getFunction()))
                continue;
            
            pos = 0;
            
            while (pos >= 0)
            {
                pos = sql.indexOf(var.getFunction());
                
                if (pos >= 0)
                {
                    String rest = sql.substring(pos);
                    int open = rest.indexOf("(");
                    int close = rest.indexOf(")");
                    
                    if (open >= 0 && close > open)
                    {
                        String value = rest.substring(open + 1, close).trim();
                        context.setCurrentValue(value);
                        
                        if (Utils.isNothing(var.getFunctionClassName()))
                            var.setFunctionClassName(driver
                                    .getDefaultFunctionClass());
                        
                        String varValue = context.execute(var);
                        
                        String toReplace = sql.substring(pos, pos + close + 1);
                        
                        sql = Utils.findAndReplace(sql, toReplace, varValue,
                                true);
                        
                        pos = -1;
                    }
                }
            }
        }
        
        return sql;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#prepare(com.toolsverse.etl.core
     * .config.EtlConfig, com.toolsverse.etl.core.engine.Scenario,
     * com.toolsverse.etl.core.engine.Destination, boolean, boolean)
     */
    public void prepare(EtlConfig config, Scenario scenario,
            Destination destination, boolean silent, boolean onlyInit)
        throws Exception
    {
        _status = PREPARED_STATUS;
        
        DataSet dataSet = destination.getDataSet();
        
        if (_unit.getWriter() != null)
        {
            write(config, scenario, destination);
            
            return;
        }
        
        if (!silent)
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.PREPARING_SQL_FOR_DESTINATION_MSG.getValue()
                            + destination.getName());
        
        if (dataSet == null)
            return;
        
        Alias alias = (Alias)config
                .getConnectionFactory()
                .getConnectionParams(
                        Utils.isNothing(destination.getConnectionName()) ? EtlConfig.DEST_CONNECTION_NAME
                                : destination.getConnectionName());
        
        String code = "";
        
        if (!Utils.isNothing(destination.getMetadataExtractorClass())
                && destination.getMetaData() == null)
        {
            dataSet.setTableName(dataSet.getDriver().getTableName(
                    destination.getObjectName()));
            
            MetadataExtractor metadataExtractor = (MetadataExtractor)ObjectFactory
                    .instance().get(destination.getMetadataExtractorClass(),
                            true);
            if (metadataExtractor != null)
            {
                Map<String, FieldDef> metadata = null;
                Map<String, FieldDef> sqlMetadata = null;
                
                if (dataSet.getFieldCount() == 0)
                {
                    sqlMetadata = metadataExtractor
                            .getMetaData(
                                    !Utils.isNothing(dataSet.getTableName()) ? dataSet
                                            .getTableName() : dataSet.getName(),
                                    dataSet.getConnection(),
                                    dataSet.getDriver(),
                                    Utils.isNothing(destination.getCursorSql()) ? destination
                                            .getCursorTableName() : destination
                                            .getCursorSql(), destination
                                            .getUseMetadataDataTypes(), false);
                    
                    if (sqlMetadata != null && sqlMetadata.size() > 0)
                    {
                        List<FieldDef> fields = new ArrayList<FieldDef>(
                                sqlMetadata.values());
                        
                        Collections.sort(fields, new Comparator<FieldDef>()
                        {
                            public int compare(FieldDef f1, FieldDef f2)
                            {
                                return f1.getIndex() - f2.getIndex();
                            }
                            
                        });
                        
                        for (FieldDef fieldDef : fields)
                        {
                            dataSet.addField(fieldDef);
                        }
                    }
                }
                
                metadata = metadataExtractor.getMetaData(
                        !Utils.isNothing(dataSet.getTableName()) ? dataSet
                                .getTableName() : dataSet.getName(), dataSet
                                .getConnection(), dataSet.getDriver(), null,
                        destination.getUseMetadataDataTypes(), false);
                
                // there is no table, so lets create it
                if (metadata == null)
                {
                    destination.updateFields(dataSet.getDriver());
                    
                    String createTableSql = dataSet.getDriver()
                            .getCreateTableSql(
                                    dataSet.getTableName(),
                                    dataSet,
                                    false,
                                    alias != null ? alias.getJdbcDriverClass()
                                            : null, config);
                    
                    addScriptToInit(destination, createTableSql, true,
                            Destination.TABLE_TYPE);
                    
                    // will try to get an sql to create indexes. If it fails -
                    // log and ignore
                    if (destination.isCreateIndexes()
                            && destination.getSource() != null
                            && destination.getSource().getConnection() != null
                            && !Utils.isNothing(destination.getSource()
                                    .getObjectName()))
                    {
                        String sourceDriverClassName = destination.getSource()
                                .getDriverClassName();
                        
                        EtlFactory etlFactory = new EtlFactory();
                        
                        try
                        {
                            ConnectionParams connectionParams = config
                                    .getConnectionFactory()
                                    .getConnectionParams(
                                            destination.getSource()
                                                    .getConnection());
                            
                            UnitLoader unitLoader = null;
                            
                            DriverUnit driverUnit = null;
                            
                            if (connectionParams != null)
                            {
                                unitLoader = SharedUnitLoader.instance();
                                
                                driverUnit = (DriverUnit)unitLoader
                                        .getUnit(Driver.class);
                            }
                            
                            Driver sourceDriver = etlFactory.getDriver(
                                    sourceDriverClassName,
                                    driverUnit,
                                    connectionParams != null ? connectionParams
                                            .getUniqueProperty() : null);
                            
                            if (sourceDriver != null)
                            {
                                String metadataClassName = sourceDriver
                                        .getMetadataClassName();
                                
                                JdbcMetadata jdbcMetadata = (JdbcMetadata)ObjectFactory
                                        .instance()
                                        .get(metadataClassName, true);
                                
                                String[] tokens = jdbcMetadata
                                        .getCatalogSchemaAndPattern(destination
                                                .getSource().getObjectName());
                                
                                String catalog = tokens[0];
                                String schema = tokens[1];
                                String pattern = tokens[2];
                                
                                DummyConnectionFactory connectionFactory = new DummyConnectionFactory(
                                        destination.getSource().getConnection());
                                
                                jdbcMetadata.init(connectionFactory,
                                        connectionFactory, sourceDriver);
                                
                                String indexes = jdbcMetadata.getIndexesAsText(
                                        catalog, schema, pattern,
                                        Metadata.TYPE_INDEXES,
                                        dataSet.getTableName(),
                                        destination.getIndexSuffix(), true,
                                        true);
                                
                                if (!Utils.isNothing(indexes))
                                {
                                    addScriptToInit(destination, indexes,
                                            false, null);
                                }
                                
                            }
                        }
                        catch (Throwable ex)
                        {
                            Logger.log(Logger.SEVERE, EtlLogger.class,
                                    EtlResource.ERROR_GETTING_METADATA
                                            .getValue(), ex);
                        }
                    }
                    
                    metadata = sqlMetadata != null ? sqlMetadata
                            : new ListHashMap<String, FieldDef>();
                }
                
                destination.setMetaData(metadata);
            }
        }
        
        if (onlyInit)
            return;
        
        if (destination.getVariables() != null
                && destination.getVariables().size() > 0
                && dataSet.getDriver().supportsCallableStatement())
            for (int i = 0; i < destination.getVariables().size(); i++)
            {
                Variable var = destination.getVariables().get(i);
                
                if (_variables.contains(var.getName().toUpperCase()))
                    continue;
                
                FieldDef fieldDef = dataSet.getFieldDef(var.getName());
                
                if (fieldDef == null && !Utils.isNothing(var.getFieldName()))
                    fieldDef = dataSet.getFieldDef(var.getFieldName());
                
                String varTypeName = var.getType();
                
                if (Utils.isNothing(varTypeName))
                {
                    if (destination.getMetaData() != null
                            && destination.getMetaData().size() > 0
                            && fieldDef != null)
                    {
                        if (fieldDef != null)
                        {
                            int type = fieldDef.getSqlDataType();
                            
                            if (SqlUtils.isLargeObject(type)
                                    && dataSet.getDriver()
                                            .supportsBinaryInProc())
                            {
                                varTypeName = dataSet.getDriver().getType(
                                        fieldDef,
                                        alias != null ? alias
                                                .getJdbcDriverClass() : null,
                                        config);
                            }
                        }
                        
                        if (Utils.isNothing(varTypeName))
                        {
                            
                            String fieldName = fieldDef.getName();
                            
                            FieldDef field = destination.getMetaData().get(
                                    fieldName);
                            
                            varTypeName = field != null ? field
                                    .getNativeDataType() : null;
                            
                            if (fieldName.equalsIgnoreCase(varTypeName))
                                varTypeName = null;
                        }
                    }
                    
                    if (Utils.isNothing(varTypeName))
                    {
                        if (fieldDef != null)
                        {
                            int type = fieldDef.getSqlDataType();
                            
                            if (!SqlUtils.isLargeObject(type)
                                    || dataSet.getDriver()
                                            .supportsBinaryInProc())
                                varTypeName = dataSet.getDriver().getType(
                                        fieldDef,
                                        alias != null ? alias
                                                .getJdbcDriverClass() : null,
                                        config);
                        }
                        else
                            varTypeName = dataSet.getDriver().getDefaultType();
                    }
                    
                    var.setType(varTypeName);
                }
                
                if (!Utils.isNothing(varTypeName) && var.isDeclare())
                    code = code
                            + (Utils.isNothing(var.getDeclare()) ? dataSet
                                    .getDriver().getVarDeclare() : var
                                    .getDeclare())
                            + dataSet.getDriver().getVarName(var.getName())
                            + " " + varTypeName
                            + dataSet.getDriver().getDelimiter() + "\n";
                
                _variables.add(var.getName().toUpperCase());
            }
        
        if (scenario.getVariables() != null)
            for (int i = 0; i < scenario.getVariables().size(); i++)
            {
                Variable var = scenario.getVariables().get(i);
                
                if (!var.isGlobal()
                        || _variables.contains(var.getName().toUpperCase()))
                    continue;
                
                String varTypeName = var.getType();
                
                if (Utils.isNothing(varTypeName) || !var.isDeclare())
                    continue;
                
                code = code
                        + (Utils.isNothing(var.getDeclare()) ? dataSet
                                .getDriver().getVarDeclare() : var.getDeclare())
                        + dataSet.getDriver().getVarName(var.getName()) + " "
                        + varTypeName + dataSet.getDriver().getDelimiter()
                        + "\n";
                
                _variables.add(var.getName().toUpperCase());
                
                if (_globalVariables.containsKey(var.getName().toUpperCase()))
                    continue;
                
                String varSql = var.getCode();
                
                if (Utils.isNothing(varSql))
                    continue;
                
                _globalVariables.put(var.getName().toUpperCase(), varSql);
            }
        
        String scr = "";
        
        _declareScript = _declareScript + code
                + dataSet.getDriver().getPostDeclareSql();
        
        String sql = destination.getSql();
        int rCount = dataSet.getRecordCount();
        boolean isFromCursor = (!Utils.isNothing(destination
                .getCursorTableName()) || !Utils.isNothing(destination
                .getCursorSql()))
                && dataSet.getDriver().supportsCallableStatement();
        
        // hand written sql
        if (!Utils.isNothing(sql))
        {
            if (rCount == 0 || isFromCursor)
            {
                scr = getInLineSql(config, destination, null, isFromCursor,
                        scenario, 0);
                
                // add to declare
                if (destination.isProcOrFunc())
                    if (dataSet.getDriver().supportsInnerFunctions())
                        _declareProcScript = _declareProcScript + scr;
                    else
                        addScriptToCreate(destination, scr);
            }
            else
                for (int i = 0; i < rCount; i++)
                {
                    if (!silent && config.getLogStep() > 0
                            && (i % config.getLogStep()) == 0)
                        Logger.log(
                                Logger.INFO,
                                EtlLogger.class,
                                EtlResource.DEST_LINE_MSG.getValue()
                                        + destination.getName() + " "
                                        + String.valueOf(i + 1) + " out of "
                                        + dataSet.getRecordCount());
                    
                    scr = getInLineSql(config, destination,
                            dataSet.getRecord(i), false, scenario, i);
                    
                    // add to declare
                    if (destination.isProcOrFunc())
                        if (dataSet.getDriver().supportsInnerFunctions())
                            _declareProcScript = _declareProcScript + scr;
                        else
                            addScriptToCreate(destination, scr);
                }
        }
        // auto sql
        else
        {
            if (isFromCursor)
                getAutoSql(config, destination, null, isFromCursor, scenario, 0);
            else
                for (int i = 0; i < rCount; i++)
                {
                    if (!silent && config.getLogStep() > 0
                            && (i % config.getLogStep()) == 0)
                        Logger.log(
                                Logger.INFO,
                                EtlLogger.class,
                                EtlResource.DEST_LINE_MSG.getValue()
                                        + destination.getName() + " "
                                        + String.valueOf(i + 1) + " out of "
                                        + dataSet.getRecordCount());
                    
                    getAutoSql(config, destination, dataSet.getRecord(i),
                            false, scenario, i);
                }
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.CodeGen#reset()
     */
    public void reset()
    {
        _variables.clear();
        _declareScript = "";
        _declareProcScript = "";
        _declareCursorVarScript = "";
        _declareCursorScript = "";
        _declareCursorEndScript = "";
        _currentExecutingScript = "";
        _scrIndex = 0;
        _scriptsToInit = null;
        _scriptsToCreate = null;
        _scriptsToCleanUpBefore = null;
        _unit = null;
        _assembledScripts.clear();
        _status = CREATED_STATUS;
    }
    
    /**
     * Sets the current code.
     * 
     * @param driver
     *            the driver
     * @param name
     *            the name
     * @param index
     *            the index
     * @return the string
     */
    public String setCurrentCode(Driver driver, String name, int index)
    {
        String script = _assembledScripts.get(index).getKey();
        
        if (driver.supportsCallableStatement()
                && !driver.supportsAnonymousBlocks())
            script = Utils.findAndReplace(script, EtlConfig.ETL_CODE,
                    driver.getIdentifierName(name, Driver.PROC_TYPE), true);
        
        _currentExecutingScript = script;
        
        _scrIndex = index;
        
        return script;
    }
    
    /**
     * Updates the large (CLOR and BLOB) objects.
     * 
     * @param destination
     *            the destination
     * @param driver
     *            the driver
     * @param preparedStatement
     *            the prepared statement
     * @param row
     *            the row
     * @throws Exception
     *             in case of any error
     */
    private void setLargeObjects(Destination destination, Driver driver,
            PreparedStatement preparedStatement, int row)
        throws Exception
    {
        if (row < 0
                || destination.getDataSet() == null
                || destination.getDataSet().getRecordCount() == 0
                || Destination.LOAD_DELETE.equalsIgnoreCase(destination
                        .getLoadAction()))
            return;
        
        int fldCount = destination.getDataSet().getFieldCount();
        int recordCount = destination.getDataSet().getRecordCount();
        
        int pos = 1;
        
        DataSetRecord record = destination.getDataSet().getRecord(
                row < recordCount ? row : 0);
        
        int series = Destination.LOAD_MERGE.equalsIgnoreCase(destination
                .getLoadAction()) ? 2 : 1;
        
        for (int index = 0; index < series; index++)
            for (int col = 0; col < fldCount; col++)
            {
                FieldDef fieldDef = destination.getDataSet().getFieldDef(col);
                
                if (fieldDef.isBlob())
                {
                    driver.setBlob(preparedStatement, record.get(col), pos++);
                }
                else if (fieldDef.isClob())
                {
                    driver.setClob(preparedStatement, record.get(col), pos++);
                }
            }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.CodeGen#setUnit(com.toolsverse.etl.core
     * .engine.EtlUnit)
     */
    public void setUnit(EtlUnit unit)
    {
        _unit = unit;
    }
    
    /**
     * Writes the file based data set into the hard drive using configured data
     * writer.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param destination
     *            the destination
     * @throws Exception
     *             in case of any error
     */
    private void write(EtlConfig config, Scenario scenario,
            Destination destination)
        throws Exception
    {
        DataSet dataSet = destination.getDataSet();
        
        if (dataSet == null)
            return;
        
        boolean hasVars = destination.getVariables() != null
                && destination.getVariables().size() > 0;
        
        if (!_unit.getWriterParams().isPrePersistOccured())
        {
            destination.updateFields(dataSet.getDriver());
            
            _unit.getWriter().prePersist(_unit.getWriterParams(), dataSet,
                    dataSet.getDriver());
            
            if (dataSet.isEmpty())
                return;
        }
        
        TaskExecutor taskExecutor = new TaskExecutor();
        
        int rows = dataSet.getRecordCount();
        
        LoadFunctionContext context = null;
        
        for (int row = 0; row < rows; row++)
        {
            DataSetRecord record = dataSet.getRecord(row);
            
            TaskResult taskResult = taskExecutor.executeInlineTasks(config,
                    destination, scenario, row);
            
            if (taskResult != null)
            {
                if (taskResult.getResult() == TaskResult.TaskResultCode.REJECT)
                    continue;
                else if (taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                    return;
            }
            
            if (hasVars)
            {
                context = new LoadFunctionContext();
                context.setCurrentRecord(record);
                context.setDestination(destination);
                context.setScope(Variable.EXECUTE_RUNTIME);
                context.setFromCursor(false);
                context.setRow(row);
                context.setConfig(config);
                
                for (int i = 0; i < destination.getVariables().size(); i++)
                {
                    Variable var = destination.getVariables().get(i);
                    
                    int col = dataSet.getFieldIndex(var.getName());
                    
                    if (Utils.isNothing(var.getFunctionClassName()))
                        var.setFunctionClassName(DefFunctions.class.getName());
                    
                    if (col >= 0)
                    {
                        String varValue = context.execute(var);
                        
                        record.set(col, varValue);
                    }
                }
            }
            
            _unit.getWriter().inlinePersist(_unit.getWriterParams(), dataSet,
                    dataSet.getDriver(), record, -1, -1);
        }
    }
}
