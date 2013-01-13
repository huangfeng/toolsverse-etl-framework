/*
 * SqlService.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.service;

import java.util.Map;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.sql.connection.SessionConnectionProvider;
import com.toolsverse.service.Service;

/**
 * SqlService provides support for execution of the sql queries.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface SqlService extends Service
{
    // types of the sql
    
    /** The unknown sql type. */
    static final int NOTHING_TYPE = 0;
    
    /** The SELECT_TYPE. */
    static final int SELECT_TYPE = 1;
    
    /** The generic DML_TYPE. */
    static final int DML_TYPE = 2;
    
    /** The UPDATE_TYPE. */
    static final int UPDATE_TYPE = 3;
    
    /** The INSERT_TYPE. */
    static final int INSERT_TYPE = 4;
    
    /** The DELETE_TYPE. */
    static final int DELETE_TYPE = 5;
    
    /** The MERGE_TYPE. */
    static final int MERGE_TYPE = 6;
    
    /** The EXECUTE_TYPE. */
    static final int EXECUTE_TYPE = 7;
    
    /** The EXECUTE_SCRIPT_TYPE. */
    static final int EXECUTE_SCRIPT_TYPE = 8;
    
    /** The Constant OUTPUT_PARAM_TYPE. */
    static final int OUTPUT_PARAM_TYPE = 9;
    
    /** The EXPLAIN_PLAN_TYPE. */
    static final int EXPLAIN_PLAN_TYPE = 10;
    
    /** The EXCEPTION_TYPE. */
    static final int EXCEPTION_TYPE = 11;
    
    /**
     * Cancels currently executed sql. Must be supported by jdbc driver.
     *
     * @param provider the connection provider
     */
    void cancel(SessionConnectionProvider<Alias> provider);
    
    /**
     * Commits current transaction.
     *
     * @param provider the connection provider
     * @param params the alis
     * @param closeOnCommit the close on commit flag. If set the connection will be released on commit
     * @throws Exception the exception
     */
    void commitTransaction(SessionConnectionProvider<Alias> provider,
            Alias params, boolean closeOnCommit)
        throws Exception;
    
    /**
     * Executes explain plan if supported by driver passed in the sql request.
     *
     * @param sqlRequest the sql request
     * @return the sql result
     * @throws Exception in case of any error
     */
    SqlResult executeExplainPlan(SqlRequest sqlRequest)
        throws Exception;
    
    /**
     * Executes sql using external tool if supported by driver passed in the sql request.
     *
     * @param sqlRequest the sql request
     * @return the sql result
     * @throws Exception in case of any error
     */
    SqlResult executeExternal(SqlRequest sqlRequest)
        throws Exception;
    
    /**
     * Executes sql script, for example anonymous sql block if supported by driver passed in the sql request.
     *
     * @param sqlRequest the sql request
     * @return the sql result
     * @throws Exception in case of any error
     */
    SqlResult executeScript(SqlRequest sqlRequest)
        throws Exception;
    
    /**
     * Executes sql.
     *
     * @param sqlRequest the sql request
     * @return the sql result
     * @throws Exception in case of any error
     */
    SqlResult executeSql(SqlRequest sqlRequest)
        throws Exception;
    
    /**
     * Gets the meta data for the database object (table, view or synonym).
     *
     * @param sqlRequest the sql request
     * @return the meta data
     * @throws Exception the exception
     */
    Map<String, FieldDef> getMetaData(SqlRequest sqlRequest)
        throws Exception;
    
    /**
     * Releases connection.
     *
     * @param provider the connection provider
     * @param params the alias
     * @throws Exception in case of any error
     */
    void releaseConnection(SessionConnectionProvider<Alias> provider,
            Alias params)
        throws Exception;
    
    /**
     * Rollbacks current transaction.
     *
     * @param provider the connection provider
     * @param params the alias
     * @param closeOnRollback the close on rollback flag. If set the connection will be closed on rollback
     * @throws Exception in case of any error
     */
    void rollbackTransaction(SessionConnectionProvider<Alias> provider,
            Alias params, boolean closeOnRollback)
        throws Exception;
    
}
