/*
 * SqlParser.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.parser;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.driver.Driver;

/**
 * The sql grammar parser.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface SqlParser extends Serializable
{
    
    /**
     * Creates a variable using given name and a driver. The variable is used to register output parameter for the callable sql statement.
     * <p>The expected format: out_type_name.
     * <p>Examples:
     * <p>out_number_abc
     * <p>out_cursor_xyz
     *
     * @param name the name
     * @param driver the driver
     * @return the variable
     */
    Variable getVariable(String name, Driver driver);
    
    /**
     * Checks if sql statement is callable. The callable statement is an anonymous sql block, function or a procedure.
     *
     * @param sql the sql
     * @return true, if sql statement is callable
     */
    boolean isCallable(String sql);
    
    /**
     * Parses the sql, updates params, paramMap and outputParams.
     *
     * @param sql the sql
     * @param params the bind variables. 
     * @param paramMap the map containing bind variable name name as key and a list of parameter indexes as a value. 
     * <p>For example:
     * <p><pre>
     *     select * from abc
     *     where xyz = :xyz and mmm = :xyz
     * </pre> 
     * <p>the map will contain "xyz" as a key and a list {1,2} as a value.
     * @param outputParams the output params
     * @param driver the driver
     * @return the string
     */
    String parseParams(String sql, Map<String, Object> params,
            Map<String, List<Integer>> paramMap, List<String> outputParams,
            Driver driver);
    
    /**
     * Removes chars prohibited in the sql.
     *
     * @param sql the sql
     * @return the sql
     */
    String removeChars(String sql);
    
    /**
     * Sets the bind variables for the given prepared statement, including registering output parameters if any exist. 
     * Properties is a map containing variable name\value pairs.
     *
     * @param st the prepared statement
     * @param properties the properties
     * @param paramMap the map containing bind variable name name as key and a list of parameter indexes as a value. 
     * <p>For example:
     * <p><pre>
     *     select * from abc
     *     where xyz = :xyz and mmm = :xyz
     * </pre> 
     * <p>the map will contain "xyz" as a key and a list {1,2} as a value.
     * 
     * @param outpurParams the list of output parameters, if any exist
     * @param driver the driver
     * @throws Exception in case of any error
     */
    void setBindVariables(PreparedStatement st, Map<String, Object> properties,
            Map<String, List<Integer>> paramMap, List<String> outpurParams,
            Driver driver)
        throws Exception;
    
    /**
     * Sets the bind variables using given properties. The bind variable can be defined inside sql as :name. Example: :abc
     * Properties is a map containing variable name\value pairs. Ignores ":name" in comments and parameters.
     *
     * @param sql the sql
     * @param properties the properties
     * @return the original sql with bind variables substituted on vales. 
     */
    String setBindVariables(String sql, Map<String, Object> properties);
    
    /**
     * Splits given sql on multiple sql statements using ";" as a separator. Ignores ";" in comments and parameters.
     *
     * @param sql the sql
     * @return the sql statements
     */
    String[] split(String sql);
    
    /**
     * Strip comments.
     *
     * @param sql the sql
     * @return the string
     */
    String stripComments(String sql);
    
}