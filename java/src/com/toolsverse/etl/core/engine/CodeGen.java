/*
 * CodeGen.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.Connection;
import java.util.List;

import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.driver.Driver;

/**
 * <code>CodeGen</code> is an interface for code generators. ETL framework
 * generates and executes code for the etl scenario. Code is a usually vendor specific SQL such as Oracle
 * PL/SQL, Microsoft Transact SQL, etc.  The call sequence for the typical
 * scenario is the following: <code>prepare(...) is called 
 * for each destination. The resulting code saved internally, <code>assembleCode(...) assembles final 
 * code by concatenating code for all destinations with added declarations, etc so it can be be executed in the
 * designated database, <code>execute(...)</code> executes it.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface CodeGen
{
    
    /** The code generator created . */
    final static int CREATED_STATUS = 0;
    
    /** The code is prepared . */
    final static int PREPARED_STATUS = 1;
    
    /** The code is executed . */
    final static int EXECUTED_STATUS = 2;
    
    /**
     * Assembles prepared code. The idea is the following:
     * <code>prepare(...) is called 
     * for each destination. The resulting code saved internally. <code>assembleCode(...) assembles final 
     * code by concatenating code for all destinations with added declarations, etc so it can be be executed in the
     * designated database. Also it splits code on chunks if necessary. For example Oracle limits size of the PL\SQL block
     * to approximately 3000 lines.
     * 
     * @param config
     *            The config
     * @param scenario
     *            The scenario
     * @param driver
     *            The driver
     * @param loadIndex
     *            If equals to <code>0</code> this chunk will be executed first.
     * @param silent
     *            If <code>true</code> logging is disabled for everything but
     *            exceptions
     * 
     * @throws Exception in case of any error
     */
    void assembleCode(EtlConfig config, Scenario scenario, Driver driver,
            int loadIndex, boolean silent)
        throws Exception;
    
    /**
     * Clean up after last block of code is executed.
     *
     * @param config the config
     * @param scenario the scenario
     * @param driver the driver
     * @param conn the connection
     * @throws Exception the exception in case of any error
     */
    void cleanUp(EtlConfig config, Scenario scenario, Driver driver,
            Connection conn)
        throws Exception;
    
    /**
     * Clean up database objects on exception.
     *
     * @param config the config
     * @param scenario the scenario
     * @param driver the driver
     * @param cleanUpConn the clean up connection
     * @throws Exception the exception
     */
    void cleanUpOnException(EtlConfig config, Scenario scenario, Driver driver,
            Connection cleanUpConn)
        throws Exception;
    
    /**
     * Copy parameters from other code generator.
     *
     * @param codeGen the code generator
     */
    void copy(CodeGen codeGen);
    
    /**
     * Executes code. Code for each destination should be already prepared and
     * stored internally.
     *
     * @param config The etl config
     * @param scenario The scenario
     * @param driver The driver
     * @param silent If <code>true</code> logging is disabled for everything but
     * exceptions
     * @param conn The JDBC connection
     * @param cleanUpConn the clean up connection
     * @param destination the destination
     * @throws Exception in case of any error
     */
    void execute(EtlConfig config, Scenario scenario, Driver driver,
            boolean silent, Connection conn, Connection cleanUpConn,
            Destination destination)
        throws Exception;
    
    /**
     * Gets the list of scripts which should be executed to clean up database object on exception.
     *
     * @return the list of scripts to clean up on exception
     */
    List<String> getScriptsToCleanOnException();
    
    /**
     * Gets the status of the code generator.
     *
     * @return the status
     */
    int getStatus();
    
    /**
     * Gets the current unit. 
     * 
     * @return the unit
     * 
     * @see com.toolsverse.etl.core.engine.EtlUnit
     */
    EtlUnit getUnit();
    
    /**
     * Prepares code for the destination using given config and scenario.
     *
     * @param config The etl config
     * @param scenario The scenario
     * @param destination The destination
     * @param silent If <code>true</code> logging is disabled for everything but
     * exceptions
     * @param onlyInit the only init
     * @throws Exception in case of any error
     */
    void prepare(EtlConfig config, Scenario scenario, Destination destination,
            boolean silent, boolean onlyInit)
        throws Exception;
    
    /**
     * Resets the status.
     */
    void reset();
    
    /**
     * Sets the current unit.
     * 
     * @param unit
     *            the new unit
     * 
     * @see com.toolsverse.etl.core.engine.EtlUnit
     */
    void setUnit(EtlUnit unit);
    
}