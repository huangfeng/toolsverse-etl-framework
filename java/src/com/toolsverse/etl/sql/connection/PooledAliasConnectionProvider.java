/*
 * PooledAliasConnectionProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.util.Utils;

/**
 * The AliasConnectionProvider which uses configurable connections pool. When new connection is created it gets added to the pool and when it is released 
 * it returned back to the pool so others can reuse it. Apache dbcp is used as a connections pool.
 *
 * @see com.toolsverse.etl.sql.connection.AliasConnectionProvider
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class PooledAliasConnectionProvider extends AliasConnectionProvider
{
    // the connections pool properties
    
    /** The MAX_ACTIVE property. */
    public static final String MAX_ACTIVE = "pool.max.active";
    
    /** The WHEN_EXHAUSTED_ACTION property. */
    public static final String WHEN_EXHAUSTED_ACTION = "pool.when.exhausted.action";
    
    /** The MAX_WAIT property. */
    public static final String MAX_WAIT = "pool.max.wait";
    
    /** The MAX_IDLE property. */
    public static final String MAX_IDLE = "pool.max.idle";
    
    /** The MIN_IDLE property. */
    public static final String MIN_IDLE = "pool.min.idle";
    
    /** The TEST_ON_BORROW property. */
    public static final String TEST_ON_BORROW = "pool.test.on.borrow";
    
    /** The Constant TEST_ON_RETURN. */
    public static final String TEST_ON_RETURN = "pool.test.on.return";
    
    /** The TIME_BETWEEN_EVICTION_RUNS_MILLIS property. */
    public static final String TIME_BETWEEN_EVICTION_RUNS_MILLIS = "pool.time.between.eviction.runs.millis";
    
    /** The NUM_TESTS_PER_EVICTION_RUN property. */
    public static final String NUM_TESTS_PER_EVICTION_RUN = "pool.num.tests.per.eviction.run";
    
    /** The MIN_EVICTABLE_IDLE_TIME_MILLIS property. */
    public static final String MIN_EVICTABLE_IDLE_TIME_MILLIS = "pool.min.evictable.idle.time.millis";
    
    /** The TEST_WHILE_IDLE property. */
    public static final String TEST_WHILE_IDLE = "pool.test.while.idle";
    
    /** The config. */
    private GenericObjectPool.Config _config;
    
    /**
     * Instantiates a new pooled alias connection provider.
     */
    public PooledAliasConnectionProvider()
    {
        _config = new GenericObjectPool.Config();
        
        _config.maxActive = Utils.str2Int(SystemConfig.instance()
                .getSystemProperty(MAX_ACTIVE),
                GenericObjectPool.DEFAULT_MAX_ACTIVE);
        
        _config.whenExhaustedAction = Utils.str2Byte(SystemConfig.instance()
                .getSystemProperty(WHEN_EXHAUSTED_ACTION),
                GenericObjectPool.WHEN_EXHAUSTED_GROW);
        
        _config.maxWait = Utils.str2Long(SystemConfig.instance()
                .getSystemProperty(MAX_WAIT), 1000 * 30);
        
        _config.maxIdle = Utils.str2Int(SystemConfig.instance()
                .getSystemProperty(MAX_IDLE),
                GenericObjectPool.DEFAULT_MAX_IDLE);
        
        _config.minIdle = Utils.str2Int(SystemConfig.instance()
                .getSystemProperty(MIN_IDLE),
                GenericObjectPool.DEFAULT_MIN_IDLE);
        
        _config.testOnBorrow = Utils.str2Boolean(SystemConfig.instance()
                .getSystemProperty(TEST_ON_BORROW),
                GenericObjectPool.DEFAULT_TEST_ON_BORROW);
        
        _config.testOnReturn = Utils.str2Boolean(SystemConfig.instance()
                .getSystemProperty(TEST_ON_RETURN),
                GenericObjectPool.DEFAULT_TEST_ON_RETURN);
        
        _config.timeBetweenEvictionRunsMillis = Utils.str2Long(SystemConfig
                .instance()
                .getSystemProperty(TIME_BETWEEN_EVICTION_RUNS_MILLIS),
                GenericObjectPool.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        
        _config.numTestsPerEvictionRun = Utils.str2Int(SystemConfig.instance()
                .getSystemProperty(NUM_TESTS_PER_EVICTION_RUN),
                GenericObjectPool.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
        
        _config.minEvictableIdleTimeMillis = Utils.str2Long(SystemConfig
                .instance().getSystemProperty(MIN_EVICTABLE_IDLE_TIME_MILLIS),
                GenericObjectPool.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        
        _config.testWhileIdle = Utils.str2Boolean(SystemConfig.instance()
                .getSystemProperty(TEST_WHILE_IDLE),
                GenericObjectPool.DEFAULT_TEST_WHILE_IDLE);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.AliasConnectionProvider#createConnection
     * (com.toolsverse.etl.common.Alias)
     */
    @Override
    protected Connection createConnection(Alias alias)
        throws Exception
    {
        java.sql.Driver driver = (java.sql.Driver)Class.forName(
                alias.getJdbcDriverClass()).newInstance();
        
        DriverManager.registerDriver(driver);
        
        org.apache.commons.dbcp.ConnectionFactory connectionFactory = null;
        
        Properties props = Utils.getProperties(alias.getParams());
        
        String userId = alias.getUserId();
        String password = alias.getPassword();
        
        String url = alias.getUrl();
        
        if (props.size() > 0)
        {
            if (!Utils.isNothing(userId))
            {
                props.put("user", userId);
                if (!Utils.isNothing(password))
                    props.put("password", password);
            }
            
            connectionFactory = new DriverManagerConnectionFactory(url, props);
        }
        else
            connectionFactory = new DriverManagerConnectionFactory(url, userId,
                    password);
        
        ObjectPool connectionPool = new GenericObjectPool(null, _config);
        
        @SuppressWarnings("unused")
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory, connectionPool, null, null, false, true);
        
        PoolingDataSource poolingDataSource = new PoolingDataSource(
                connectionPool);
        
        return poolingDataSource.getConnection();
    }
}
