/*
 * AliasConnectionProvider.java
 * 
 * Copyright 2010-2012 Toolsvrese. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.SimpleDriver;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The ConnectionProvider implementation which uses Alias as a ConnectionParams
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class AliasConnectionProvider implements ConnectionProvider<Alias>
{
    
    /**
     * Creates the jdbc connection.
     * 
     * @param alias
     *            the alias
     * @return the jdbc connection
     * @throws Exception
     *             in case of any error
     */
    protected Connection createConnection(Alias alias)
        throws Exception
    {
        java.sql.Driver driver = (java.sql.Driver)Class.forName(
                alias.getJdbcDriverClass()).newInstance();
        
        DriverManager.registerDriver(driver);
        
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
            
            return DriverManager.getConnection(url, props);
        }
        else
            return DriverManager.getConnection(url, userId, password);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionProvider#getConnection(com
     * .toolsverse.etl.common.ConnectionParams)
     */
    public Connection getConnection(Alias alias)
        throws Exception
    {
        Connection con = createConnection(alias);
        
        try
        {
            con.setAutoCommit(false);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.INFO, this, Resource.ERROR_GENERAL.getValue(), ex);
        }
        
        if (!Utils.isNothing(alias.getInitSql()))
            SqlUtils.executeSql(
                    con,
                    alias.getInitSql(),
                    null,
                    (Driver)ObjectFactory.instance().get(
                            Driver.class.getName(),
                            SimpleDriver.class.getName(), true));
        
        return con;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionProvider#releaseConnection
     * (java.sql.Connection)
     */
    public void releaseConnection(final Connection con)
        throws Exception
    {
        if (con != null)
        {
            Thread releaseThread = new Thread(new Runnable()
            {
                public void run()
                {
                    try
                    {
                        try
                        {
                            con.commit();
                        }
                        finally
                        {
                            con.close();
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.INFO, this,
                                Resource.ERROR_GENERAL.getValue(), ex);
                    }
                }
            });
            
            releaseThread.start();
        }
        
    }
    
}
