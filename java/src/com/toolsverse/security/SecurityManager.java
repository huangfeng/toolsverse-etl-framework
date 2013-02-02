/*
 * SecurityManager.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.Permission;
import java.util.Map;

import com.toolsverse.security.resource.SecurityResource;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The singleton class which contains all security-checking related functions.
 * Instantiates appropriate SecurityProvider based on the current execution mode
 * (client, client-server, web, etc).
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class SecurityManager implements SecurityProvider
{
    
    /** The ADMINISTRATOR role. */
    public static final String ADMINISTRATOR = "administrator";
    
    /** The_instance of the v. */
    private volatile static SecurityManager _instance;
    
    /**
     * Returns an instance of the SecurityManager.
     * 
     * @return the security manager
     */
    public static SecurityManager instance()
    {
        if (_instance == null)
        {
            synchronized (SecurityManager.class)
            {
                if (_instance == null)
                    _instance = new SecurityManager();
            }
        }
        
        return _instance;
    }
    
    /** The provider. */
    private SecurityProvider _provider;
    
    /**
     * Instantiates a new SecurityManager.
     */
    private SecurityManager()
    {
        initProvider();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.PermissionsProvider#addPermission(java.security
     * .Permission)
     */
    public void addPermission(Permission permission)
    {
        if (_provider == null)
            initProvider();
        
        if (_provider != null)
            _provider.addPermission(permission);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.PermissionsProvider#clearPermissions()
     */
    public void clearPermissions()
    {
        if (_provider == null)
            initProvider();
        
        if (_provider != null)
            _provider.clearPermissions();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.PermissionsProvider#deletePermission(java.security
     * .Permission)
     */
    public void deletePermission(Permission permission)
    {
        if (_provider == null)
            initProvider();
        
        if (_provider != null)
            _provider.deletePermission(permission);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.PermissionsProvider#getPermissions()
     */
    public Map<String, Permission> getPermissions()
    {
        if (_provider == null)
            initProvider();
        
        if (_provider != null)
            return _provider.getPermissions();
        else
            return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#getRoles()
     */
    public Map<String, Role> getRoles()
    {
        if (_provider == null)
            initProvider();
        
        if (_provider != null)
            return _provider.getRoles();
        else
            return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#hasRole(java.lang.String,
     * com.toolsverse.security.SecurityContext)
     */
    public boolean hasRole(String role, SecurityContext context)
    {
        if (_provider == null)
            initProvider();
        
        return _provider != null && _provider.hasRole(role, context);
    }
    
    /**
     * Instantiates the SecurityProvider.
     */
    private void initProvider()
    {
        try
        {
            _provider = (SecurityProvider)ObjectFactory.instance().get(
                    SecurityProvider.class.getName());
        }
        catch (RuntimeException ex)
        {
            
            Logger.log(Logger.SEVERE, getClass(),
                    SecurityResource.CANNOT_INIT_NO_PROVIDER.getValue());
            
            throw ex;
        }
    }
    
    /**
     * Checks if SecurityProvider is initialized.
     * 
     * @return true, if is initialized
     */
    public boolean isInitialized()
    {
        return _provider != null;
    }
    
    /**
     * Checks if NullSecurityProvider is used (no security).
     * 
     * @return true, if NullSecurityProvider is used
     */
    public boolean isNullProvider()
    {
        return _provider == null || _provider instanceof NullSecurityProvider;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.SecurityProvider#isPermitted(java.security.Permission
     * , com.toolsverse.security.SecurityContext)
     */
    public boolean isPermitted(Permission permission, SecurityContext context)
    {
        if (_provider == null)
            initProvider();
        
        return _provider != null && _provider.isPermitted(permission, context);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.SecurityProvider#isPermitted(com.toolsverse.security
     * .SecuritySubject, com.toolsverse.security.SecurityContext)
     */
    public boolean isPermitted(SecuritySubject subject, SecurityContext context)
    {
        if (_provider == null)
            initProvider();
        
        return _provider != null && _provider.isPermitted(subject, context);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.SecurityProvider#isPermitted(java.lang.String,
     * com.toolsverse.security.SecurityContext)
     */
    public boolean isPermitted(String permissionName, SecurityContext context)
    {
        if (_provider == null)
            initProvider();
        
        return _provider != null
                && _provider.isPermitted(permissionName, context);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#setRoles(java.util.Map)
     */
    public synchronized void setRoles(Map<String, Role> value)
    {
        if (_provider == null)
            initProvider();
        
        if (_provider != null)
            _provider.setRoles(value);
    }
}
