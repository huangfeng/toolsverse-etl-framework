/*
 * NullSecurityProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.Permission;
import java.util.Map;

/**
 * The SecurityManager is always active and must be initialized. So permissions are always checked when user is requesting certain resources. It is not
 * however always require to have a authorization and role-based security. For example single user app doesn't need it. This is where NullSecurityProvider comes into 
 * the play. It basically does nothing and always answers true on the isPermited question. The NullSecurityProvider is automatically instantiated by the SecurityManager
 * for the single user (no server url defined) client (app.deployment=client) application.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class NullSecurityProvider implements SecurityProvider
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.PermissionsProvider#addPermission(java.security
     * .Permission)
     */
    public void addPermission(Permission permission)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.PermissionsProvider#clearPermissions()
     */
    public void clearPermissions()
    {
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
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.PermissionsProvider#getPermissions()
     */
    public Map<String, Permission> getPermissions()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#getRoles()
     */
    public Map<String, Role> getRoles()
    {
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
        return true;
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
        return true;
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
        return true;
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
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#setRoles(java.util.Map)
     */
    public void setRoles(Map<String, Role> value)
    {
    }
}
