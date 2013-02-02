/*
 * PermissionsProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.Permission;
import java.util.Map;

/**
 * The interface for all classes which need to manage permissions.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public interface PermissionsProvider
{
    
    /**
     * Adds the permission.
     *
     * @param permission the permission
     */
    void addPermission(Permission permission);
    
    /**
     * Clear permissions.
     */
    void clearPermissions();
    
    /**
     * Deletes permission.
     *
     * @param permission the permission
     */
    void deletePermission(Permission permission);
    
    /**
     * Gets the permissions.
     *
     * @return the permissions
     */
    Map<String, Permission> getPermissions();
}