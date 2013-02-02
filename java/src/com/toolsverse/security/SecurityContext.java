/*
 * SecurityContext.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.util.HashSet;
import java.util.Set;

import com.toolsverse.mvc.model.Getter;
import com.toolsverse.mvc.model.ModelImpl;
import com.toolsverse.mvc.model.Setter;

/**
 * This class contains information needed by the security subsystem to uniquely identify user. It also contains all roles assigned to the user.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class SecurityContext extends ModelImpl
{
    
    /** CREDENTIALS. */
    public static final String CREDENTIALS = "seccredentials";
    
    /** PRINCIPALS. */
    public static final String PRINCIPALS = "secprincipas";
    
    /** ROLES. */
    public static final String ROLES = "secroles";
    
    /** the security context associated with the current thread . */
    private static final ThreadLocal<SecurityContext> currentSecurityContext = new InheritableThreadLocal<SecurityContext>();
    
    /**
     * Gets the security context associated with the current thread
     *
     * @return the security context associated with the current thread
     */
    public static SecurityContext getCurrentSecurityContext()
    {
        return currentSecurityContext.get();
    }
    
    /**
     * Sets the current security context.
     *
     * @param value the new current security context
     */
    public static void setCurrentSecurityContext(SecurityContext value)
    {
        currentSecurityContext.set(value);
    }
    
    /**
     * Instantiates a new security context.
     */
    public SecurityContext()
    {
        setRoles(new HashSet<String>());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SecurityContext other = (SecurityContext)obj;
        if (getCredentials() == null)
        {
            if (other.getCredentials() != null)
                return false;
        }
        else if (!getCredentials().equals(other.getCredentials()))
            return false;
        return true;
    }
    
    /**
     * Gets the credentials.
     *
     * @return the credentials
     */
    @Getter(name = CREDENTIALS)
    public SecurityCredentials getCredentials()
    {
        return (SecurityCredentials)getAttributeValue(CREDENTIALS);
    }
    
    /**
     * Gets the principals.
     *
     * @return the principals
     */
    @Getter(name = PRINCIPALS)
    public SecurityPrincipals getPrincipals()
    {
        return (SecurityPrincipals)getAttributeValue(PRINCIPALS);
    }
    
    /**
     * Gets the roles associated with the security context
     *
     * @return the roles
     */
    @SuppressWarnings("unchecked")
    @Getter(name = ROLES)
    public Set<String> getRoles()
    {
        return (Set<String>)getAttributeValue(ROLES);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((getCredentials() == null) ? 0 : getCredentials().hashCode());
        return result;
    }
    
    /**
     * Checks if security context has the role.
     *
     * @param role the role
     * @return true, if successful
     */
    public boolean hasRole(String role)
    {
        return getRoles() != null && getRoles().contains(role);
    }
    
    /**
     * Checks if is administrator.
     *
     * @return true, if is administrator
     */
    public boolean isAdministrator()
    {
        return hasRole(SecurityManager.ADMINISTRATOR);
    }
    
    /**
     * Sets the credentials.
     *
     * @param value the new credentials
     */
    @Setter(name = CREDENTIALS)
    public void setCredentials(SecurityCredentials value)
    {
        setAttributeValue(CREDENTIALS, value);
    }
    
    /**
     * Sets the principals.
     *
     * @param value the new principals
     */
    @Setter(name = PRINCIPALS)
    public void setPrincipals(SecurityPrincipals value)
    {
        setAttributeValue(PRINCIPALS, value);
    }
    
    /**
     * Sets the roles for the sercurity context
     *
     * @param value the new roles
     */
    @Setter(name = ROLES)
    public void setRoles(Set<String> value)
    {
        setAttributeValue(ROLES, value);
    }
}
