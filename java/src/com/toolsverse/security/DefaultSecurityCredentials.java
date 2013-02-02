/*
 * DefaultSecurityCredentials.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import com.toolsverse.mvc.model.Getter;
import com.toolsverse.mvc.model.ModelImpl;
import com.toolsverse.mvc.model.Setter;
import com.toolsverse.security.resource.SecurityResource;
import com.toolsverse.util.Utils;

/**
 * The default implementation of the SecurityCredentials. Includes user name and password.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class DefaultSecurityCredentials extends ModelImpl implements
        SecurityCredentials
{
    
    /** The USER_NAME. */
    public static final String USER_NAME = "credentialsusername";
    
    /** The PASSWORD. */
    public static final String PASSWORD = "credentialspassword";
    
    /** The CONF_PASSWORD. */
    public static final String CONF_PASSWORD = "credentialsconfpassword";
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityCredentials#ckecksOut()
     */
    public String ckecksOut()
    {
        if (Utils.isNothing(getPassword()))
            return SecurityResource.WRONG_PASSWORD_ONLY.getValue();
        else if (!Utils.equals(getPassword(), getConfPassword()))
            return SecurityResource.PASSWORDS_DID_NOT_MATCH.getValue();
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.ModelImpl#clone()
     */
    @Override
    public Object clone()
        throws CloneNotSupportedException
    {
        Object clone = super.clone();
        
        ((DefaultSecurityCredentials)clone).setConfPassword(null);
        
        return clone;
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
        DefaultSecurityCredentials other = (DefaultSecurityCredentials)obj;
        if (getUserName() == null)
        {
            if (other.getUserName() != null)
                return false;
        }
        else if (!getUserName().equals(other.getUserName()))
            return false;
        return true;
    }
    
    /**
     * Gets the confirmation password. Used to double check password entered by the user.
     *
     * @return the confirmation password
     */
    @Getter(name = CONF_PASSWORD)
    public String getConfPassword()
    {
        return (String)getAttributeValue(CONF_PASSWORD);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityCredentials#getId()
     */
    public String getId()
    {
        return getUserName();
    }
    
    /**
     * Gets the password.
     *
     * @return the password
     */
    @Getter(name = PASSWORD)
    public String getPassword()
    {
        return (String)getAttributeValue(PASSWORD);
    }
    
    /**
     * Gets the user name.
     *
     * @return the user name
     */
    @Getter(name = USER_NAME)
    public String getUserName()
    {
        return (String)getAttributeValue(USER_NAME);
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
        result = prime * result
                + ((getUserName() == null) ? 0 : getUserName().hashCode());
        return result;
    }
    
    /**
     * Sets the confirmation password. Used to double check password entered by the user.
     *
     * @param value the new confirmation password
     */
    @Setter(name = CONF_PASSWORD)
    public void setConfPassword(String value)
    {
        setAttributeValue(CONF_PASSWORD, value);
    }
    
    /**
     * Sets the password.
     *
     * @param value the new password
     */
    @Setter(name = PASSWORD)
    public void setPassword(String value)
    {
        setAttributeValue(PASSWORD, value);
    }
    
    /**
     * Sets the user name.
     *
     * @param value the new user name
     */
    @Setter(name = USER_NAME)
    public void setUserName(String value)
    {
        setAttributeValue(USER_NAME, value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return getUserName();
    }
    
}
