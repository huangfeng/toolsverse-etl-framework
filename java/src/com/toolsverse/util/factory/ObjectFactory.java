/*
 * ObjectFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.factory;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.reflect.Modifier;

import com.toolsverse.cache.CacheManager;
import com.toolsverse.cache.SynchListMemoryCache;
import com.toolsverse.cache.SynchMemoryCache;
import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.license.LicenseManager;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.Utils;

/**
 * An ObjectFactory is responsible for creating objects of a specific type. 
 * The framework allows for object implementations to be loaded in dynamically via object factory. 
 * For example, when creating a parser, if Parser interface binded to ParticularParser implementation  
 * the Parser could be used to create a parser object. If needed objects can be created as "singletons". This is not a real singleton which itself
 * can be instantiated only once, instead the ObjectFactory will return a reference to the same object if called with the same parameters.   
 * The mapping between interfaces and implementations is usually stored in the property files but also can be dynamically changed via bind method of the registered
 * ObjectFactoryModule class.
 * 
 * <p>   
 * An ObjectFactory can create objects based on the current execution context. For example there is a binding: 
 * <p>  
 * com.toolsverse.util.lang.ParserTypeFactory.client=com.toolsverse.ui.swing.codeeditor.RSyntaxTextAreaParserTypeFactory
 * <p>
 * com.toolsverse.util.lang.ParserTypeFactory.server=com.toolsverse.ui.wings.codeeditor.CodeMirrorParserTypeFactory
 * <p>
 * if system property app.deployment=client ObjectFactory returns RSyntaxTextAreaParserTypeFactory, if it is app.deployment=server 
 * ObjectFactory returns CodeMirrorParserTypeFactory. 
 * 
 * @see com.toolsverse.util.factory.ObjectFactoryModule
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ObjectFactory
{
    
    /** OBJECT_FACTORY_OBJECTS. */
    public static final String OBJECT_FACTORY_OBJECTS = "objectfactoryobjects";
    
    /** The registered modules. */
    private SynchListMemoryCache<ObjectFactoryModule> _modules;
    
    /** The singletons. */
    private SynchMemoryCache<String, Reference<Object>> _singletons;
    
    /** The _instance. */
    private static ObjectFactory _instance = new ObjectFactory();
    
    /**
     * Instance.
     *
     * @return the ObjectFactory
     */
    public static ObjectFactory instance()
    {
        return _instance;
    }
    
    /**
     * Instantiates a new ObjectFactory.
     */
    private ObjectFactory()
    {
        _singletons = new SynchMemoryCache<String, Reference<Object>>();
        
        CacheManager.instance().add(OBJECT_FACTORY_OBJECTS, _singletons);
        
        _modules = new SynchListMemoryCache<ObjectFactoryModule>();
    }
    
    /**
     * Creates an object which either implements <code>name</code> interface or is an instance of the <code>name</code> class.
     * If object can not be created returns null. 
     *
     * @param name the name of the class
     * @return the object
     */
    public Object get(final String name)
    {
        return get(name, false);
    }
    
    /**
     * If <code>singleton == true</code> creates a "singleton" instance of the object which either implements <code>name</code> interface 
     * or is an instance of the <code>name</code> class. Otherwise just creates an object.
     * If object can not be created returns null.
     *
     * @param name the name of the class
     * @param singleton if true the singleton object will be created 
     * @return the object
     */
    public Object get(final String name, final boolean singleton)
    {
        return get(name, null, null, null, singleton, true);
    }
    
    /**
     * Creates an object which either implements <code>name</code> interface or is an instance of the <code>name</code> class.
     * If object can not be created or can not be cast to <code>classTo</code> returns null. 
     *
     * @param name the name of the class
     * @param classTo the class to cast to
     * @return the object
     */
    public Object get(final String name, final Class<?> classTo)
    {
        return get(name, null, null, classTo, false, true);
    }
    
    /**
     * If <code>singleton== true</code> creates a "singleton" instance of the object which either implements <code>name</code> interface
     * or is an instance of the <code>name</code> class. Otherwise just creates an object.
     * If object can not be created or can not be cast to <code>classTo</code> returns null.
     *
     * @param name the name of the class
     * @param classTo the class to cast to
     * @param singleton if true the singleton object will be created
     * @return the object
     */
    public Object get(final String name, final Class<?> classTo,
            final boolean singleton)
    {
        return get(name, null, null, classTo, singleton, true);
    }
    
    /**
     * First tries to create an object which either implements <code>name</code> interface or is an instance of the <code>name</code> class.
     * If it fails uses <code>realClassName</code>. If it also fails returns null. 
     *
     * @param name the name of the class
     * @param realClassName the alternative name of the class
     * @return the object
     */
    public Object get(final String name, final String realClassName)
    {
        return get(name, null, realClassName, null, false, true);
    }
    
    /**
     * If <code>singleton == true</code> first tries to create a "singleton" instance of the object which either implements <code>name</code> 
     * interface or is an instance of the <code>name</code> class.
     * If it fails uses <code>defaultName</code>. If it also fails returns null. 
     *
     * @param name the name of the class
     * @param defaultName the alternative name of the class
     * @param singleton if true the singleton object will be created
     * @return the object
     */
    public Object get(final String name, final String defaultName,
            final boolean singleton)
    {
        return get(name, defaultName, null, null, singleton, true);
    }
    
    /**
     * If <code>singleton == true</code> first tries to create a "singleton" instance of the object which either implements <code>name</code> 
     * interface or is an instance of the <code>name</code> class.
     * If it fails uses <code>defaultName</code>. If it also fails or object can not be cast to <code>classTo</code> returns null. 
     *
     * @param name the name of the class
     * @param defaultName the alternative name of the class
     * @param classTo the class to cast to
     * @param singleton if true the singleton object will be created
     * @return the object
     */
    public Object get(final String name, final String defaultName,
            final Class<?> classTo, final boolean singleton)
    {
        return get(name, defaultName, null, classTo, singleton, true);
    }
    
    /**
     * First tries to create an object which either implements <code>name</code> interface or is an instance of the <code>name</code> class.
     * If it fails uses <code>realClassName</code>. If it fails uses <code>defaultName</code> as a last resort. If it also fails returns null. 
     *
     * @param name the name of the class
     * @param defaultName the "last resort" name of class if both name and realClassName didn't work  
     * @param realClassName the alternative name of the class     
     * @return the object
     */
    public Object get(final String name, final String defaultName,
            final String realClassName)
    {
        return get(name, defaultName, realClassName, null, false, true);
    }
    
    /**
     * First tries to create an object which either implements <code>name</code> interface or is an instance of the <code>name</code> class.
     * If it fails uses <code>realClassName</code>. If it fails uses <code>defaultName</code> as a last resort. 
     * If it also fails or object can not be cast to <code>classTo</code> returns null. 
     *
     * @param name the name of the class
     * @param defaultName the "last resort" name of class if both name and realClassName didn't work  
     * @param realClassName the alternative name of the class
     * @param classTo the class to cast to     
     * @return the object
     */
    public Object get(final String name, final String defaultName,
            final String realClassName, final Class<?> classTo)
    {
        return get(name, defaultName, realClassName, classTo, false, true);
    }
    
    /**
     * First tries to create an object which either implements <code>name</code> interface or is an instance of the <code>name</code> class.
     * If it fails uses <code>realClassName</code>. If it fails uses <code>defaultName</code> as a last resort.
     * If it also fails or object can not be cast to <code>classTo</code> returns null.
     * If <code>singleton == true</code> the object will be created as "singleton".
     *
     * @param name the name of the class
     * @param defaultName the "last resort" name of class if both name and realClassName didn't work
     * @param realClassName the alternative name of the class
     * @param classTo the class to cast to
     * @param singleton if true the singleton object will be created
     * @param checkLicense the check license flag. If true object factory checks license
     * @return the object
     */
    public Object get(final String name, final String defaultName,
            final String realClassName, final Class<?> classTo,
            final boolean singleton, final boolean checkLicense)
    {
        if (Utils.isNothing(name))
            return null;
        
        Class<?> objClass = null;
        
        Object obj = null;
        
        try
        {
            if (singleton)
            {
                Reference<Object> ref = _singletons.get(name);
                
                if (ref != null)
                    obj = ref.get();
            }
            
            if (obj == null)
            {
                String className = getClassName(realClassName != null ? realClassName
                        : name);
                
                try
                {
                    objClass = Class.forName(className);
                }
                catch (ClassNotFoundException ex)
                {
                    if (!Utils.isNothing(defaultName))
                        return get(defaultName, null, realClassName, classTo,
                                singleton, checkLicense);
                    else
                        throw new ClassNotFoundException(className);
                }
                
                int modifiers = objClass.getModifiers();
                
                if (Modifier.isAbstract(modifiers)
                        || Modifier.isInterface(modifiers))
                {
                    if (!Utils.isNothing(defaultName))
                        return get(defaultName, null, realClassName, classTo,
                                singleton, true);
                    else
                        return null;
                }
                
                if (classTo != null && !classTo.isAssignableFrom(objClass))
                    throw new ClassCastException(objClass.getName() + " -> "
                            + classTo.getName());
                
                obj = objClass.newInstance();
                
                if (obj instanceof ExtensionModule)
                {
                    if (!Utils.isNothing(((ExtensionModule)obj)
                            .getLicensePropertyName()))
                    {
                        if (!LicenseManager.instance()
                                .hasLicense(
                                        ((ExtensionModule)obj)
                                                .getLicensePropertyName()))
                        {
                            if (!checkLicense)
                                return obj;
                            
                            if (!Utils.isNothing(defaultName))
                                return get(defaultName, null, realClassName,
                                        classTo, singleton, true);
                            else
                                return null;
                        }
                    }
                    
                }
                
                if (singleton)
                    _singletons.put(name, new SoftReference<Object>(obj));
            }
        }
        catch (Throwable ex)
        {
            throw new RuntimeException(
                    Resource.ERROR_INSTANTIATING_CLASS.getValue() + name, ex);
        }
        
        return obj;
    }
    
    /**
     * Gets the binded class name. If there is no binding returns classFrom.  
     *
     * @param classFrom the original class name
     * @return the class name
     */
    private String getClassName(String classFrom)
    {
        int size = _modules.size();
        
        for (int i = 0; i < size; i++)
        {
            ObjectFactoryModule module = _modules.get(i);
            
            String classTo = module.get(classFrom);
            
            if (!Utils.isNothing(classTo))
                return classTo;
        }
        return classFrom;
    }
    
    /**
     * Registers ObjectFactoryModule.
     *
     * @param module the ObjectFactoryModule
     */
    public void register(ObjectFactoryModule module)
    {
        _modules.put(0, module);
    }
    
    /**
     * Unregisters ObjectFactoryModule.
     *
     * @param module the ObjectFactoryModule
     */
    public void unRegister(ObjectFactoryModule module)
    {
        _modules.clearByValue(module);
    }
}
