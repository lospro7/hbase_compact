/**
 * This file is part of hbaseadmin.
 * Copyright (C) 2011 StumbleUpon, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. This program is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy
 * of the GNU Lesser General Public License along with this program. If not,
 * see <http: *www.gnu.org/licenses/>.
 */

/*
 *
 * This file is mostly stolen straight out of http://jmxcmd.svn.sourceforge.net,
 * which in turn has been adapted from http://crawler.archive.org/cmdline-jmxclient/
 *
 */

package com.stumbleupon.hbaseadmin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes a jmx query
 */
public class JMXQuery {
    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(JMXQuery.class.getName());
    /** argument pattern */
    protected static final Pattern CMD_LINE_ARGS_PATTERN = Pattern.compile("^([^=]+)(?:(?:\\=)(.+))?$");

    /** param */
    private String beanName = "";
    /** param */
    private String command = "";

    /**
     * Construct a new JMXQuery
     * @param mbean the mbean
     * @param jmxCommand The jmx command
     */
    public JMXQuery(String mbean, String jmxCommand) {
        this.beanName = mbean;
        this.command = jmxCommand;
    }

    /**
     * Entry point
     * @param args command line arguments
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        JMXQuery client = new JMXQuery("hadoop:name=RegionServerStatistics,service=RegionServer",
            "compactionQueueSize");
        JmxServerSpec spec = new JmxServerSpec(args[0], args[1], null);
        LOG.info("Output: " + client.execute(spec));
    }

    /**
     * Get jmx credentials
     * @param filename The path to the password file
     * @return The map of credentials
     * @throws FileNotFoundException 
     * @throws IOException 
     */
    protected Map<String, String[]> getCredentials(String filename) throws FileNotFoundException, IOException {
        Map<String, String[]> env = null;
        final StringBuilder contents = new StringBuilder();
        final BufferedReader input = new BufferedReader(new FileReader(new File(filename)));

        try {
            String line = input.readLine(); // not declared within while loop
            if (line != null) {
                contents.append(line);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            input.close();
        }

        final String userpass = contents.toString();
        // split on ascii space (32)
        final int index = userpass.indexOf(32);
        
        if (index <= 0) {
            throw new RuntimeException("Unable to parse: " + userpass);
        }

        final String[] creds = { userpass.substring(0, index), userpass.substring(index + 1) };
        env = new HashMap<String, String[]>(1);
        env.put("jmx.remote.credentials", creds);
        
        return env;
    }

    /**
     * Execute query
     * @param spec the jmx server specs
     * @return The result of the query
     * @throws Exception 
     */
    public String execute(JmxServerSpec spec) throws Exception {
        Iterator i;
        String result = "";
        final JMXServiceURL rmiurl = new JMXServiceURL(
            "service:jmx:rmi://" + spec.getHostport() + "/jndi/rmi://" + spec.getHostport() + "/jmxrmi");

        final JMXConnector jmxc;
        if (spec.getPasswordFile() != null) {
            jmxc = JMXConnectorFactory.connect(rmiurl, getCredentials(spec.getPasswordFile()));
        } else {
            jmxc = JMXConnectorFactory.connect(rmiurl);
        }

        try {
            final MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            if (beanName == null) {
                beanName = "";
            }

            final ObjectName objName = new ObjectName(beanName);
            final Set beans = mbsc.queryMBeans(objName, null);

            if (beans.size() == 0) {
                LOG.warn(objName.getCanonicalName() + " is not a registered bean");
            } else if (beans.size() == 1) {
                final ObjectInstance instance = (ObjectInstance)beans.iterator().next();
                result = doBean(mbsc, instance, command);
            } else {

                for (i = beans.iterator(); i.hasNext();) {
                    final Object obj = i.next();

                    if (obj instanceof ObjectName) {
                        System.out.println(((ObjectName)obj).getCanonicalName());
                    } else if (obj instanceof ObjectInstance) {
                        System.out.println(((ObjectInstance)obj).getObjectName().getCanonicalName());
                    } else {
                        LOG.error("Unexpected object type: " + obj);
                    }
                }
            }
        } finally {
            jmxc.close();
        }
        
        return result;
    }

    /**
     * doBean
     * @param mbsc 
     * @param instance 
     * @param command 
     * @return string
     * @throws Exception 
     */
    public String doBean(MBeanServerConnection mbsc, ObjectInstance instance, String command) throws Exception {
        String ret = "";
        
        if (command == null) {
            listOptions(mbsc, instance);
            ret = "";
        } else {
            ret = doSubCommand(mbsc, instance, command);
        }

        return ret;
    }

    /**
     * doSubCommand
     * @param mbsc 
     * @param instance 
     * @param subCommand 
     * @return string
     * @throws Exception 
     */
    protected String doSubCommand(MBeanServerConnection mbsc, ObjectInstance instance, String subCommand)
        throws Exception {
        final MBeanAttributeInfo[] attributeInfo = mbsc.getMBeanInfo(instance.getObjectName()).getAttributes();
        final MBeanOperationInfo[] operationInfo = mbsc.getMBeanInfo(instance.getObjectName()).getOperations();
        Object result = null;

        if (Character.isUpperCase(subCommand.charAt(0))) {
            if ((!(isFeatureInfo(attributeInfo, subCommand))) && (isFeatureInfo(operationInfo, subCommand))) {
                result = doBeanOperation(mbsc, instance, subCommand, operationInfo);
            } else {
                result = doAttributeOperation(mbsc, instance, subCommand, attributeInfo);
            }
        } else if ((!(isFeatureInfo(operationInfo, subCommand))) && (isFeatureInfo(attributeInfo, subCommand))) {
            result = doAttributeOperation(mbsc, instance, subCommand, attributeInfo);
        } else {
            result = doBeanOperation(mbsc, instance, subCommand, operationInfo);
        }

        if (result instanceof CompositeData) {
            result = recurseCompositeData(new StringBuffer("\n"), "", "", (CompositeData)result);
        } else if (result instanceof TabularData) {
            result = recurseTabularData(new StringBuffer("\n"), "", "", (TabularData)result);
        } else if (result instanceof String[]) {
            String[] strs = (String[])result;
            StringBuffer buffer = new StringBuffer("\n");

            for (int i = 0; i < strs.length; ++i) {
                buffer.append(strs[i]);
                buffer.append("\n");
            }

            result = buffer;
        }

        return result.toString();
    }

    /**
     * isFeatureInfo
     * @param infos feature info
     * @param cmd The command
     * @return boolean
     */
    protected boolean isFeatureInfo(MBeanFeatureInfo[] infos, String cmd) {
        return (getFeatureInfo(infos, cmd) != null);
    }

    /**
     * getFeatureInfo
     * @param infos feature info
     * @param cmd the command
     * @return MBeanFeatureInfo
     */
    protected MBeanFeatureInfo getFeatureInfo(MBeanFeatureInfo[] infos, String cmd) {
        final int index = cmd.indexOf(61);
        final String name = (index > 0) ? cmd.substring(0, index) : cmd;
        MBeanFeatureInfo ret = null;

        for (int i = 0; i < infos.length; ++i) {
            if (infos[i].getName().equals(name)) {
                ret = infos[i];
                break;
            }
        }
        
        return ret;
    }

    /**
     * recurseTabularData
     * @param buffer A string buffer
     * @param indent The indent string
     * @param name The name
     * @param data The tabular data object
     * @return StringBuffer
     */
    protected StringBuffer recurseTabularData(StringBuffer buffer, String indent, String name, TabularData data) {
        addNameToBuffer(buffer, indent, name);
        final Collection c = data.values();
        
        for (Iterator i = c.iterator(); i.hasNext();) {
            final Object obj = i.next();
            
            if (obj instanceof CompositeData) {
                recurseCompositeData(buffer, indent + " ", "", (CompositeData)obj);
            } else if (obj instanceof TabularData) {
                recurseTabularData(buffer, indent, "", (TabularData)obj);
            } else {
                buffer.append(obj);
            }
        }
        
        return buffer;
    }

    /**
     * recurseCompositeData
     * @param buffer The buffer
     * @param indent the indent
     * @param name the name
     * @param data the composite data
     * @return StringBuffer
     */
    protected StringBuffer recurseCompositeData(StringBuffer buffer, String indent, String name, CompositeData data) {
        String newIndent = addNameToBuffer(buffer, indent, name);
        Iterator i = data.getCompositeType().keySet().iterator();
        
        while (i.hasNext()) {
            String key = (String)i.next();
            Object o = data.get(key);
            
            if (o instanceof CompositeData) {
                recurseCompositeData(buffer, newIndent + " ", key, (CompositeData)o);
            } else if (o instanceof TabularData) {
                recurseTabularData(buffer, newIndent, key, (TabularData)o);
            } else {
                buffer.append(newIndent);
                buffer.append(key);
                buffer.append(": ");
                buffer.append(o);
                buffer.append("\n");
            }
        }
        
        return buffer;
    }

    /**
     * addNameToBuffer
     * @param buffer The buffer
     * @param indent the indent
     * @param name the name
     * @return String
     */
    protected String addNameToBuffer(StringBuffer buffer, String indent, String name) {
        String ret = null;
        
        if ((name == null) || (name.length() == 0)) {
            ret = indent;
        } else {
            buffer.append(indent);
            buffer.append(name);
            buffer.append(":\n");
            ret = indent + " ";
        }

        return ret;
    }

    /**
     * doAttributeOperation
     * @param mbsc MBeanServerConnection
     * @param instance ObjectInstance
     * @param command the jmx command
     * @param infos MBeanAttributeInfo 
     * @return Object
     * @throws Exception 
     */
    protected Object doAttributeOperation(MBeanServerConnection mbsc, ObjectInstance instance, String command,
        MBeanAttributeInfo[] infos) throws Exception {
        final CommandParse parse = new CommandParse(command);
        Object ret = null;

        if ((parse.getArgs() == null) || (parse.getArgs().length == 0)) {
            ret = mbsc.getAttribute(instance.getObjectName(), parse.getCmd());
        } else {
            if (parse.getArgs().length != 1) {
                throw new IllegalArgumentException("One only argument setting attribute values: " + parse.getArgs());
            }
    
            final MBeanAttributeInfo info = (MBeanAttributeInfo)getFeatureInfo(infos, parse.getCmd());
    
            @SuppressWarnings("unchecked")
            final Constructor c = getResolvedClass(info.getType()).getConstructor(new Class[] { String.class });
            final Attribute a = new Attribute(parse.getCmd(), c.newInstance(new Object[] { parse.getArgs()[0] }));
            mbsc.setAttribute(instance.getObjectName(), a);
        }
        
        return ret;
    }

    /**
     * doBeanOperation
     * @param mbsc MBeanServerConnection
     * @param instance ObjectInstance
     * @param command The jmx command
     * @param infos MBeanOperationInfo
     * @return Object
     * @throws Exception 
     */
    protected Object doBeanOperation(MBeanServerConnection mbsc, ObjectInstance instance, String command,
        MBeanOperationInfo[] infos) throws Exception {
        final CommandParse parse = new CommandParse(command);
        final MBeanOperationInfo op = (MBeanOperationInfo)getFeatureInfo(infos, parse.getCmd());
        Object result = null;

        if (op == null) {
            result = "Operation " + parse.getCmd() + " not found.";
        } else {
            final MBeanParameterInfo[] paraminfos = op.getSignature();
            final int paraminfosLength = (paraminfos == null) ? 0 : paraminfos.length;
            int objsLength = (parse.getArgs() == null) ? 0 : parse.getArgs().length;

            // FIXME: bad solution for comma containing parameter
            if (paraminfosLength == 1) {
                // concat all to one string
                String realParameter = parse.getArgs()[0];
                for (int j = 1; j < objsLength; j++) {
                    realParameter = realParameter + "," + parse.getArgs()[j];
                }
                objsLength = 1;
                parse.setArgs(new String[] { realParameter });
            }

            if (paraminfosLength != objsLength) {
                result = "Passed param count does not match signature count";
            } else {
                final String[] signature = new String[paraminfosLength];
                final Object[] params = (paraminfosLength == 0) ? null : new Object[paraminfosLength];

                for (int i = 0; i < paraminfosLength; ++i) {
                    final MBeanParameterInfo paraminfo = paraminfos[i];
                    // System.out.println( "paraminfo.getType() = " + paraminfo.getType());
                    final String classType = paraminfo.getType();
                    // Constructor c = Class.forName(classType).getConstructor(new Class[] { String.class });
                    @SuppressWarnings("unchecked")
                    final Constructor c = getResolvedClass(paraminfo.getType()).getConstructor(
                        new Class[] { String.class });

                    params[i] = c.newInstance(new Object[] { parse.getArgs()[i] });
                    signature[i] = classType;
                }

                result = mbsc.invoke(instance.getObjectName(), parse.getCmd(), params, signature);
            }
        }

        return result;
    }

    /**
     * getResolvedClass
     * @param className The class name
     * @return Class
     * @throws ClassNotFoundException 
     */
    private static Class getResolvedClass(String className) throws ClassNotFoundException {
        // Check if the className is the name of a primitive. If this is the case, use their
        // corresponding class.
        Class ret = null;
        
        if ("boolean".equals(className)) {
            ret = Boolean.class;
        } else if ("byte".equals(className)) {
            ret = Byte.class;
        } else if ("char".equals(className)) {
            ret = Character.class;
        } else if ("double".equals(className)) {
            ret = Double.class;
        } else if ("float".equals(className)) {
            ret = Float.class;
        } else if ("int".equals(className)) {
            ret = Integer.class;
        } else if ("long".equals(className)) {
            ret = Long.class;
        } else if ("short".equals(className)) {
            ret = Short.class;
        } else {
            ret = Class.forName(className);
        }
        
        return ret;
    }

    /**
     * listOptions
     * @param mbsc MBeanServerConnection
     * @param instance ObjectInstance
     * @throws InstanceNotFoundException 
     * @throws IntrospectionException 
     * @throws ReflectionException 
     * @throws IOException 
     */
    protected void listOptions(MBeanServerConnection mbsc, ObjectInstance instance) throws InstanceNotFoundException,
        IntrospectionException, ReflectionException, IOException {
        final MBeanInfo info = mbsc.getMBeanInfo(instance.getObjectName());
        final MBeanAttributeInfo[] attributes = info.getAttributes();

        if (attributes.length > 0) {
            System.out.println("Attributes:");

            for (int i = 0; i < attributes.length; ++i) {
                System.out.println(' ' + attributes[i].getName() + ": " + attributes[i].getDescription() + " (type="
                    + attributes[i].getType() + ")");
            }
        }

        MBeanOperationInfo[] operations = info.getOperations();

        if (operations.length > 0) {
            System.out.println("Operations:");

            for (int i = 0; i < operations.length; ++i) {
                final MBeanParameterInfo[] params = operations[i].getSignature();
                final StringBuffer paramsStrBuffer = new StringBuffer();

                if (params != null) {
                    for (int j = 0; j < params.length; ++j) {
                        paramsStrBuffer.append("\n   name=");
                        paramsStrBuffer.append(params[j].getName());
                        paramsStrBuffer.append(" type=");
                        paramsStrBuffer.append(params[j].getType());
                        paramsStrBuffer.append(" ");
                        paramsStrBuffer.append(params[j].getDescription());
                    }
                }
                System.out.println(' ' + operations[i].getName() + ": " + operations[i].getDescription()
                    + "\n  Parameters " + params.length + ", return type=" + operations[i].getReturnType()
                    + paramsStrBuffer.toString());
            }
        }
    }

    /**
     * CommandParse. Class to parse commands?
     */
    protected class CommandParse {
        /** the command */
        private String cmd;
        /** the command arguments */
        private String[] args;

        /**
         * Construct a new CommandParse
         * @param paramString 
         * @throws ParseException 
         */
        protected CommandParse(String paramString) throws ParseException {
            parse(paramString);
        }

        /**
         * Parse the command
         * @param command the command
         * @throws ParseException 
         */
        private void parse(String command) throws ParseException {
            final Matcher m = JMXQuery.CMD_LINE_ARGS_PATTERN.matcher(command);
            if ((m == null) || (!(m.matches()))) {
                throw new ParseException("Failed parse of " + command, 0);
            }

            this.cmd = m.group(1);
            if ((m.group(2) != null) && (m.group(2).length() > 0)) {
                this.args = m.group(2).split(",");
            } else {
                this.args = null;
            }
        }

        /**
         * Get the command
         * @return the command
         */
        protected String getCmd() {
            return this.cmd;
        }

        /**
         * Get the arguments
         * @return the arguments
         */
        protected String[] getArgs() {
            return this.args;
        }

        /**
         * Set the arguments
         * @param args the args
         */
        public void setArgs(String[] args) {
            this.args = args;
        }
    }

}
