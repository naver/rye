/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */

package rye.jdbc.driver;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import rye.jdbc.jci.BrokerHealthCheck;
import rye.jdbc.log.BasicLogger;
import rye.jdbc.log.Log;

public class ConnectionProperties
{
    public enum ZERO_DATE_BEHAVIOR {
	EXCEPTION, ROUND, CONVERT_TO_NULL
    };

    private static final String ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL = "convertToNull";
    private static final String ZERO_DATETIME_BEHAVIOR_EXCEPTION = "exception";
    private static final String ZERO_DATETIME_BEHAVIOR_ROUND = "round";

    private static final String[] booleanTrueValues = { "true", "yes", "on" };
    private static final String[] booleanFalseValues = { "false", "no", "off" };

    public static final int MAX_QUERY_TIMEOUT = 2000000;
    public static final int MAX_CONNECT_TIMEOUT = 2000000;

    private static final String DEFAULT_CHARSET = "utf-8";

    static ArrayList<Field> PROPERTY_LIST = new ArrayList<Field>();
    static {
	try {
	    Field[] declaredFields = ConnectionProperties.class.getDeclaredFields();

	    for (int i = 0; i < declaredFields.length; i++) {
		if (ConnectionProperties.ConnectionProperty.class.isAssignableFrom(declaredFields[i].getType())) {
		    PROPERTY_LIST.add(declaredFields[i]);
		}
	    }
	} catch (Exception e) {
	    RuntimeException rtEx = new RuntimeException();
	    rtEx.initCause(e);
	    throw rtEx;
	}
    }

    private final BooleanConnectionProperty logOnException = new BooleanConnectionProperty("logOnException", false);

    private final BooleanConnectionProperty logSlowQueries = new BooleanConnectionProperty("logSlowQueries", false);

    private final IntegerConnectionProperty slowQueryThresholdMillis = new IntegerConnectionProperty(
		    "slowQueryThresholdMillis", 60000, 0, Integer.MAX_VALUE);

    private final StringConnectionProperty logFile = new StringConnectionProperty("logFile", "rye_jdbc.log");

    private final CharSetConnectionProperty charSet = new CharSetConnectionProperty("charSet");

    private final IntegerConnectionProperty rcTime = new IntegerConnectionProperty("rcTime", 600, 0, Integer.MAX_VALUE);

    private final IntegerConnectionProperty queryTimeout = new IntegerConnectionProperty("queryTimeout", -1, -1,
		    MAX_QUERY_TIMEOUT);

    private final IntegerConnectionProperty connectTimeout = new IntegerConnectionProperty("connectTimeout",
		    getDefaultConnectTimeout(), -1, MAX_CONNECT_TIMEOUT);

    private final BooleanConnectionProperty connLoadBal = new BooleanConnectionProperty("loadBalance", false);

    private final ZeroDateTimeBehaviorConnectionProperty zeroDateTimeBehavior = new ZeroDateTimeBehaviorConnectionProperty(
		    "zeroDateTimeBehavior");

    private final BooleanConnectionProperty useLazyConnection = new BooleanConnectionProperty("useLazyConnection",
		    false);

    private final BooleanConnectionProperty logShardInfo = new BooleanConnectionProperty("logShardInfo", false);
    private final StringConnectionProperty logFileShardInfo = new StringConnectionProperty("logFileShardInfo",
		    "rye_jdbc_shard_info.log");

    private Log logger = null;

    ConnectionProperties(String propStr) throws SQLException
    {
	Properties properties = strToProperties(propStr);
	setProperties(properties);
    }

    public void dump(PrintStream out)
    {
	int numProperties = PROPERTY_LIST.size();
	for (int i = 0; i < numProperties; i++) {
	    Field propertyField = (Field) PROPERTY_LIST.get(i);

	    try {
		ConnectionProperty prop = (ConnectionProperty) propertyField.get(this);
		out.printf("%s%n", prop.toString());
	    } catch (Exception e) {
		out.printf("%s%n", e.getMessage());
	    }
	}
    }

    private Properties strToProperties(String propStr) throws SQLException
    {
	if (propStr == null) {
	    return null;
	}

	Properties p = new Properties();
	StringTokenizer st = new StringTokenizer(propStr, "?&;");
	while (st.hasMoreTokens()) {
	    String prop = st.nextToken();
	    StringTokenizer pt = new StringTokenizer(prop, "=");
	    if (pt.hasMoreTokens()) {
		String name = pt.nextToken().toLowerCase();
		if (pt.hasMoreTokens()) {
		    String value = pt.nextToken();
		    p.put(name, value);
		}
	    }
	}
	return p;
    }

    private void setProperties(Properties info) throws SQLException
    {
	if (info == null) {
	    return;
	}

	int numProperties = PROPERTY_LIST.size();
	for (int i = 0; i < numProperties; i++) {
	    Field propertyField = (Field) PROPERTY_LIST.get(i);

	    try {
		ConnectionProperty prop = (ConnectionProperty) propertyField.get(this);
		String propName = prop.getPropertyName().toLowerCase();
		String propValue = info.getProperty(propName);
		if (propValue == null) {
		    propName = prop.getPropertyName();
		    propValue = info.getProperty(propName);
		}

		if (propValue != null) {
		    prop.setValue(propValue);
		}
	    } catch (IllegalAccessException iae) {
		throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_URL,
				" illegal access properties", null);
	    }
	}
	if (this.getReconnectTime() < (BrokerHealthCheck.MONITORING_INTERVAL / 1000)) {
	    this.rcTime.setValue((Integer) (BrokerHealthCheck.MONITORING_INTERVAL / 1000));
	}
    }

    private int getDefaultConnectTimeout()
    {
	int timeout = java.sql.DriverManager.getLoginTimeout();
	return timeout > 0 ? timeout : 30;
    }

    public boolean getLogOnException()
    {
	return logOnException.booleanValue;
    }

    public boolean getLogSlowQueris()
    {
	return logSlowQueries.booleanValue;
    }

    public int getSlowQueryThresholdMillis()
    {
	return slowQueryThresholdMillis.intValue;
    }

    private String getLogFile()
    {
	return logFile.stringValue;
    }

    public String getCharset()
    {
	return charSet.charset;
    }

    public int getReconnectTime()
    {
	return rcTime.intValue;
    }

    public int getQueryTimeout()
    {
	return queryTimeout.intValue;
    }

    public int getConnectTimeout()
    {
	return connectTimeout.intValue;
    }

    public boolean getConnLoadBal()
    {
	return connLoadBal.booleanValue;
    }

    public ZERO_DATE_BEHAVIOR getZeroDateTimeBehavior()
    {
	return zeroDateTimeBehavior.zeroDateBehavior;
    }

    public boolean getUseLazyConnection()
    {
	return useLazyConnection.booleanValue;
    }

    public boolean getLogShardInfo()
    {
	return logShardInfo.booleanValue;
    }

    public String getLogFileShardInfo()
    {
	return logFileShardInfo.stringValue;
    }

    synchronized public Log getLogger()
    {
	if (logger == null) {
	    logger = new BasicLogger(getLogFile());
	}
	return logger;
    }

    private abstract class ConnectionProperty
    {
	String propertyName;

	ConnectionProperty(String propertyName)
	{
	    this.propertyName = propertyName;
	}

	abstract void setValue(Object o) throws SQLException;

	void errorUncompatibleValue(Object value) throws SQLException
	{
	    String msg = String.format(" '%s' uncompitable value for the %s", value, propertyName);
	    throw RyeException.createRyeException((RyeConnectionUrl) null, RyeErrorCode.ER_INVALID_URL, msg, null);
	}

	String getPropertyName()
	{
	    return propertyName;
	}
    }

    private class BooleanConnectionProperty extends ConnectionProperty
    {
	boolean booleanValue;

	BooleanConnectionProperty(String propertyName, boolean defaultValue)
	{
	    super(propertyName);
	    this.booleanValue = defaultValue;
	}

	void setValue(Object o) throws SQLException
	{
	    if (o == null) {
	    }
	    else if (o instanceof Boolean) {
		booleanValue = ((Boolean) o).booleanValue();
		return;
	    }
	    else if (o instanceof String) {
		for (int i = 0; i < booleanTrueValues.length; i++) {
		    if (booleanTrueValues[i].equalsIgnoreCase((String) o)) {
			booleanValue = true;
			return;
		    }
		}
		for (int i = 0; i < booleanFalseValues.length; i++) {
		    if (booleanFalseValues[i].equalsIgnoreCase((String) o)) {
			booleanValue = false;
			return;
		    }
		}
	    }

	    errorUncompatibleValue(o);
	}

	public String toString()
	{
	    return String.format("%s:%s=%s", this.getClass().getName(), this.getPropertyName(), booleanValue);
	}
    }

    private class IntegerConnectionProperty extends ConnectionProperty
    {
	private int lowerBound;
	private int upperBound;

	int intValue;

	IntegerConnectionProperty(String propertyName, int defaultValue, int lowerBound, int upperBound)
	{
	    super(propertyName);
	    this.intValue = defaultValue;
	    this.lowerBound = lowerBound;
	    this.upperBound = upperBound;
	}

	void setIntValue(Integer value) throws SQLException
	{
	    if (value.intValue() < this.lowerBound || value.intValue() > this.upperBound) {
		errorUncompatibleValue(value);
	    }
	    else {
		this.intValue = value;
	    }

	}

	void setValue(Object o) throws SQLException
	{
	    if (o == null) {
	    }
	    else if (o instanceof Integer) {
		setIntValue((Integer) o);
		return;
	    }
	    else if (o instanceof String) {
		try {
		    setIntValue(Integer.valueOf((String) o));
		    return;
		} catch (NumberFormatException e) {
		}
	    }

	    errorUncompatibleValue(o);
	}

	public String toString()
	{
	    return String.format("%s:%s=%d", this.getClass().getName(), this.getPropertyName(), intValue);
	}
    }

    private class StringConnectionProperty extends ConnectionProperty
    {
	String stringValue;

	StringConnectionProperty(String propertyName, String defaultValue)
	{
	    super(propertyName);
	    this.stringValue = defaultValue;
	}

	void setValue(Object o) throws SQLException
	{
	    if (o == null) {
	    }
	    else if (o instanceof String) {
		this.stringValue = ((String) o);
		return;
	    }

	    errorUncompatibleValue(o);
	}

	public String toString()
	{
	    return String.format("%s:%s=%s", this.getClass().getName(), this.getPropertyName(), stringValue);
	}
    }

    private class CharSetConnectionProperty extends ConnectionProperty
    {
	String charset = DEFAULT_CHARSET;

	CharSetConnectionProperty(String propertyName)
	{
	    super(propertyName);
	}

	void setValue(Object o) throws SQLException
	{
	    if (o == null) {
	    }
	    else if (o instanceof String) {
		try {
		    byte[] s = { 0 };
		    new String(s, (String) o);

		    this.charset = ((String) o);
		    return;
		} catch (UnsupportedEncodingException e) {
		}
	    }

	    errorUncompatibleValue(o);
	}

	public String toString()
	{
	    return String.format("%s:%s=%s", this.getClass().getName(), this.getPropertyName(), charset);
	}
    }

    private class ZeroDateTimeBehaviorConnectionProperty extends ConnectionProperty
    {
	ZERO_DATE_BEHAVIOR zeroDateBehavior = ZERO_DATE_BEHAVIOR.EXCEPTION;;

	ZeroDateTimeBehaviorConnectionProperty(String propertyName)
	{
	    super(propertyName);
	}

	void setValue(Object o) throws SQLException
	{
	    if (o == null) {
	    }
	    else if (o instanceof String) {
		String behavior = (String) o;
		if (behavior.equals(ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL)) {
		    this.zeroDateBehavior = ZERO_DATE_BEHAVIOR.CONVERT_TO_NULL;
		    return;
		}
		if (behavior.equals(ZERO_DATETIME_BEHAVIOR_EXCEPTION)) {
		    this.zeroDateBehavior = ZERO_DATE_BEHAVIOR.EXCEPTION;
		    return;
		}
		if (behavior.equals(ZERO_DATETIME_BEHAVIOR_ROUND)) {
		    this.zeroDateBehavior = ZERO_DATE_BEHAVIOR.ROUND;
		    return;
		}
	    }

	    errorUncompatibleValue(o);
	}

	public String toString()
	{
	    String behavior = null;
	    switch (zeroDateBehavior)
	    {
	    case EXCEPTION:
		behavior = ZERO_DATETIME_BEHAVIOR_EXCEPTION;
		break;
	    case ROUND:
		behavior = ZERO_DATETIME_BEHAVIOR_ROUND;
		break;
	    case CONVERT_TO_NULL:
		behavior = ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL;
		break;
	    }

	    return String.format("%s:%s=%s", this.getClass().getName(), this.getPropertyName(), behavior);
	}
    }
}
