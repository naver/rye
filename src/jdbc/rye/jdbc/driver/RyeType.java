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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import rye.jdbc.jci.JciConnection;

abstract public class RyeType
{
    public static final byte TYPE_NULL = 0;
    public static final byte TYPE_VARCHAR = 1;
    public static final byte TYPE_BINARY = 2;
    public static final byte TYPE_NUMERIC = 3;
    public static final byte TYPE_INT = 4;
    public static final byte TYPE_DOUBLE = 5;
    public static final byte TYPE_DATE = 6;
    public static final byte TYPE_TIME = 7;
    public static final byte TYPE_BIGINT = 8;
    public static final byte TYPE_DATETIME = 9;

    public static final int VARCHAR_MAX_PRECISION = 1073741823;
    public static final int NUMERIC_MAX_PRECISION = 38;
    public static final int BINARY_MAX_PRECISION = 1073741823;

    public static byte getObjectDBtype(Object value)
    {
	if (value == null)
	    return TYPE_NULL;
	else if (value instanceof String)
	    return TYPE_VARCHAR;
	else if (value instanceof Byte)
	    return TYPE_INT;
	else if (value instanceof byte[])
	    return TYPE_BINARY;
	else if (value instanceof Boolean || value instanceof Short || value instanceof Integer)
	    return TYPE_INT;
	else if (value instanceof Long)
	    return TYPE_BIGINT;
	else if (value instanceof Double || value instanceof Float)
	    return TYPE_DOUBLE;
	else if (value instanceof BigDecimal)
	    return TYPE_NUMERIC;
	else if (value instanceof Date)
	    return TYPE_DATE;
	else if (value instanceof Time)
	    return TYPE_TIME;
	else if (value instanceof Timestamp)
	    return TYPE_DATETIME;
	else
	    return TYPE_NULL;
    }

    public static String getJavaClassName(byte ryeType)
    {
	switch (ryeType)
	{
	case RyeType.TYPE_NULL:
	    return "null";
	case RyeType.TYPE_BINARY:
	    return "byte[]";
	case RyeType.TYPE_VARCHAR:
	    return "java.lang.String";
	case RyeType.TYPE_NUMERIC:
	    return "java.math.BigDecimal";
	case RyeType.TYPE_INT:
	    return "java.lang.Integer";
	case RyeType.TYPE_BIGINT:
	    return "java.lang.Long";
	case RyeType.TYPE_DOUBLE:
	    return "java.lang.Double";
	case RyeType.TYPE_DATE:
	    return "java.sql.Date";
	case RyeType.TYPE_TIME:
	    return "java.sql.Time";
	case RyeType.TYPE_DATETIME:
	    return "java.sql.Timestamp";
	default:
	    return "";
	}
    }

    public static BigDecimal getBigDecimal(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof BigDecimal) {
	    return (BigDecimal) data;
	}
	else if (data instanceof String) {
	    try {
		return new BigDecimal((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Long) {
	    return new BigDecimal(((Long) data).longValue());
	}
	else if (data instanceof Number) {
	    return new BigDecimal(((Number) data).doubleValue());
	}
	else if (data instanceof Boolean) {
	    return new BigDecimal((((Boolean) data).booleanValue() == true) ? (double) 1 : (double) 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static boolean getBoolean(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return false;
	}
	else if (data instanceof Boolean) {
	    return ((Boolean) data).booleanValue();
	}
	else if (data instanceof String) {
	    return ((((String) data).compareTo("0") == 0) ? false : true);
	}
	else if (data instanceof Number) {
	    return ((((Number) data).doubleValue() == (double) 0) ? false : true);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static byte getByte(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return (byte) 0;
	}
	else if (data instanceof Number) {
	    return ((Number) data).byteValue();
	}
	else if (data instanceof String) {
	    try {
		return Byte.parseByte((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Boolean) {
	    return ((((Boolean) data).booleanValue() == true) ? (byte) 1 : (byte) 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static byte[] getBytes(Object data, JciConnection jciCon) throws SQLException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof byte[]) {
	    return ((byte[]) data).clone();
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static Date getDate(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof Date) {
	    return new Date(((Date) data).getTime());
	}
	else if (data instanceof Timestamp) {
	    return new Date(((Timestamp) data).getTime());
	}
	else if (data instanceof String) {
	    try {
		return Date.valueOf((String) data);
	    } catch (IllegalArgumentException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static double getDouble(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return (double) 0;
	}
	else if (data instanceof Number) {
	    return ((Number) data).doubleValue();
	}
	else if (data instanceof String) {
	    try {
		return Double.parseDouble((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Boolean) {
	    return ((((Boolean) data).booleanValue() == true) ? (double) 1 : (double) 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static float getFloat(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return (float) 0;
	}
	else if (data instanceof Number) {
	    return ((Number) data).floatValue();
	}
	else if (data instanceof String) {
	    try {
		return Float.parseFloat((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Boolean) {
	    return ((((Boolean) data).booleanValue() == true) ? (float) 1 : (float) 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static int getInt(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return 0;
	}
	else if (data instanceof Number) {
	    return ((Number) data).intValue();
	}
	else if (data instanceof String) {
	    try {
		return Integer.parseInt((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Boolean) {
	    return ((((Boolean) data).booleanValue() == true) ? 1 : 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static long getLong(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return (long) 0;
	}
	else if (data instanceof String) {
	    try {
		return Long.parseLong((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Number) {
	    return ((Number) data).longValue();
	}
	else if (data instanceof Boolean) {
	    return ((((Boolean) data).booleanValue() == true) ? (long) 1 : (long) 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static short getShort(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return (short) 0;
	}
	else if (data instanceof Number) {
	    return ((Number) data).shortValue();
	}
	else if (data instanceof String) {
	    try {
		return Short.parseShort((String) data);
	    } catch (NumberFormatException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Boolean) {
	    return ((((Boolean) data).booleanValue() == true) ? (short) 1 : (short) 0);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static String getString(Object data, JciConnection jciCon) throws SQLException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof String) {
	    return ((String) data);
	}
	else if (data instanceof BigDecimal) {
	    return ((BigDecimal) data).toPlainString();
	}
	else if ((data instanceof Number) || (data instanceof Boolean) || (data instanceof Date)
			|| (data instanceof Time)) {
	    return data.toString();
	}
	else if (data instanceof Timestamp) {
	    String form = "yyyy-MM-dd HH:mm:ss.SSS";
	    java.text.SimpleDateFormat f = new java.text.SimpleDateFormat(form);
	    return f.format(data);
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static Time getTime(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof Time) {
	    return new Time(((Time) data).getTime());
	}
	else if (data instanceof String) {
	    try {
		return Time.valueOf((String) data);
	    } catch (IllegalArgumentException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Timestamp) {
	    return new Time(((Timestamp) data).getTime());
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static Timestamp getTimestamp(Object data, JciConnection jciCon) throws RyeException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof Timestamp) {
	    return new Timestamp(((Timestamp) data).getTime());
	}
	else if (data instanceof String) {
	    try {
		return Timestamp.valueOf((String) data);
	    } catch (IllegalArgumentException e) {
		throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, e);
	    }
	}
	else if (data instanceof Date) {
	    return new Timestamp(((Date) data).getTime());
	}
	else if (data instanceof Time) {
	    return new Timestamp(((Time) data).getTime());
	}

	throw RyeException.createRyeException(jciCon, RyeErrorCode.ER_CONVERSION_ERROR, null);
    }

    public static Object getObject(Object data) throws RyeException
    {
	if (data == null) {
	    return null;
	}
	else if (data instanceof byte[]) {
	    return ((byte[]) data).clone();
	}
	else if (data instanceof Date) {
	    return ((Date) data).clone();
	}
	else if (data instanceof Time) {
	    return ((Time) data).clone();
	}
	else if (data instanceof Timestamp) {
	    return new Timestamp(((Timestamp) data).getTime());
	}
	else {
	    return data;
	}
    }
}
