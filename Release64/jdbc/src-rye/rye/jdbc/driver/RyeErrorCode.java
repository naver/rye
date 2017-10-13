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

import java.util.HashMap;

/*
 * Error codes and messages
 */

abstract public class RyeErrorCode
{
    public static final int ER_NO_ERROR = 0;

    /* driver error code */
    public static final int ER_UNKNOWN = -21100;
    public static final int ER_CONNECTION_CLOSED = -21101;
    public static final int ER_STATEMENT_CLOSED = -21102;
    public static final int ER_PREPARED_STATEMENT_CLOSED = -21103;
    public static final int ER_RESULTSTE_CLOSED = -21104;
    public static final int ER_NO_SUPPORTED = -21105;
    public static final int ER_INVALID_TRAN_ISOLATION_LEVEL = -21106;
    public static final int ER_INVALID_URL = -21107;
    public static final int ER_NO_DBNAME = -21108;
    public static final int ER_INVALID_QUERY_TYPE_FOR_EXECUTEQUERY = -21109;
    public static final int ER_INVALID_QUERY_TYPE_FOR_EXECUTEUPDATE = -21110;
    public static final int ER_NEGATIVE_VALUE_FOR_LENGTH = -21111;
    public static final int ER_IOEXCEPTION_IN_STREAM = -21112;
    public static final int ER_DEPRECATED = -21113;
    public static final int ER_NOT_NUMERIC_OBJECT = -21114;
    public static final int ER_INVALID_INDEX = -21115;
    public static final int ER_INVALID_COLUMN_NAME = -21116;
    public static final int ER_INVALID_ROW = -21117;
    public static final int ER_CONVERSION_ERROR = -21118;
    public static final int ER_INVALID_TUPLE = -21119;
    public static final int ER_INVALID_VALUE = -21120;
    public static final int ER_DBMETADATA_CLOSED = -21122;
    public static final int ER_NON_SCROLLABLE = -21123;
    public static final int ER_NON_SENSITIVE = -21124;
    public static final int ER_NON_UPDATABLE = -21125;
    public static final int ER_NON_UPDATABLE_COLUMN = -21126;
    public static final int ER_EMPTY_INPUT_STREAM = -21129;
    public static final int ER_EMPTY_READER = -21130;
    public static final int ER_NON_SCROLLABLE_STATEMENT = -21132;
    public static final int ER_POOLED_CONNECTION_CLOSED = -21134;
    public static final int ER_INVALID_OBJECT_BIND = -21135;
    public static final int ER_NOT_ALL_BINDED = -21136;
    public static final int ER_INTERNAL_ERROR = -21137;
    public static final int ER_INVALID_QUERY_TYPE_FOR_EXECUTEBATCH = -21138;

    public static final int ER_REQUEST_TIMEOUT = -21141;

    public static final int ER_SHARD_GROUPID_INFO_OBSOLETE = -21184;
    /*
     * ER_SHARD_DIFFERENT_SHARD_KEY_INFO is internal error code. ShardGroupStatement's shard info is different with
     * shard node's shard info. if this error happens, shardGroupStatement will retry execution
     */
    public static final int ER_SHARD_DIFFERENT_SHARD_KEY_INFO = -21185;

    public static final int ER_SHARD_DML_SHARD_KEY = -21186;
    public static final int ER_SHARD_COMMIT_FAIL = -21187;
    public static final int ER_SHARD_DML_FAIL = -21188;
    public static final int ER_SHARD_DDL_FAIL = -21189;
    public static final int ER_SHARD_INTERNAL_ERROR = -21190;
    public static final int ER_SHARD_INVALID_SHARDKEY_TYPE = -21191;
    public static final int ER_SHARD_INCOMPATIBLE_CON_REQUEST = -21193;
    public static final int ER_SHARD_MORE_THAN_ONE_SHARD_TRAN = -21194;
    public static final int ER_SHARD_GROUPID_INVALID = -21195;
    public static final int ER_SHARD_NODE_MAX_SERVICE_PORT_EXCEED = -21196;
    public static final int ER_SHARD_NODE_CONNECTION_INVALID = -21197;
    public static final int ER_SHARD_NODE_INFO_OBSOLETE = -21198;
    public static final int ER_SHARD_INFO_INVALID = -21199;

    public static final int ER_DBMS = -21002;
    public static final int ER_COMMUNICATION = -21003;
    public static final int ER_NO_MORE_DATA = -21004;
    public static final int ER_TYPE_CONVERSION = -21005;
    public static final int ER_NOT_BIND = -21007;
    public static final int ER_COLUMN_INDEX = -21009;
    public static final int ER_TRUNCATE = -21010;
    public static final int ER_SCHEMA_TYPE = -21011;
    public static final int ER_FILE = -21012;
    public static final int ER_CONNECTION = -21013;
    public static final int ER_ILLEGAL_REQUEST = -21015;
    public static final int ER_INVALID_ARGUMENT = -21016;
    public static final int ER_IS_CLOSED = -21017;
    public static final int ER_ILLEGAL_FLAG = -21018;
    public static final int ER_ILLEGAL_DATA_SIZE = -21019;
    public static final int ER_NO_MORE_RESULT = -21020;
    public static final int ER_JCI_UNKNOWN = -21023;
    public static final int ER_TIMEOUT = -21024;
    public static final int ER_ILLEGAL_TIMESTAMP = -21027;
    public static final int ER_INTERNAL = -21028;

    public static final int ER_INVALID_JCI_CONNECTION_SHARD_REQUEST = -21091;
    public static final int ER_SHARD_INFO_RECV_FAIL = -21092; /* internal use only */
    public static final int ER_SHARD_MGMT = -21099; /* JciConnection internal exception. */

    private static final int CAS_ERROR_MIN = -20000;

    /* CAS Error Code */

    public static final int CAS_ER_DBMS = -10000;
    public static final int CAS_ER_INTERNAL = -10001;
    public static final int CAS_ER_NO_MORE_MEMORY = -10002;
    public static final int CAS_ER_COMMUNICATION = -10003;
    public static final int CAS_ER_ARGS = -10004;
    public static final int CAS_ER_TRAN_TYPE = -10005;
    public static final int CAS_ER_SRV_HANDLE = -10006;
    public static final int CAS_ER_NUM_BIND = -10007;
    public static final int CAS_ER_UNKNOWN_U_TYPE = -10008;
    public static final int CAS_ER_DB_VALUE = -10009;
    public static final int CAS_ER_TYPE_CONVERSION = -10010;
    public static final int CAS_ER_PARAM_NAME = -10011;
    public static final int CAS_ER_NO_MORE_DATA = -10012;
    public static final int CAS_ER_OBJECT = -10013;
    public static final int CAS_ER_OPEN_FILE = -10014;
    public static final int CAS_ER_SCHEMA_TYPE = -10015;
    public static final int CAS_ER_VERSION = -10016;
    public static final int CAS_ER_NOT_AUTHORIZED_CLIENT = -10017;
    public static final int CAS_ER_QUERY_CANCEL = -10018;
    public static final int CAS_ER_NO_MORE_RESULT_SET = -10019;
    public static final int CAS_ER_STMT_POOLING = -10020;
    public static final int CAS_ER_DBSERVER_DISCONNECTED = -10021;
    public static final int CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED = -10022;
    public static final int CAS_ER_HOLDABLE_NOT_ALLOWED = -10023;
    public static final int CAS_ER_QUERY_CANCELLED_BY_USER = -10024;

    /* broker error codes */
    public static final int BR_ER_FREE_SERVER = -11000;
    public static final int BR_ER_BROKER_NOT_FOUND = -11001;
    public static final int BR_ER_NOT_SHARD_MGMT_OPCODE = -11002;
    public static final int BR_ER_NO_MORE_MEMORY = -11003;
    public static final int BR_ER_INVALID_OPCODE = -11004;
    public static final int BR_ER_SHARD_INFO_NOT_AVAILABLE = -11005;
    public static final int BR_ER_COMMUNICATION = -11006;
    public static final int BR_ER_METADB = -11007;
    public static final int BR_ER_NODE_INFO_EXIST = -11008;
    public static final int BR_ER_NODE_INFO_NOT_EXIST = -11009;
    public static final int BR_ER_NODE_IN_USE = -11010;
    public static final int BR_ER_DBNAME_MISMATCHED = -11011;
    public static final int BR_ER_MIGRATION_INVALID_NODEID = -11012;
    public static final int BR_ER_REQUEST_TIMEOUT = -11013;
    public static final int BR_ER_REBALANCE_RUNNING = -11014;
    public static final int BR_ER_LAUNCH_PROCESS = -11015;
    public static final int BR_ER_NODE_ADD_INVALID_SRC_NODE = -11006;
    public static final int BR_ER_NODE_ADD_IN_PROGRESS = -11017;
    public static final int BR_ER_INVALID_ARGUMENT = -11018;
    public static final int BR_ER_SCHEMA_MIGRATION_FAIL = -11019;
    public static final int BR_ER_GLOBAL_TABLE_MIGRATION_FAIL = -11020;

    public static final int CAS_ER_NOT_IMPLEMENTED = -11000;

    public static final int CAS_ERROR_INDICATOR = -1;
    public static final int DBMS_ERROR_INDICATOR = -2;

    private static HashMap<Integer, String> messageString, CASMessageString;

    static {
	setMessageHash();
	setCASMessageHash();
    }

    public static String getErrorMsg(int index)
    {
	if (index > CAS_ERROR_MIN) {
	    return getCasErrorMsg(index);
	}

	return (String) messageString.get(new Integer(index));
    }

    public static String getCasErrorMsg(int index)
    {
	return (String) CASMessageString.get(new Integer(index));
    }

    private static void setMessageHash()
    {
	messageString = new HashMap<Integer, String>();

	/* driver error message */
	driverMsgPut(ER_UNKNOWN, "");
	driverMsgPut(ER_CONNECTION_CLOSED, "Attempt to operate on a closed Connection.");
	driverMsgPut(ER_STATEMENT_CLOSED, "Attempt to access a closed Statement.");
	driverMsgPut(ER_PREPARED_STATEMENT_CLOSED, "Attempt to access a closed PreparedStatement.");
	driverMsgPut(ER_RESULTSTE_CLOSED, "Attempt to access a closed ResultSet.");
	driverMsgPut(ER_NO_SUPPORTED, "Not supported method");
	driverMsgPut(ER_INVALID_TRAN_ISOLATION_LEVEL, "Unknown transaction isolation level.");
	driverMsgPut(ER_INVALID_URL, "invalid URL - ");
	driverMsgPut(ER_NO_DBNAME, "The database name should be given.");
	driverMsgPut(ER_INVALID_QUERY_TYPE_FOR_EXECUTEQUERY,
			"The query is not applicable to the executeQuery(). Use the executeUpdate() instead.");
	driverMsgPut(ER_INVALID_QUERY_TYPE_FOR_EXECUTEUPDATE,
			"The query is not applicable to the executeUpdate(). Use the executeQuery() instead.");
	driverMsgPut(ER_NEGATIVE_VALUE_FOR_LENGTH, "The length of the stream cannot be negative.");
	driverMsgPut(ER_IOEXCEPTION_IN_STREAM, "An IOException was caught during reading the inputstream.");
	driverMsgPut(ER_DEPRECATED, "Not supported method, because it is deprecated.");
	driverMsgPut(ER_NOT_NUMERIC_OBJECT, "The object does not seem to be a number.");
	driverMsgPut(ER_INVALID_INDEX, "Missing or invalid position of the bind variable provided.");
	driverMsgPut(ER_INVALID_COLUMN_NAME, "The column name is invalid.");
	driverMsgPut(ER_INVALID_ROW, "Invalid cursor position.");
	driverMsgPut(ER_CONVERSION_ERROR, "Type conversion error.");
	driverMsgPut(ER_INVALID_TUPLE, "Internal error: The number of attributes is different from the expected.");
	driverMsgPut(ER_INVALID_VALUE, "The argument is invalid.");
	driverMsgPut(ER_DBMETADATA_CLOSED, "Attempt to operate on a closed DatabaseMetaData.");
	driverMsgPut(ER_NON_SCROLLABLE,
			"Attempt to call a method related to scrollability of non-scrollable ResultSet.");
	driverMsgPut(ER_NON_SENSITIVE, "Attempt to call a method related to sensitivity of non-sensitive ResultSet.");
	driverMsgPut(ER_NON_UPDATABLE, "Attempt to call a method related to updatability of non-updatable ResultSet.");
	driverMsgPut(ER_NON_UPDATABLE_COLUMN, "Attempt to update a column which cannot be updated.");
	driverMsgPut(ER_EMPTY_INPUT_STREAM, "Given InputStream object has no data.");
	driverMsgPut(ER_EMPTY_READER, "Given Reader object has no data.");
	driverMsgPut(ER_NON_SCROLLABLE_STATEMENT,
			"Attempt to call a method related to scrollability of TYPE_FORWARD_ONLY Statement.");
	driverMsgPut(ER_POOLED_CONNECTION_CLOSED, "Attempt to operate on a closed PooledConnection.");
	driverMsgPut(ER_REQUEST_TIMEOUT, "Request timed out.");
	driverMsgPut(ER_INVALID_OBJECT_BIND, "Invalid object binded");
	driverMsgPut(ER_NOT_ALL_BINDED, "Not all the parameters are binded");
	driverMsgPut(ER_INTERNAL_ERROR, "Internal error:");
	driverMsgPut(ER_INVALID_QUERY_TYPE_FOR_EXECUTEBATCH, "The query is not applicable to the executeBatch().");

	driverMsgPut(ER_SHARD_GROUPID_INFO_OBSOLETE, "shard groupid information is obsolete");
	driverMsgPut(ER_SHARD_DIFFERENT_SHARD_KEY_INFO, "Internal error: invalid shard key info");
	driverMsgPut(ER_SHARD_DML_SHARD_KEY, "Cannot find appropriate shard key value");
	driverMsgPut(ER_SHARD_COMMIT_FAIL, "COMMIT failure on shard:\n");
	driverMsgPut(ER_SHARD_DML_FAIL, "DML failure on shard:\n");
	driverMsgPut(ER_SHARD_DDL_FAIL, "DDL failure on shard:\n");
	driverMsgPut(ER_SHARD_INTERNAL_ERROR, "Internal error:");
	driverMsgPut(ER_SHARD_INVALID_SHARDKEY_TYPE, "Invalid shardkey type is binded ");
	driverMsgPut(ER_SHARD_INCOMPATIBLE_CON_REQUEST, "Incompatible shard connection requests.");
	driverMsgPut(ER_SHARD_MORE_THAN_ONE_SHARD_TRAN, "Transaction over more than one shard key is not supported");
	driverMsgPut(ER_SHARD_GROUPID_INVALID, "invalid groupid");
	driverMsgPut(ER_SHARD_NODE_MAX_SERVICE_PORT_EXCEED, "Maximum number of service ports exceeded");
	driverMsgPut(ER_SHARD_NODE_CONNECTION_INVALID, "Cannot make a connection for the shard node");
	driverMsgPut(ER_SHARD_NODE_INFO_OBSOLETE, "internal error: shard node information is obsolete");
	driverMsgPut(ER_SHARD_INFO_INVALID, "Cannot receive shard information from server");

	/* jci error message */
	driverMsgPut(ER_JCI_UNKNOWN, "Error");
	driverMsgPut(ER_NO_ERROR, "No Error");
	driverMsgPut(ER_DBMS, "Server error");
	driverMsgPut(ER_COMMUNICATION, "Cannot communicate with the broker");
	driverMsgPut(ER_NO_MORE_DATA, "Invalid cursor position");
	driverMsgPut(ER_TYPE_CONVERSION, "Type conversion error");
	driverMsgPut(ER_NOT_BIND, "Attempt to execute the query when not all the parameters are binded");
	driverMsgPut(ER_COLUMN_INDEX, "Column index is out of range");
	driverMsgPut(ER_TRUNCATE, "Data is truncated because receive buffer is too small");
	driverMsgPut(ER_SCHEMA_TYPE, "Internal error: Illegal schema type");
	driverMsgPut(ER_FILE, "File access failed");
	driverMsgPut(ER_CONNECTION, "Cannot connect to a broker");
	driverMsgPut(ER_ILLEGAL_REQUEST, "Internal error: The requested information is not available");
	driverMsgPut(ER_INVALID_ARGUMENT, "The argument is invalid");
	driverMsgPut(ER_IS_CLOSED, "Connection or Statement might be closed");
	driverMsgPut(ER_ILLEGAL_FLAG, "Internal error: Invalid argument");
	driverMsgPut(ER_ILLEGAL_DATA_SIZE, "Cannot communicate with the broker or received invalid packet");
	driverMsgPut(ER_NO_MORE_RESULT, "No More Result");
	driverMsgPut(ER_TIMEOUT, "Request timed out");
	driverMsgPut(ER_ILLEGAL_TIMESTAMP, "Zero date can not be represented as java.sql.Timestamp");
	driverMsgPut(ER_INTERNAL, "Internal error");
	driverMsgPut(ER_INVALID_JCI_CONNECTION_SHARD_REQUEST,
			"internal error: invalid request of JciShardGroupConnection");
    }

    private static void driverMsgPut(int code, String msg)
    {
	messageString.put(new Integer(code), msg);
    }

    private static void setCASMessageHash()
    {
	CASMessageString = new HashMap<Integer, String>();

	casMsgPut(CAS_ER_DBMS, "Database connection error");
	casMsgPut(CAS_ER_INTERNAL, "General server error");
	casMsgPut(CAS_ER_NO_MORE_MEMORY, "Memory allocation error");
	casMsgPut(CAS_ER_COMMUNICATION, "Communication error");
	casMsgPut(CAS_ER_ARGS, "Invalid argument");
	casMsgPut(CAS_ER_TRAN_TYPE, "Unknown transaction type");
	casMsgPut(CAS_ER_SRV_HANDLE, "Internal server error");
	casMsgPut(CAS_ER_NUM_BIND, "Parameter binding error");
	casMsgPut(CAS_ER_UNKNOWN_U_TYPE, "Parameter binding error");
	casMsgPut(CAS_ER_DB_VALUE, "Cannot make DB_VALUE");
	casMsgPut(CAS_ER_TYPE_CONVERSION, "Type conversion error");
	casMsgPut(CAS_ER_PARAM_NAME, "Invalid database parameter name");
	casMsgPut(CAS_ER_NO_MORE_DATA, "No more data");
	casMsgPut(CAS_ER_OBJECT, "Object is not valid");
	casMsgPut(CAS_ER_OPEN_FILE, "File open error");
	casMsgPut(CAS_ER_SCHEMA_TYPE, "Invalid schema type");
	casMsgPut(CAS_ER_VERSION, "Version mismatch");
	casMsgPut(CAS_ER_NOT_AUTHORIZED_CLIENT, "Authorization error");
	casMsgPut(CAS_ER_QUERY_CANCEL, "Cannot cancel the query");
	casMsgPut(CAS_ER_NO_MORE_RESULT_SET, "No More Result");
	casMsgPut(CAS_ER_STMT_POOLING, "Statement Pooling");
	casMsgPut(CAS_ER_DBSERVER_DISCONNECTED, "DB Server disconnected");
	casMsgPut(CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED,
			"Cannot prepare more than MAX_PREPARED_STMT_COUNT statements");
	casMsgPut(CAS_ER_HOLDABLE_NOT_ALLOWED, "Holdable results may not be updatable or sensitive");
	casMsgPut(CAS_ER_QUERY_CANCELLED_BY_USER, "Query cancelled");
	casMsgPut(BR_ER_FREE_SERVER, "Cannot process the request. Try again later");
	casMsgPut(BR_ER_BROKER_NOT_FOUND, "Cannot find the broker name");
	casMsgPut(BR_ER_NO_MORE_MEMORY, "Broker cannot allocate memory");

	casMsgPut(BR_ER_COMMUNICATION, "Cannot communicate with the shard_mgmt");
	casMsgPut(BR_ER_METADB, "Cannot access to metadb");
	casMsgPut(BR_ER_NODE_INFO_EXIST, "The shard node already exists");
	casMsgPut(BR_ER_NODE_INFO_NOT_EXIST, "The shard node does not exist");
	casMsgPut(BR_ER_NODE_IN_USE, "The shard node is in use");

	casMsgPut(BR_ER_DBNAME_MISMATCHED, "wrong database name");
	casMsgPut(BR_ER_MIGRATION_INVALID_NODEID, "migration: invalid nodeid");
	casMsgPut(BR_ER_REQUEST_TIMEOUT, "request timeout");
	casMsgPut(BR_ER_REBALANCE_RUNNING, "The request is not permitted because of ongoing rebalancing operation");
	casMsgPut(BR_ER_NODE_ADD_INVALID_SRC_NODE, "invalid node id");
	casMsgPut(BR_ER_LAUNCH_PROCESS, "Internal error: cannot launch process");
	casMsgPut(BR_ER_NODE_ADD_IN_PROGRESS, "The request is not permitted because of ongoing node-add operation");
	casMsgPut(BR_ER_INVALID_ARGUMENT, "Internal error: invalid argument");
	casMsgPut(BR_ER_SCHEMA_MIGRATION_FAIL, "schema migration fail");
	casMsgPut(BR_ER_GLOBAL_TABLE_MIGRATION_FAIL, "global table migration fail");

	casMsgPut(CAS_ER_NOT_IMPLEMENTED, "Attempt to use a not supported service");
    }

    private static void casMsgPut(int code, String msg)
    {
	CASMessageString.put(new Integer(code), msg);
    }

    public static boolean isBrokerNotAvailable(int errCode)
    {
	switch (errCode)
	{
	case ER_COMMUNICATION:
	case ER_CONNECTION:
	case ER_TIMEOUT:
	case BR_ER_FREE_SERVER:
	case BR_ER_BROKER_NOT_FOUND:
	    return true;
	default:
	    return false;
	}
    }
}
