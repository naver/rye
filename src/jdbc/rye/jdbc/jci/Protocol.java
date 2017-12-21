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

package rye.jdbc.jci;

import rye.jdbc.driver.RyeDriver;
import rye.jdbc.driver.RyeErrorCode;

abstract public class Protocol
{
    private static final int BRREQ_MSG_MAGIC_LEN = 4;
    private static final byte[] BRREQ_MSG_MAGIC_STR = { 'R', 'Y', 'E', 0x01 };
    private static final int BRREQ_MSG_SIZE = 20;

    private static final int BROKER_RESPONSE_MAX_ADDITIONAL_MSG = 5;
    private static final int BROKER_RESPONSE_SIZE = 12 + 4 * BROKER_RESPONSE_MAX_ADDITIONAL_MSG;

    public static final byte BR_RES_SHARD_INFO_ALL = 0, BR_RES_SHARD_INFO_CHANGED_ONLY = 1;

    /* normal broker request */
    public static final byte BRREQ_OP_CODE_CAS_CONNECT = 1;
    public static final byte BRREQ_OP_CODE_QUERY_CANCEL = 2;
    /* local mgmt request */
    public static final byte BRREQ_OP_CODE_LAUNCH_PROCESS = 17;
    public static final byte BRREQ_OP_CODE_GET_SHARD_MGMT_INFO = 18;
    public static final byte BRREQ_OP_CODE_NUM_SHARD_VERSION_INFO = 19;
    public static final byte BRREQ_OP_CODE_READ_RYE_FILE = 20;
    public static final byte BRREQ_OP_CODE_WRITE_RYE_CONF = 21;
    public static final byte BRREQ_OP_CODE_UPDATE_CONF = 22;
    public static final byte BRREQ_OP_CODE_DELETE_CONF = 23;
    public static final byte BRREQ_OP_CODE_GET_CONF = 24;
    public static final byte BRREQ_OP_CODE_BR_ACL_RELOAD = 25;
    public static final byte BRREQ_OP_CODE_PING = 27;
    public static final byte BRREQ_OP_CODE_RM_TMP_FILE = 28;

    /* shard mgmt request */
    public static final byte BRREQ_OP_CODE_GET_SHARD_INFO = 64;
    public static final byte BRREQ_OP_CODE_INIT = 65;
    public static final byte BRREQ_OP_CODE_ADD_NODE = 66;
    public static final byte BRREQ_OP_CODE_DROP_NODE = 67;
    public static final byte BRREQ_OP_CODE_MIGRATION_START = 68;
    public static final byte BRREQ_OP_CODE_MIGRATION_END = 69;
    public static final byte BRREQ_OP_CODE_DDL_START = 70;
    public static final byte BRREQ_OP_CODE_DDL_END = 71;
    public static final byte BRREQ_OP_CODE_REBALANCE_REQ = 72;
    public static final byte BRREQ_OP_CODE_REBALANCE_JOB_COUNT = 73;
    public static final byte BRREQ_OP_CODE_GC_START = 74;
    public static final byte BRREQ_OP_CODE_GC_END = 75;
    public static final byte BRREQ_OP_CODE_PING_SHARD_MGMT = 76;

    public static final int READ_RYE_FILE_RYE_CONF = 1;
    public static final int READ_RYE_FILE_BR_ACL = 2;

    private static final Integer BR_MGMT_REQ_LAST_ARG_VALUE = 0x12345678;

    public static final int BRREQ_REBALANCE_TYPE_REBALANCE = 0;
    public static final int BRREQ_REBALANCE_TYPE_EMPTY_NODE = 1;

    public static final int MGMT_REBALANCE_JOB_COUNT_TYPE_REMAIN = 0;
    public static final int MGMT_REBALANCE_JOB_COUNT_TYPE_COMPLETE = 1;
    public static final int MGMT_REBALANCE_JOB_COUNT_TYPE_FAILED = 2;

    public static final int MGMT_LAUNCH_PROCESS_MIGRATOR = 1;
    public static final int MGMT_LAUNCH_PROCESS_RYE_COMMAND = 2;

    public static final int MGMT_LAUNCH_FLAG_NO_FLAG = 0;
    public static final int MGMT_LAUNCH_FLAG_NO_RESULT = 1;

    public static final int MGMT_LAUNCH_ERROR_EXEC_FAIL = -101;
    public static final int MGMT_LAUNCH_ERROR_ABNORMALLY_TERMINATED = -102;

    private static final byte CAS_CLIENT_JDBC = 1;

    static final byte CAS_FC_CONNECT_DB = 0;
    static final byte CAS_FC_END_TRAN = 1;
    static final byte CAS_FC_PREPARE = 2;
    static final byte CAS_FC_EXECUTE = 3;
    static final byte CAS_FC_GET_DB_PARAMETER = 4;
    static final byte CAS_FC_CLOSE_REQ_HANDLE = 5;
    static final byte CAS_FC_FETCH = 6;
    static final byte CAS_FC_SCHEMA_INFO = 7;
    static final byte CAS_FC_GET_DB_VERSION = 8;
    static final byte CAS_FC_GET_CLASS_NUM_OBJS = 9;
    static final byte CAS_FC_EXECUTE_BATCH = 10;
    static final byte CAS_FC_GET_QUERY_PLAN = 11;
    static final byte CAS_FC_CON_CLOSE = 12;
    static final byte CAS_FC_CHECK_CAS = 13;
    static final byte CAS_FC_CURSOR_CLOSE = 14;
    static final byte CAS_FC_CHANGE_DBUSER = 15;
    static final byte CAS_FC_UPDATE_GROUP_ID = 16;
    static final byte CAS_FC_SERVER_MODE = 21;

    static final byte CAS_CURSOR_STATUS_OPEN = 0;
    static final byte CAS_CURSOR_STATUS_CLOSED = 1;

    static final String HEALTH_CHECK_DUMMY_DB = "___health_check_dummy_db___";

    static final int SESSION_ID_SIZE = 20;
    static final int CAS_STATUS_INFO_SIZE = 16;
    static final int MSG_HEADER_SIZE = 4 + CAS_STATUS_INFO_SIZE;
    static final byte CAS_STATUS_INFO_IDX_STATUS = 0;
    static final byte CAS_STATUS_INFO_IDX_SERVER_NODEID = 1;
    static final byte CAS_STATUS_INFO_IDX_SHARD_INFO_VER = 3;

    static final byte CAS_INFO_STATUS_INACTIVE = 0;
    static final byte CAS_INFO_STATUS_ACTIVE = 1;

    static final byte ERROR_RESPONSE = 0;
    static final byte SUCCESS_RESPONSE = 1;

    static final byte EXEC_QUERY_ERROR = 0;
    static final byte EXEC_QUERY_SUCCESS = 1;

    static final byte CAS_DBMS_RYE = 1;

    static final byte CAS_HOLDABLE_RESULT_NOT_SUPPORT = 0;
    static final byte CAS_HOLDABLE_RESULT_SUPPORT = 1;

    static final byte CAS_STATEMENT_POOLING_OFF = 0;
    static final byte CAS_STATEMENT_POOLING_ON = 1;

    static final byte END_TRAN_COMMIT = 1;
    static final byte END_TRAN_ROLLBACK = 2;

    static final byte PREPARE_FLAG_HOLDABLE = 0x01;

    static final byte EXEC_CONTAIN_FETCH_RESULT = 0x01;

    static final byte CLOSE_CURRENT_RESULT = 0x00;
    static final byte KEEP_CURRENT_RESULT = 0x01;

    public static final int SCH_TABLE = 1;
    public static final int SCH_QUERY_SPEC = 2; /* not used */
    public static final int SCH_COLUMN = 3;
    public static final int SCH_INDEX_INFO = 4;
    public static final int SCH_TABLE_PRIVILEGE = 5;
    public static final int SCH_COLUMN_PRIVILEGE = 6;
    public static final int SCH_PRIMARY_KEY = 7;

    public static final int SCH_TABLE_IDX_NAME = 0;
    public static final int SCH_TABLE_IDX_TYPE = 1;

    public static final int SCH_TABLE_TYPE_TABLE = 2;
    public static final int SCH_TABLE_TYPE_VIEW = 1;
    public static final int SCH_TABLE_TYPE_SYSTEM = 0;

    public static final int SCH_COLUMN_IDX_NAME = 0;
    public static final int SCH_COLUMN_IDX_TYPE = 1;
    public static final int SCH_COLUMN_IDX_SCALE = 2;
    public static final int SCH_COLUMN_IDX_PRECISION = 3;
    public static final int SCH_COLUMN_IDX_INDEXED = 4;
    public static final int SCH_COLUMN_IDX_NON_NULL = 5;
    public static final int SCH_COLUMN_IDX_UNIQUE = 6;
    public static final int SCH_COLUMN_IDX_DEFAULT = 7;
    public static final int SCH_COLUMN_IDX_ORDER = 8;
    public static final int SCH_COLUMN_IDX_TABLE_NAME = 9;
    public static final int SCH_COLUMN_IDX_IS_KEY = 10;

    public static final int SCH_INDEX_INFO_IDX_TYPE = 0;
    public static final int SCH_INDEX_INFO_IDX_IS_UNIQUE = 1;
    public static final int SCH_INDEX_INFO_IDX_NAME = 2;
    public static final int SCH_INDEX_INFO_IDX_KEY_ORDER = 3;
    public static final int SCH_INDEX_INFO_IDX_COLUMN_NAME = 4;
    public static final int SCH_INDEX_INFO_IDX_ASC_DESC = 5;
    public static final int SCH_INDEX_INFO_IDX_NUM_KEYS = 6;
    public static final int SCH_INDEX_INFO_IDX_NUM_PAGES = 7;

    public static final int SCH_PRIVILEGE_IDX_NAME = 0;
    public static final int SCH_PRIVILEGE_IDX_GRANTOR = 1;
    public static final int SCH_PRIVILEGE_IDX_GRANTEE = 2;
    public static final int SCH_PRIVILEGE_IDX_PRIVILEGE = 3;
    public static final int SCH_PRIVILEGE_IDX_GRANTABLE = 4;

    public static final int SCH_PK_IDX_TABLE_NAME = 0;
    public static final int SCH_PK_IDX_COLUMN_NAME = 1;
    public static final int SCH_PK_IDX_KEY_SEQ = 2;
    public static final int SCH_PK_IDX_KEY_NAME = 3;

    private static final byte MGMT_REQ_ARG_INT = 1;
    private static final byte MGMT_REQ_ARG_INT64 = 2;
    private static final byte MGMT_REQ_ARG_STR = 3;
    private static final byte MGMT_REQ_ARG_STR_ARRAY = 4;
    private static final byte MGMT_REQ_ARG_INT_ARRAY = 5;

    private final static int MGMT_REQ_ARG_TYPE_SIZE = 1;
    private final static int MGMT_REQ_ARG_INT_SIZE = 4;
    private final static int MGMT_REQ_ARG_INT64_SIZE = 8;

    public static final int SERVER_ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED = -111;
    public static final int SERVER_ER_NET_SERVER_CRASHED = -199;
    public static final int SERVER_ER_OBJ_NO_CONNECT = -224;
    public static final int SERVER_ER_BO_CONNECT_FAILED = -677;
    public static final int SERVER_ER_SHARD_INVALID_GROUPID = -506;
    public static final int SERVER_ER_SHARD_CANT_GLOBAL_DML_UNDER_MIGRATION = -513;

    public static final int HA_STATE_NA = -1;
    public static final int HA_STATE_UNKNOWN = 0; /* initial state */
    public static final int HA_STATE_MASTER = 1;
    public static final int HA_STATE_TO_BE_MASTER = 2;
    public static final int HA_STATE_SLAVE = 3;
    public static final int HA_STATE_TO_BE_SLAVE = 4;
    public static final int HA_STATE_REPLICA = 5;
    public static final int HA_STATE_DEAD = 6; /* server is dead - virtual state; not exists */

    public static final int HA_STATE_FOR_DRIVER_UNKNOWN = 0;
    public static final int HA_STATE_FOR_DRIVER_MASTER = 1;
    public static final int HA_STATE_FOR_DRIVER_TO_BE_MASTER = 2;
    public static final int HA_STATE_FOR_DRIVER_SLAVE = 3;
    public static final int HA_STATE_FOR_DRIVER_TO_BE_SLAVE = 4;
    public static final int HA_STATE_FOR_DRIVER_REPLICA = 5;

    static byte[] getConnectMsg(byte[] portNameMsgProtocol)
    {
	byte[] msg = packBrokerRequestMsg(BRREQ_OP_CODE_CAS_CONNECT, (short) portNameMsgProtocol.length);
	JciUtil.copy_bytes(msg, BRREQ_MSG_SIZE, portNameMsgProtocol.length, portNameMsgProtocol);
	return msg;
    }

    static byte[] getPingCheckMsg(byte[] portNameMsgProtocol)
    {
	byte[] msg = packBrokerRequestMsg(BRREQ_OP_CODE_PING, (short) portNameMsgProtocol.length);
	JciUtil.copy_bytes(msg, BRREQ_MSG_SIZE, portNameMsgProtocol.length, portNameMsgProtocol);
	return msg;
    }

    static byte[] getQueryCancelMsg(int casId, int casPid, byte[] portNameMsgProtocol)
    {
	byte[] msg = packBrokerRequestMsg(BRREQ_OP_CODE_QUERY_CANCEL, (short) (portNameMsgProtocol.length + 8));
	int pos = JciUtil.copy_bytes(msg, BRREQ_MSG_SIZE, portNameMsgProtocol.length, portNameMsgProtocol);
	pos = JciUtil.int2bytes(casId, msg, pos);
	pos = JciUtil.int2bytes(casPid, msg, pos);
	return msg;
    }

    static byte[] packPortName(String portName)
    {
	byte[] msg;
	int pos = 0;

	if (portName == null) {
	    msg = new byte[4];
	    pos = JciUtil.int2bytes(0, msg, pos);
	}
	else {
	    byte[] tmpBytes = portName.trim().toLowerCase().getBytes();
	    msg = new byte[4 + tmpBytes.length + 1];
	    pos = JciUtil.int2bytes(tmpBytes.length + 1, msg, pos);
	    pos = JciUtil.copy_bytes(msg, pos, tmpBytes.length, tmpBytes);
	    pos = JciUtil.copy_byte(msg, pos, (byte) '\0');
	}
	return msg;
    }

    public static byte[] mgmtRequestMsg(byte opcode, Object... args) throws JciException
    {
	int msgSize = 0;
	byte[] msg = new byte[1024];
	int pos = 0;

	msgSize += MGMT_REQ_ARG_INT_SIZE; /* num args */
	msg = checkMgmtMsgBuf(msg, pos, msgSize);
	pos = copyMgmtArgInt(msg, pos, args.length + 1);

	for (int argIdx = 0; argIdx < args.length; argIdx++) {
	    Object argValue = args[argIdx];
	    if (argValue == null) {
		throw new JciException(RyeErrorCode.ER_INVALID_ARGUMENT);
	    }

	    if (argValue instanceof Integer) {
		msgSize += (MGMT_REQ_ARG_TYPE_SIZE + MGMT_REQ_ARG_INT_SIZE);
		msg = checkMgmtMsgBuf(msg, pos, msgSize);

		pos = copyMgmtArgType(msg, pos, MGMT_REQ_ARG_INT);
		pos = copyMgmtArgInt(msg, pos, ((Integer) argValue).intValue());
	    }
	    else if (argValue instanceof Long) {
		msgSize += (MGMT_REQ_ARG_TYPE_SIZE + MGMT_REQ_ARG_INT64_SIZE);
		msg = checkMgmtMsgBuf(msg, pos, msgSize);

		pos = copyMgmtArgType(msg, pos, MGMT_REQ_ARG_INT64);
		pos = copyMgmtArgLong(msg, pos, ((Long) argValue).longValue());
	    }
	    else if (argValue instanceof String || argValue instanceof byte[]) {
		byte[] bStr;
		if (argValue instanceof String) {
		    bStr = ((String) argValue).getBytes();
		}
		else {
		    bStr = ((byte[]) argValue);
		}

		msgSize += (MGMT_REQ_ARG_TYPE_SIZE + MGMT_REQ_ARG_INT_SIZE + bStr.length + 1);
		msg = checkMgmtMsgBuf(msg, pos, msgSize);

		pos = copyMgmtArgType(msg, pos, MGMT_REQ_ARG_STR);
		pos = copyMgmtArgString(msg, pos, bStr);
	    }
	    else if (argValue instanceof String[]) {
		String[] strArr = (String[]) argValue;
		msgSize += (MGMT_REQ_ARG_TYPE_SIZE + MGMT_REQ_ARG_INT_SIZE);
		msg = checkMgmtMsgBuf(msg, pos, msgSize);

		pos = copyMgmtArgType(msg, pos, MGMT_REQ_ARG_STR_ARRAY);
		pos = copyMgmtArgInt(msg, pos, strArr.length);

		for (int j = 0; j < strArr.length; j++) {
		    if (strArr[j] == null) {
			throw new JciException(RyeErrorCode.ER_INVALID_ARGUMENT);
		    }
		    byte[] bStr = strArr[j].getBytes();

		    msgSize += (MGMT_REQ_ARG_INT_SIZE + bStr.length + 1);
		    msg = checkMgmtMsgBuf(msg, pos, msgSize);

		    pos = copyMgmtArgString(msg, pos, bStr);
		}
	    }
	    else if (argValue instanceof int[]) {
		int[] intArr = (int[]) argValue;
		msgSize += (MGMT_REQ_ARG_TYPE_SIZE + MGMT_REQ_ARG_INT_SIZE + MGMT_REQ_ARG_INT_SIZE * intArr.length);
		msg = checkMgmtMsgBuf(msg, pos, msgSize);

		pos = copyMgmtArgType(msg, pos, MGMT_REQ_ARG_INT_ARRAY);
		pos = copyMgmtArgInt(msg, pos, intArr.length);

		for (int j = 0; j < intArr.length; j++) {
		    pos = copyMgmtArgInt(msg, pos, intArr[j]);
		}
	    }
	    else {
		throw new JciException(RyeErrorCode.ER_INVALID_ARGUMENT);
	    }
	}

	msgSize += (MGMT_REQ_ARG_TYPE_SIZE + MGMT_REQ_ARG_INT_SIZE);
	msg = checkMgmtMsgBuf(msg, pos, msgSize);
	pos = copyMgmtArgType(msg, pos, MGMT_REQ_ARG_INT);
	pos = copyMgmtArgInt(msg, pos, BR_MGMT_REQ_LAST_ARG_VALUE);

	if (pos != msgSize) {
	    throw new JciException(RyeErrorCode.ER_INVALID_ARGUMENT);
	}

	byte[] brRequest = packBrokerRequestMsg(opcode, (short) msgSize);
	System.arraycopy(msg, 0, brRequest, BRREQ_MSG_SIZE, pos);

	return brRequest;
    }

    static private byte[] checkMgmtMsgBuf(byte[] msg, int pos, int requiredSize)
    {
	int newSize = msg.length;

	while (newSize < requiredSize) {
	    newSize *= 2;
	}

	if (newSize != msg.length) {
	    byte[] tmpbuf = new byte[newSize];
	    System.arraycopy(msg, 0, tmpbuf, 0, pos);
	    msg = tmpbuf;
	}

	return msg;
    }

    static private int copyMgmtArgType(byte[] msg, int pos, byte type)
    {
	pos = JciUtil.copy_byte(msg, pos, type);
	return pos;
    }

    static private int copyMgmtArgLong(byte[] msg, int pos, long value)
    {
	pos = JciUtil.long2bytes(value, msg, pos);
	return pos;
    }

    static private int copyMgmtArgInt(byte[] msg, int pos, int value)
    {
	pos = JciUtil.int2bytes(value, msg, pos);
	return pos;
    }

    static private int copyMgmtArgString(byte[] msg, int pos, byte[] bStr)
    {
	pos = JciUtil.int2bytes(bStr.length + 1, msg, pos);
	pos = JciUtil.copy_bytes(msg, pos, bStr.length, bStr);
	pos = JciUtil.copy_byte(msg, pos, (byte) 0);
	return pos;
    }

    static BrokerResponse unpackBrokerResponse(byte[] resMsg)
    {
	if (resMsg.length < BROKER_RESPONSE_SIZE) {
	    return null;
	}

	int msgPos = 0;

	short verMajor = JciUtil.bytes2short(resMsg, msgPos);
	msgPos += 2;
	short verMinor = JciUtil.bytes2short(resMsg, msgPos);
	msgPos += 2;
	short verPatch = JciUtil.bytes2short(resMsg, msgPos);
	msgPos += 2;
	short verBuild = JciUtil.bytes2short(resMsg, msgPos);
	msgPos += 2;

	int resCode = JciUtil.bytes2int(resMsg, msgPos);
	msgPos += 4;

	int[] additionalMsgSize = new int[BROKER_RESPONSE_MAX_ADDITIONAL_MSG];
	for (int i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++) {
	    additionalMsgSize[i] = JciUtil.bytes2int(resMsg, msgPos);
	    msgPos += 4;
	}

	return (new BrokerResponse(verMajor, verMinor, verPatch, verBuild, resCode, additionalMsgSize));
    }

    static private byte[] packBrokerRequestMsg(byte opcode, short opcodeMsgSize)
    {
	byte[] msg = new byte[BRREQ_MSG_SIZE + opcodeMsgSize];

	int pos = 0;
	pos = JciUtil.copy_bytes(msg, pos, BRREQ_MSG_MAGIC_LEN, BRREQ_MSG_MAGIC_STR);
	pos = JciUtil.short2bytes(RyeDriver.driverVersion.getMajor(), msg, pos);
	pos = JciUtil.short2bytes(RyeDriver.driverVersion.getMinor(), msg, pos);
	pos = JciUtil.short2bytes(RyeDriver.driverVersion.getPatch(), msg, pos);
	pos = JciUtil.short2bytes(RyeDriver.driverVersion.getBuild(), msg, pos);

	pos = JciUtil.copy_byte(msg, pos, CAS_CLIENT_JDBC);
	pos = JciUtil.copy_byte(msg, pos, opcode);

	pos = JciUtil.short2bytes(opcodeMsgSize, msg, pos);

	return msg;
    }

    static byte[] createNullSessionId()
    {
	return new byte[SESSION_ID_SIZE];
    }
}
