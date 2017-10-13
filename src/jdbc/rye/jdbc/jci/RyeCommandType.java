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

abstract public class RyeCommandType
{
    public enum SQL_TYPE {
	UNKNOWN, DDL, DML, SELECT, SELECT_UPDATE, COMMIT, ROLLBACK, IGNORE_ON_SHARDING
    };

    private static SQL_TYPE[] statementType;

    /* RYE_STMT_XXX should be the same value with RYE_STMT_TYPE in api/rye_api.h */
    public final static byte RYE_STMT_ALTER_CLASS = 0;
    public final static byte RYE_STMT_ALTER_SERIAL = 1;
    public final static byte RYE_STMT_COMMIT_WORK = 2;
    public final static byte RYE_STMT_REGISTER_DATABASE = 3;
    public final static byte RYE_STMT_CREATE_CLASS = 4;
    public final static byte RYE_STMT_CREATE_INDEX = 5;
    public final static byte RYE_STMT_CREATE_SERIAL = 7;
    public final static byte RYE_STMT_DROP_DATABASE = 8;
    public final static byte RYE_STMT_DROP_CLASS = 9;
    public final static byte RYE_STMT_DROP_INDEX = 10;
    public final static byte RYE_STMT_DROP_LABEL = 11;
    public final static byte RYE_STMT_DROP_SERIAL = 13;
    public final static byte RYE_STMT_RENAME_CLASS = 15;
    public final static byte RYE_STMT_ROLLBACK_WORK = 16;
    public final static byte RYE_STMT_GRANT = 17;
    public final static byte RYE_STMT_REVOKE = 18;
    public final static byte RYE_STMT_STATISTICS = 19;
    public final static byte RYE_STMT_INSERT = 20;
    public final static byte RYE_STMT_SELECT = 21;
    public final static byte RYE_STMT_UPDATE = 22;
    public final static byte RYE_STMT_DELETE = 23;
    public final static byte RYE_STMT_GET_ISO_LVL = 25;
    public final static byte RYE_STMT_GET_TIMEOUT = 26;
    public final static byte RYE_STMT_GET_OPT_LVL = 27;
    public final static byte RYE_STMT_SET_OPT_LVL = 28;
    public final static byte RYE_STMT_SCOPE = 29;
    public final static byte RYE_STMT_SAVEPOINT = 32;
    public final static byte RYE_STMT_ON_LDB = 38;
    public final static byte RYE_STMT_GET_LDB = 39;
    public final static byte RYE_STMT_SET_LDB = 40;
    public final static byte RYE_STMT_CREATE_USER = 42;
    public final static byte RYE_STMT_DROP_USER = 43;
    public final static byte RYE_STMT_ALTER_USER = 44;
    public final static byte RYE_STMT_SET_SYS_PARAMS = 45;
    public final static byte RYE_STMT_ALTER_INDEX = 46;
    public final static byte RYE_STMT_SELECT_UPDATE = 49;

    public final static byte RYE_STMT_UNKNOWN = 53;
    public final static byte RYE_STMT_MAX = RYE_STMT_UNKNOWN;

    static {
	statementType = new SQL_TYPE[RYE_STMT_MAX];
	for (int i = 0; i < statementType.length; i++) {
	    statementType[i] = SQL_TYPE.UNKNOWN;
	}

	statementType[RYE_STMT_ALTER_CLASS] = SQL_TYPE.DDL;
	statementType[RYE_STMT_CREATE_CLASS] = SQL_TYPE.DDL;
	statementType[RYE_STMT_CREATE_INDEX] = SQL_TYPE.DDL;
	statementType[RYE_STMT_DROP_CLASS] = SQL_TYPE.DDL;
	statementType[RYE_STMT_DROP_INDEX] = SQL_TYPE.DDL;
	statementType[RYE_STMT_RENAME_CLASS] = SQL_TYPE.DDL;
	statementType[RYE_STMT_GRANT] = SQL_TYPE.DDL;
	statementType[RYE_STMT_REVOKE] = SQL_TYPE.DDL;
	statementType[RYE_STMT_STATISTICS] = SQL_TYPE.DDL;
	statementType[RYE_STMT_CREATE_USER] = SQL_TYPE.DDL;
	statementType[RYE_STMT_DROP_USER] = SQL_TYPE.DDL;
	statementType[RYE_STMT_ALTER_USER] = SQL_TYPE.DDL;
	statementType[RYE_STMT_ALTER_INDEX] = SQL_TYPE.DDL;

	statementType[RYE_STMT_INSERT] = SQL_TYPE.DML;
	statementType[RYE_STMT_UPDATE] = SQL_TYPE.DML;
	statementType[RYE_STMT_DELETE] = SQL_TYPE.DML;

	statementType[RYE_STMT_SELECT] = SQL_TYPE.SELECT;
	statementType[RYE_STMT_GET_ISO_LVL] = SQL_TYPE.SELECT;
	statementType[RYE_STMT_GET_TIMEOUT] = SQL_TYPE.SELECT;
	statementType[RYE_STMT_SELECT_UPDATE] = SQL_TYPE.SELECT_UPDATE;

	statementType[RYE_STMT_COMMIT_WORK] = SQL_TYPE.COMMIT;
	statementType[RYE_STMT_ROLLBACK_WORK] = SQL_TYPE.ROLLBACK;

	statementType[RYE_STMT_ALTER_SERIAL] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_REGISTER_DATABASE] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_CREATE_SERIAL] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_DROP_DATABASE] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_DROP_LABEL] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_DROP_SERIAL] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_GET_OPT_LVL] = SQL_TYPE.UNKNOWN;
	// statementType[RYE_STMT_SET_OPT_LVL] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_SET_OPT_LVL] = SQL_TYPE.DML;
	statementType[RYE_STMT_SCOPE] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_SAVEPOINT] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_ON_LDB] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_GET_LDB] = SQL_TYPE.UNKNOWN;
	statementType[RYE_STMT_SET_LDB] = SQL_TYPE.UNKNOWN;
	// statementType[RYE_STMT_SET_SYS_PARAMS] = SQL_TYPE.IGNORE_ON_SHARDING;
	statementType[RYE_STMT_SET_SYS_PARAMS] = SQL_TYPE.DML;
    }

    public static SQL_TYPE getSQLType(int stmt)
    {
	if (stmt >= 0 && stmt < RYE_STMT_UNKNOWN) {
	    return statementType[stmt];
	}
	else {
	    return SQL_TYPE.UNKNOWN;
	}
    }

    public static boolean hasResultset(int stmt)
    {
	SQL_TYPE type = getSQLType(stmt);
	return (type == SQL_TYPE.SELECT || type == SQL_TYPE.SELECT_UPDATE);
    }

    public static byte unknownStmt()
    {
	return RYE_STMT_UNKNOWN;
    }
}
