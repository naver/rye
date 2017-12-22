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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import rye.jdbc.jci.JciResultTuple;
import rye.jdbc.jci.Protocol;
import rye.jdbc.jci.JciConnection;
import rye.jdbc.jci.JciStatement;

public class RyeDatabaseMetaData implements DatabaseMetaData
{
    /* below compare key is 0 base column's index */
    private static final int[] tablesCompareKey = { 3, 2 };
    private static final int[] columnsCompareKey = { 2, 16 };
    private static final int[] columnPrivilegesCompareKey = { 3, 6 };
    private static final int[] tablePrivilegesCompareKey = { 2, 5 };
    private static final int[] primaryKeyCompareKey = { 3 };
    private static final int[] indexInfoCompareKey = { 3, 6, 5, 7 };

    private static final int SQL_MAX_CHAR_LITERAL_LEN = 1073741823;

    RyeConnection con;
    JciConnection jciCon;
    int major_version;
    int minor_version;

    protected RyeDatabaseMetaData(RyeConnection c)
    {
	con = c;
	jciCon = con.jciCon;
	major_version = -1;
	minor_version = -1;
    }

    /*
     * java.sql.DatabaseMetaData interface
     */

    public synchronized boolean allProceduresAreCallable() throws SQLException
    {
	return false;
    }

    public synchronized boolean allTablesAreSelectable() throws SQLException
    {
	return false;
    }

    public synchronized String getURL() throws SQLException
    {
	return con.url;
    }

    public synchronized String getUserName() throws SQLException
    {
	return con.user;
    }

    public synchronized boolean isReadOnly() throws SQLException
    {
	return false;
    }

    public synchronized boolean nullsAreSortedHigh() throws SQLException
    {
	return false;
    }

    public synchronized boolean nullsAreSortedLow() throws SQLException
    {
	return true;
    }

    public synchronized boolean nullsAreSortedAtStart() throws SQLException
    {
	return false;
    }

    public synchronized boolean nullsAreSortedAtEnd() throws SQLException
    {
	return false;
    }

    public synchronized String getDatabaseProductName() throws SQLException
    {
	return "Rye";
    }

    public synchronized String getDatabaseProductVersion() throws SQLException
    {
	String ver = jciCon.getVersionRequest();
	if (ver == null) {
	    return "";
	}
	else {
	    StringTokenizer st = new StringTokenizer(ver, ".");
	    if (st.countTokens() == 4) { // ex)
					 // 8.4.9.9999(major.minor.patch.build
		this.major_version = Integer.parseInt(st.nextToken());
		this.minor_version = Integer.parseInt(st.nextToken());
	    }

	    return ver;
	}
    }

    public synchronized String getDriverName() throws SQLException
    {
	return "Rye JDBC Driver";
    }

    public synchronized String getDriverVersion() throws SQLException
    {
	return RyeDriver.version_string;
    }

    public int getDriverMajorVersion()
    {
	return RyeDriver.driverVersion.getMajor();
    }

    public int getDriverMinorVersion()
    {
	return RyeDriver.driverVersion.getMinor();
    }

    public synchronized boolean usesLocalFiles() throws SQLException
    {
	return false;
    }

    public synchronized boolean usesLocalFilePerTable() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsMixedCaseIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized boolean storesUpperCaseIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized boolean storesLowerCaseIdentifiers() throws SQLException
    {
	return true;
    }

    public synchronized boolean storesMixedCaseIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
	return false;
    }

    public synchronized String getIdentifierQuoteString() throws SQLException
    {
	return "\"";
    }

    public synchronized String getSQLKeywords() throws SQLException
    {
	return "ADD, ADD_MONTHS, AFTER, ALIAS, ASYNC, ATTACH, ATTRIBUTE, BEFORE, "
			+ "BOOLEAN, BREADTH, CALL, CHANGE, CLASS, CLASSES, CLUSTER, COMPLETION, "
			+ "CYCLE, DATA, DATA_TYPE___, DEPTH, DICTIONARY, DIFFERENCE, EACH, ELSEIF, "
			+ "EQUALS, EVALUATE, EXCLUDE, FILE, FUNCTION, GENERAL, IF, IGNORE, INCREMENT, "
			+ "INDEX, INHERIT, INOUT, INTERSECTION, LAST_DAY, LDB, LEAVE, LESS, LIMIT, "
			+ "LIST, LOOP, LPAD, LTRIM, MAXVALUE, METHOD, MINVALUE, MODIFY, MONETARY, "
			+ "MONTHS_BETWEEN, MULTISET, MULTISET_OF, NA, NOCYCLE, NOMAXVALUE, NOMINVALUE, "
			+ "NONE, OBJECT, OFF, OID, OLD, OPERATION, OPERATORS, OPTIMIZATION, OTHERS, "
			+ "OUT, PARAMETERS, PENDANT, PREORDER, PRIVATE, PROXY, PROTECTED, QUERY, "
			+ "RECURSIVE, REF, REFERENCING, REGISTER, RENAME, REPLACE, RESIGNAL, RETURN, "
			+ "RETURNS, ROLE, ROUTINE, ROW, RPAD, RTRIM, SAVEPOINT, SCOPE___, SEARCH, "
			+ "SENSITIVE, SEQUENCE, SEQUENCE_OF, SERIAL, SERIALIZABLE, SETEQ, SETNEQ, "
			+ "SET_OF, SHARED, SHORT, SIGNAL, SIMILAR, SQLEXCEPTION, SQLWARNING, START, "
			+ "TATISTICS, STDDEV, STRING, STRUCTURE, SUBCLASS, SUBSET, SUBSETEQ, "
			+ "SUPERCLASS, SUPERSET, SUPERSETEQ, SYS_DATE, SYS_TIME, SYS_TIMESTAMP, "
			+ "SYS_USER, TEST, THERE, TO_CHAR, TO_DATE, TO_NUMBER, TO_TIME, TO_TIMESTAMP, "
			+ "TRIGGER, TYPE, UNDER, USE, UTIME, VARIABLE, VARIANCE, VCLASS, VIRTUAL, "
			+ "VISIBLE, WAIT, WHILE, WITHOUT, SYS_DATETIME, TO_DATETIME";
    }

    public synchronized String getNumericFunctions() throws SQLException
    {
	return "AVG, COUNT, MAX, MIN, STDDEV, SUM, VARIANCE";
    }

    public synchronized String getStringFunctions() throws SQLException
    {
	return "BIT_LENGTH, CHAR_LENGTH, LOWER, LTRIM, OCTET_LENGTH, POSITION, REPLACE, "
			+ "RPAD, RTRIM, SUBSTRING, TRANSLATE, TRIM, TO_CHAR, TO_DATE, TO_NUMBER, "
			+ "TO_TIME, TO_TIMESTAMP, TO_DATETIME, UPPER";
    }

    public synchronized String getSystemFunctions() throws SQLException
    {
	return "";
    }

    public synchronized String getTimeDateFunctions() throws SQLException
    {
	return "ADD_MONTHS, LAST_DAY, MONTH_BETWEEN, SYS_DATE, SYS_TIME, SYS_TIMESTMAP, TO_DATE, TO_TIME, TO_TIMESTAMP, TO_DATETIME";
    }

    public synchronized String getSearchStringEscape() throws SQLException
    {
	return "";
    }

    public synchronized String getExtraNameCharacters() throws SQLException
    {
	return "%#";
    }

    public synchronized boolean supportsAlterTableWithAddColumn() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsAlterTableWithDropColumn() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsColumnAliasing() throws SQLException
    {
	return false;
    }

    public synchronized boolean nullPlusNonNullIsNull() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsConvert() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsConvert(int fromType, int toType) throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsTableCorrelationNames() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsExpressionsInOrderBy() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsOrderByUnrelated() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsGroupBy() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsGroupByUnrelated() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsGroupByBeyondSelect() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsLikeEscapeClause() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsMultipleResultSets() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsMultipleTransactions() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsNonNullableColumns() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsMinimumSQLGrammar() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsCoreSQLGrammar() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsExtendedSQLGrammar() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsANSI92IntermediateSQL() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsANSI92FullSQL() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsOuterJoins() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsFullOuterJoins() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsLimitedOuterJoins() throws SQLException
    {
	return false;
    }

    public synchronized String getSchemaTerm() throws SQLException
    {
	return "";
    }

    public synchronized String getProcedureTerm() throws SQLException
    {
	return "";
    }

    public synchronized String getCatalogTerm() throws SQLException
    {
	return "";
    }

    public synchronized boolean isCatalogAtStart() throws SQLException
    {
	return true;
    }

    public synchronized String getCatalogSeparator() throws SQLException
    {
	return "";
    }

    public synchronized boolean supportsSchemasInDataManipulation() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsSchemasInProcedureCalls() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsSchemasInTableDefinitions() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsCatalogsInDataManipulation() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsPositionedDelete() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsPositionedUpdate() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsSelectForUpdate() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsStoredProcedures() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsSubqueriesInComparisons() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsSubqueriesInExists() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsSubqueriesInIns() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsCorrelatedSubqueries() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsUnion() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsUnionAll() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
	return false;
    }

    public synchronized int getMaxBinaryLiteralLength() throws SQLException
    {
	return (SQL_MAX_CHAR_LITERAL_LEN / 8);
    }

    public synchronized int getMaxCharLiteralLength() throws SQLException
    {
	return SQL_MAX_CHAR_LITERAL_LEN;
    }

    public synchronized int getMaxColumnNameLength() throws SQLException
    {
	return 254;
    }

    public synchronized int getMaxColumnsInGroupBy() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxColumnsInIndex() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxColumnsInOrderBy() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxColumnsInSelect() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxColumnsInTable() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxConnections() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxCursorNameLength() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxIndexLength() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxSchemaNameLength() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxProcedureNameLength() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxCatalogNameLength() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxRowSize() throws SQLException
    {
	return 0;
    }

    public synchronized boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {
	return false;
    }

    public synchronized int getMaxStatementLength() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxStatements() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxTableNameLength() throws SQLException
    {
	return 254;
    }

    public synchronized int getMaxTablesInSelect() throws SQLException
    {
	return 0;
    }

    public synchronized int getMaxUserNameLength() throws SQLException
    {
	return 31;
    }

    public synchronized int getDefaultTransactionIsolation() throws SQLException
    {
	return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    public synchronized boolean supportsTransactions() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsTransactionIsolationLevel(int level) throws SQLException
    {
	switch (level)
	{
	case Connection.TRANSACTION_READ_UNCOMMITTED:
	    return true;
	case Connection.TRANSACTION_READ_COMMITTED:
	case Connection.TRANSACTION_REPEATABLE_READ:
	case Connection.TRANSACTION_SERIALIZABLE:
	default:
	    return false;
	}
    }

    public synchronized boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsDataManipulationTransactionsOnly() throws SQLException
    {
	return true;
    }

    public synchronized boolean dataDefinitionCausesTransactionCommit() throws SQLException
    {
	return false;
    }

    public synchronized boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
	return false;
    }

    public synchronized ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
		    throws SQLException
    {

	String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS",
			"PROCEDURE_TYPE" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT };
	boolean[] nullable = { true, true, false, true, true, true, false, false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getProcedureColumns(String catalog, String schemaPattern,
		    String procedureNamePattern, String columnNamePattern) throws SQLException
    {
	String[] names = { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE",
			"DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_INT,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, false, false, false, false, false, false, false, false, false, false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern,
		    String[] types) throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS" };
	int[] type = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, false, false };

	HashMap<Integer, String> searchTypes = new HashMap<Integer, String>();
	if (types == null) {
	    searchTypes.put(Integer.valueOf(Protocol.SCH_TABLE_TYPE_TABLE), "TABLE");
	    searchTypes.put(Integer.valueOf(Protocol.SCH_TABLE_TYPE_VIEW), "VIEW");
	    searchTypes.put(Integer.valueOf(Protocol.SCH_TABLE_TYPE_SYSTEM), "SYSTEM TABLE");
	}
	else {
	    for (int i = 0; i < types.length; i++) {
		if (types[i].equalsIgnoreCase("TABLE")) {
		    searchTypes.put(Integer.valueOf(Protocol.SCH_TABLE_TYPE_TABLE), "TABLE");
		}
		if (types[i].equalsIgnoreCase("VIEW")) {
		    searchTypes.put(Integer.valueOf(Protocol.SCH_TABLE_TYPE_VIEW), "VIEW");
		}
		if (types[i].equalsIgnoreCase("SYSTEM TABLE")) {
		    searchTypes.put(Integer.valueOf(Protocol.SCH_TABLE_TYPE_SYSTEM), "SYSTEM TABLE");
		}

	    }
	}

	ArrayList<Object[]> resultList = schemaInfoRequestInternal(Protocol.SCH_TABLE, tableNamePattern, null);

	int[] precision = new int[5];
	precision[0] = 0; /* TABLE_CAT */
	precision[1] = 0; /* TABLE_SCHEM */
	precision[2] = 255; /* TABLE_NAME */
	precision[3] = 12; /* TABLE_TYPE */
	precision[4] = 0; /* REMARKS */

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();
	Object[] value = new Object[5];
	value[0] = null;
	value[1] = null;
	value[4] = null;

	for (Object[] tuple : resultList) {
	    int tableTpye = getInt(tuple, Protocol.SCH_TABLE_IDX_TYPE);
	    String tableName = getString(tuple, Protocol.SCH_TABLE_IDX_NAME);

	    value[2] = tableName;
	    value[3] = searchTypes.get(Integer.valueOf(tableTpye));
	    if (value[3] == null) {
		continue;
	    }

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(type, names, nullable, precision, tupleList, tablesCompareKey, jciCon);
    }

    public synchronized ResultSet getSchemas() throws SQLException
    {
	String[] names = { "TABLE_SCHEM" };
	int[] types = { RyeType.TYPE_VARCHAR };
	boolean[] nullable = { false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getCatalogs() throws SQLException
    {
	String[] names = { "TABLE_CAT" };
	int[] types = { RyeType.TYPE_VARCHAR };
	boolean[] nullable = { false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getTableTypes() throws SQLException
    {
	String[] names = { "TABLE_TYPE" };
	int[] types = { RyeType.TYPE_VARCHAR };
	boolean[] nullable = { false };

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	Object[] value = new Object[1];
	value[0] = "SYSTEM TABLE";
	tupleList.add(new JciResultTuple(value, true));
	value[0] = "TABLE";
	tupleList.add(new JciResultTuple(value, true));
	value[0] = "VIEW";
	tupleList.add(new JciResultTuple(value, true));

	return new RyeResultSetWithoutQuery(types, names, nullable, null, tupleList, null, jciCon);
    }

    public synchronized ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern,
		    String columnNamePattern) throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
			"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
			"COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
			"IS_NULLABLE" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_NULL, RyeType.TYPE_INT,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, false, false, false, false, true, false, false, false, false, true,
			true, true, false, false, false };

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	ArrayList<Object[]> resultList = schemaInfoRequestInternal(Protocol.SCH_COLUMN, tableNamePattern,
			columnNamePattern);

	Object[] value = new Object[18];

	value[0] = null;
	value[1] = null;
	value[7] = null;
	value[9] = new Integer(10);
	value[11] = null;
	value[13] = null;
	value[14] = null;

	for (Object[] tuple : resultList) {
	    // type-independent decisions
	    value[2] = getString(tuple, Protocol.SCH_COLUMN_IDX_TABLE_NAME);
	    value[3] = getString(tuple, Protocol.SCH_COLUMN_IDX_NAME);
	    value[6] = value[15] = new Integer(getInt(tuple, Protocol.SCH_COLUMN_IDX_PRECISION));
	    value[8] = new Integer(getInt(tuple, Protocol.SCH_COLUMN_IDX_SCALE));
	    value[16] = new Integer(getInt(tuple, Protocol.SCH_COLUMN_IDX_ORDER));
	    if (getInt(tuple, Protocol.SCH_COLUMN_IDX_NON_NULL) == 1) {
		value[10] = new Integer(columnNoNulls);
		value[17] = "NO";
	    }
	    else {
		value[10] = new Integer(columnNullable);
		value[17] = "YES";
	    }
	    value[12] = getObject(tuple, Protocol.SCH_COLUMN_IDX_DEFAULT);

	    // type-dependent decisions
	    int type = getInt(tuple, Protocol.SCH_COLUMN_IDX_TYPE);
	    if (type == RyeType.TYPE_BINARY) {
		value[4] = new Integer(java.sql.Types.VARBINARY);
		value[5] = "BIT VARYING";
	    }
	    else if (type == RyeType.TYPE_VARCHAR) {
		value[4] = new Integer(java.sql.Types.VARCHAR);
		value[5] = "VARCHAR";
	    }
	    else if (type == RyeType.TYPE_BIGINT) {
		value[4] = new Integer(java.sql.Types.BIGINT);
		value[5] = "BIGINT";
	    }
	    else if (type == RyeType.TYPE_INT) {
		value[4] = new Integer(java.sql.Types.INTEGER);
		value[5] = "INTEGER";
	    }
	    else if (type == RyeType.TYPE_NUMERIC) {
		value[4] = new Integer(java.sql.Types.NUMERIC);
		value[5] = "NUMERIC";
	    }
	    else if (type == RyeType.TYPE_DOUBLE) {
		value[4] = new Integer(java.sql.Types.DOUBLE);
		value[5] = "DOUBLE PRECISION";
	    }
	    else if (type == RyeType.TYPE_TIME) {
		value[4] = new Integer(java.sql.Types.TIME);
		value[5] = "TIME";
	    }
	    else if (type == RyeType.TYPE_DATE) {
		value[4] = new Integer(java.sql.Types.DATE);
		value[5] = "DATE";
	    }
	    else if (type == RyeType.TYPE_DATETIME) {
		value[4] = new Integer(java.sql.Types.TIMESTAMP);
		value[5] = "DATETIME";
	    }

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(types, names, nullable, null, tupleList, columnsCompareKey, jciCon);
    }

    public synchronized ResultSet getColumnPrivileges(String catalog, String schema, String table,
		    String columnNamePattern) throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
			"IS_GRANTABLE" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, false, true, false, false, false };

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	ArrayList<Object[]> resultList = schemaInfoRequestInternal(Protocol.SCH_COLUMN_PRIVILEGE, table,
			columnNamePattern);

	Object[] value = new Object[8];

	value[0] = null;
	value[1] = null;
	value[2] = table;

	for (Object[] tuple : resultList) {
	    value[3] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_NAME);
	    value[4] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_GRANTOR);
	    value[5] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_GRANTEE);
	    value[6] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_PRIVILEGE);
	    value[7] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_GRANTABLE);

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(types, names, nullable, null, tupleList, columnPrivilegesCompareKey, jciCon);
    }

    public synchronized ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
		    throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, true, false, false, false };

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	ArrayList<Object[]> resultList = schemaInfoRequestInternal(Protocol.SCH_TABLE_PRIVILEGE, tableNamePattern, null);

	Object[] value = new Object[7];

	value[0] = null;
	value[1] = null;

	for (Object[] tuple : resultList) {
	    value[2] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_NAME);
	    value[3] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_GRANTOR);
	    value[4] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_GRANTEE);
	    value[5] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_PRIVILEGE);
	    value[6] = getString(tuple, Protocol.SCH_PRIVILEGE_IDX_GRANTABLE);

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(types, names, nullable, null, tupleList, tablePrivilegesCompareKey, jciCon);
    }

    public synchronized ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope,
		    boolean nullable) throws SQLException
    {
	String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
			"DECIMAL_DIGITS", "PSEUDO_COLUMN" };
	int[] types = { RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT };
	boolean[] Nullable = { false, false, false, false, false, true, false, false };

	return new RyeResultSetWithoutQuery(types, names, Nullable, jciCon);
    }

    public synchronized ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException
    {
	String[] names = { "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
			"DECIMAL_DIGITS", "PSEUDO_COLUMN" };
	int[] types = { RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT };
	boolean[] nullable = { true, false, false, false, false, false, false, false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME" };

	int[] type = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_VARCHAR };

	boolean[] nullable = { true, true, false, false, false, false };

	ArrayList<Object[]> resultList = schemaInfoRequestInternal(Protocol.SCH_PRIMARY_KEY, table, null);

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	Object[] value = new Object[6];

	value[0] = null;
	value[1] = null;

	for (Object[] tuple : resultList) {
	    value[2] = getString(tuple, Protocol.SCH_PK_IDX_TABLE_NAME);
	    value[3] = getString(tuple, Protocol.SCH_PK_IDX_COLUMN_NAME);
	    value[4] = getInt(tuple, Protocol.SCH_PK_IDX_KEY_SEQ);
	    value[5] = getString(tuple, Protocol.SCH_PK_IDX_KEY_NAME);

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(type, names, nullable, null, tupleList, primaryKeyCompareKey, jciCon);
    }

    private synchronized ResultSet getForeignKeys() throws SQLException
    {
	String[] names = { "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
			"FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
			"FK_NAME", "PK_NAME", "DEFERRABILITY" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_INT };
	boolean[] nullable = { true, true, false, false, true, true, false, false, false, false, false, true, true,
			false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException
    {
	return getForeignKeys();
    }

    public synchronized ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException
    {
	return getForeignKeys();
    }

    public synchronized ResultSet getCrossReference(String primaryCatalog, String primarySchema, String primaryTable,
		    String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
    {
	return getForeignKeys();
    }

    public synchronized ResultSet getTypeInfo() throws SQLException
    {
	String[] names = { "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
			"NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
			"AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
			"SQL_DATETIME_SUB", "NUM_PREC_RADIX" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_INT,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT };
	boolean[] nullable = { false, false, false, true, true, true, false, false, false, false, false, false, true,
			false, false, true, true, false };

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	/* Type Name */
	Object[] column1 = { "NUMERIC", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "VARCHAR", "DATE", "TIME", "DATETIME" };
	/* Data Type */
	Object[] column2 = { new Integer(java.sql.Types.NUMERIC), new Integer(java.sql.Types.INTEGER),
			new Integer(java.sql.Types.BIGINT), new Integer(java.sql.Types.FLOAT),
			new Integer(java.sql.Types.DOUBLE), new Integer(java.sql.Types.VARCHAR),
			new Integer(java.sql.Types.DATE), new Integer(java.sql.Types.TIME),
			new Integer(java.sql.Types.TIMESTAMP) };

	/* Precision */
	Object[] column3 = { new Integer(38), new Integer(10), new Integer(20), new Integer(15), new Integer(15),
			new Integer(1073741823), new Integer(10), new Integer(10), new Integer(26) };
	/* Literal prefix */
	Object[] column4 = { null, null, null, null, null, "'", "DATE'", "TIME'", "DATETIME'" };
	/* Literal Suffix */
	Object[] column5 = { null, null, null, null, null, "'", "'", "'", "'" };

	/* column6 : Create Params */

	/* Nullable */
	Object column7 = new Integer(typeNullable);

	/* case sensitive */
	Object column8 = new Boolean(false);

	/* column9 : SEARCHABLE */

	/* Unsigned attribute */
	Object column10 = new Boolean(false);

	/* FIXED_PREC_SCALE */
	Object column11 = new Boolean(false);

	/* AUTO_INCREMENT */
	Object column12 = new Boolean(false);

	/* LOCAL_TYPE_NAME */
	Object[] column13 = column1;

	/* MINIMUM_SCALE */
	Object column14 = new Integer(0);

	/* column15 : MAXIMUM_SCALE */

	/* SQL_DATA_TYPE */
	Object column16 = null;
	/* SQL_DATETIME_SUB */
	Object column17 = column16;
	/* NUM_PREC_RADIX */
	Object column18 = new Integer(10);

	Object[] value = new Object[18];

	for (int i = 0; i < column1.length; i++) {
	    value[0] = column1[i];
	    value[1] = column2[i];
	    value[2] = column3[i];
	    value[3] = column4[i];
	    value[4] = column5[i];

	    /* column6 : Create Params */
	    value[5] = null;

	    value[6] = column7;
	    value[7] = column8;

	    /* column9 : SEARCHABLE */
	    if (((String) column1[i]).equals("VARCHAR")) {
		value[8] = new Integer(typeSearchable);
	    }
	    else {
		value[8] = new Integer(typePredBasic);
	    }

	    value[9] = column10;
	    value[10] = column11;
	    value[11] = column12;
	    value[12] = column13[i];
	    value[13] = column14;

	    /* column15 : MAXIMUM_SCALE */
	    if (((String) column1[i]).equals("NUMERIC")) {
		value[14] = new Integer(38);
	    }
	    else {
		value[14] = new Integer(0);
	    }

	    value[15] = column16;
	    value[16] = column17;
	    value[17] = column18;

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(types, names, nullable, null, tupleList, null, jciCon);
    }

    public synchronized ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique,
		    boolean approximate) throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
			"TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES",
			"FILTER_CONDITION" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_INT,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_BIGINT, RyeType.TYPE_INT,
			RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, false, true, false, false, false, false, false, false, false, true };

	ArrayList<JciResultTuple> tupleList = new ArrayList<JciResultTuple>();

	ArrayList<Object[]> resultList = schemaInfoRequestInternal(Protocol.SCH_INDEX_INFO, table, null);

	Object[] tuple;

	Object[] value = new Object[13];
	value[2] = table;

	for (int i = 0; i < resultList.size(); i++) {
	    tuple = resultList.get(i);

	    boolean isUniqueIndex = getBoolean(tuple, Protocol.SCH_INDEX_INFO_IDX_IS_UNIQUE);

	    if (unique && isUniqueIndex == false)
		continue;

	    value[3] = new Boolean(!isUniqueIndex);
	    value[5] = getString(tuple, Protocol.SCH_INDEX_INFO_IDX_NAME);
	    value[6] = new Short((short) getInt(tuple, Protocol.SCH_INDEX_INFO_IDX_TYPE));
	    value[7] = new Short((short) getInt(tuple, Protocol.SCH_INDEX_INFO_IDX_KEY_ORDER));
	    value[8] = getString(tuple, Protocol.SCH_INDEX_INFO_IDX_COLUMN_NAME);
	    value[9] = getString(tuple, Protocol.SCH_INDEX_INFO_IDX_ASC_DESC);
	    value[10] = new Long(getLong(tuple, Protocol.SCH_INDEX_INFO_IDX_NUM_KEYS));
	    value[11] = new Integer(getInt(tuple, Protocol.SCH_INDEX_INFO_IDX_NUM_PAGES));

	    tupleList.add(new JciResultTuple(value, true));
	}

	return new RyeResultSetWithoutQuery(types, names, nullable, null, tupleList, indexInfoCompareKey, jciCon);
    }

    public synchronized boolean supportsResultSetType(int type) throws SQLException
    {
	if (type == ResultSet.TYPE_FORWARD_ONLY)
	    return true;
	if (type == ResultSet.TYPE_SCROLL_INSENSITIVE)
	    return true;
	if (type == ResultSet.TYPE_SCROLL_SENSITIVE)
	    return true;
	return false;
    }

    public synchronized boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException
    {
	if (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY)
	    return true;
	if (type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_UPDATABLE)
	    return true;
	if (type == ResultSet.TYPE_SCROLL_INSENSITIVE && concurrency == ResultSet.CONCUR_READ_ONLY)
	    return true;
	if (type == ResultSet.TYPE_SCROLL_INSENSITIVE && concurrency == ResultSet.CONCUR_UPDATABLE)
	    return true;
	if (type == ResultSet.TYPE_SCROLL_SENSITIVE && concurrency == ResultSet.CONCUR_READ_ONLY)
	    return true;
	if (type == ResultSet.TYPE_SCROLL_SENSITIVE && concurrency == ResultSet.CONCUR_UPDATABLE)
	    return true;
	return false;
    }

    public synchronized boolean ownUpdatesAreVisible(int type) throws SQLException
    {
	if (type == ResultSet.TYPE_SCROLL_SENSITIVE)
	    return true;
	return false;
    }

    public synchronized boolean ownDeletesAreVisible(int type) throws SQLException
    {
	return false;
    }

    public synchronized boolean ownInsertsAreVisible(int type) throws SQLException
    {
	return false;
    }

    public synchronized boolean othersUpdatesAreVisible(int type) throws SQLException
    {
	if (type == ResultSet.TYPE_SCROLL_SENSITIVE)
	    return true;
	return false;
    }

    public synchronized boolean othersDeletesAreVisible(int type) throws SQLException
    {
	return false;
    }

    public synchronized boolean othersInsertsAreVisible(int type) throws SQLException
    {
	return false;
    }

    public synchronized boolean updatesAreDetected(int type) throws SQLException
    {
	return false;
    }

    public synchronized boolean deletesAreDetected(int type) throws SQLException
    {
	if (type == ResultSet.TYPE_SCROLL_SENSITIVE)
	    return true;
	return false;
    }

    public synchronized boolean insertsAreDetected(int type) throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsBatchUpdates() throws SQLException
    {
	return true;
    }

    public synchronized ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
		    throws SQLException
    {
	throw new UnsupportedOperationException();
    }

    public synchronized Connection getConnection() throws SQLException
    {
	return con;
    }

    public synchronized ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
		    String attributeNamePattern) throws SQLException
    {
	String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME",
			"ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF",
			"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
			"SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT,
			RyeType.TYPE_INT, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT,
			RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_INT, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_INT };
	boolean[] nullable = { true, true, false, false, false, false, false, false, false, false, true, true, false,
			false, false, false, false, false, false, false, false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized int getDatabaseMajorVersion() throws SQLException
    {
	if (this.major_version == -1) {
	    getDatabaseProductVersion();
	}
	return this.major_version;
    }

    public synchronized int getDatabaseMinorVersion() throws SQLException
    {
	if (this.minor_version == -1) {
	    getDatabaseProductVersion();
	}
	return this.minor_version;
    }

    public synchronized int getJDBCMajorVersion() throws SQLException
    {
	return 3;
    }

    public synchronized int getJDBCMinorVersion() throws SQLException
    {
	return 0;
    }

    public synchronized int getResultSetHoldability() throws SQLException
    {
	return con.getHoldability();
    }

    public synchronized int getSQLStateType() throws SQLException
    {
	return DatabaseMetaData.sqlStateSQL99;
    }

    public synchronized boolean locatorsUpdateCopy() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsGetGeneratedKeys() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsMultipleOpenResults() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsNamedParameters() throws SQLException
    {
	return false;
    }

    public synchronized boolean supportsResultSetHoldability(int holdability) throws SQLException
    {
	if (holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT || holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT)
	    return true;
	return false;
    }

    public synchronized boolean supportsSavepoints() throws SQLException
    {
	return true;
    }

    public synchronized boolean supportsStatementPooling() throws SQLException
    {
	return false;
    }

    public synchronized ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
		    throws SQLException
    {
	String[] names = { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public synchronized ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
		    throws SQLException
    {
	String[] names = { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME" };
	int[] types = { RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR,
			RyeType.TYPE_VARCHAR, RyeType.TYPE_VARCHAR };
	boolean[] nullable = { true, true, false, true, true, false };

	return new RyeResultSetWithoutQuery(types, names, nullable, jciCon);
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public ResultSet getClientInfoProperties() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
		    String columnNamePattern) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
		    String columnNamePattern) throws SQLException
    {
	throw new java.lang.UnsupportedOperationException();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException
    {
	return false;
    }

    private Object getObject(Object[] tuple, int index)
    {
	if (index < 0 || index >= tuple.length) {
	    return null;
	}
	else {
	    return tuple[index];
	}
    }

    private String getString(Object[] tuple, int index) throws SQLException
    {
	return RyeType.getString(getObject(tuple, index), jciCon);
    }

    private int getInt(Object[] tuple, int index) throws SQLException
    {
	return RyeType.getInt(getObject(tuple, index), jciCon);
    }

    private long getLong(Object[] tuple, int index) throws SQLException
    {
	return RyeType.getLong(getObject(tuple, index), jciCon);
    }

    private boolean getBoolean(Object[] tuple, int index) throws SQLException
    {
	return RyeType.getBoolean(getObject(tuple, index), jciCon);
    }

    protected synchronized void autoCommit()
    {
	try {
	    con.autoCommit();
	} catch (SQLException e) {
	}
    }

    protected synchronized void autoRollback()
    {
	try {
	    con.autoRollback();
	} catch (SQLException e) {
	    e.printStackTrace();
	}
    }

    private ArrayList<Object[]> schemaInfoRequestInternal(int command, String arg1, String arg2) throws SQLException
    {
	JciStatement us = null;

	try {
	    us = jciCon.schemaInfoRequest(command, arg1, arg2);
	    int numColumns = us.getQueryInfo().getColumnCount();

	    ArrayList<Object[]> result = new ArrayList<Object[]>();

	    while (us.cursorNext()) {
		Object[] tuple = new Object[numColumns];
		for (int i = 0; i < numColumns; i++) {
		    tuple[i] = us.getObject(i);
		}
		result.add(tuple);
	    }

	    us.close();
	    us = null;

	    autoCommit();

	    return result;
	} catch (SQLException e) {
	    if (us != null) {
		us.close();
	    }
	    autoRollback();
	    throw e;
	}
    }
}
