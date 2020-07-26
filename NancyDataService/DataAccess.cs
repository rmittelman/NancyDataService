using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data.OleDb;
using MySql.Data.MySqlClient;
using System.Linq;

namespace NancyDataService
{
    #region enumerations
    public enum FilterCombiners
    {
        None,
        And,
        Or
    }

    public enum FilterTypes
    {
        less,
        lessorequal,
        equal,
        notequal,
        greaterorequal,
        greater,
        contains,
        notcontains,
        starts,
        ends
    }
    public enum DataSourceTypes
    {
        table,
        procedure,
        sql
    }
    public enum DbFlavors
    {
        Access,
        Sql,
        MySql
    }
    #endregion

    public static class DataAccess
    {

        #region variables
        public const string opers = "less, lessorequal, equal, notequal, greaterorequal, greater, contains, notcontains, starts, ends";
        public static Dictionary<string, DataTable> savedData = new Dictionary<string, DataTable>();

        private static string connString;
        private static string dbName;
        private static string json;

        #endregion

        #region properties

        public static string LastError { get; private set; }
        public static string Status { get; private set; }
        #endregion

        #region public methods

        /// <summary>
        /// This overload gets data from a table or stored procedure.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: sql/procedure.</param>
        /// <param name="dataSourceName">Name of table / stored procedure to select data from.</param>
        /// <param name="dbColumns">The data columns to return. Ex: ID, Name,... If empty, return all columns</param>
        /// <param name="filters">A list of <see cref="Filter"/> objects used to narrow selection.</param>
        /// <param name="orderBy">Fields to order records by. Ex: Col1, Col3 Desc.</param>
        /// <returns></returns>
        public static DataTable GetData(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName, string dbColumns, List<Filter> filters, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                json = SerializeDictionary(new Dictionary<string, string> { { "filters", Get_Filter_String(filters) } });
                json = InsertJsonProperties(json, Formatting.None, "status", "testing", "method", "GetData", "data source type", dataSourceType.ToString());
                LastError = json;
                return null;
            }

            connString = ConfigurationManager.AppSettings[$"{dbFlavor.ToString()}Connection"];
            dbName = Get_String_Segment(connString, "database");

            switch (dbFlavor)
            {
                case DbFlavors.Access:
                    return Select_Data_Access(clientIp, dbFlavor, dataSourceType, dataSourceName, dbColumns, filters, orderBy);
                case DbFlavors.MySql:
                    return Select_Data_MySql(clientIp, dbFlavor, dataSourceType, dataSourceName, dbColumns, filters, orderBy);
                case DbFlavors.Sql:
                    return Select_Data_Sql(clientIp, dbFlavor, dataSourceType, dataSourceName, dbColumns, filters, orderBy);
                default:
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "GetData" }, { "reason", "dbFlavor missing. Should be Access, Sql or MySql" } });
                    return null;
            }
        }

        /// <summary>
        /// This overload gets data by using a SQL Select statement.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourcetype">A <see cref="DataSourceTypes"/> enumeration member. Ex: sql/procedure.</param>
        /// <param name="sql">The SQL Select Statement.</param>
        /// <param name="filters">A list of <see cref="Filter"/> objects used to narrow selection.</param>
        /// <returns></returns>
        public static DataTable GetData(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourcetype, string sql, List<Filter> filters)
        {
            bool isTest = false;
            if (isTest)
            {
                json = SerializeDictionary(new Dictionary<string, string> { { "filters", Get_Filter_String(filters) } });
                json = InsertJsonProperties(json, Formatting.None, "status", "testing", "method", "GetData", "sql", sql);
                LastError = json;
                return null;
            }

            connString = ConfigurationManager.AppSettings[$"{dbFlavor.ToString()}Connection"];
            dbName = Get_String_Segment(connString, "database");

            switch (dbFlavor)
            {
                case DbFlavors.Access:
                    return Select_Data_Access(clientIp, dbFlavor, dataSourcetype, sql, filters);
                case DbFlavors.MySql:
                    return Select_Data_MySql(clientIp, dbFlavor, dataSourcetype, sql, filters);
                case DbFlavors.Sql:
                    return Select_Data_Sql(clientIp, dbFlavor, dataSourcetype, sql, filters);
                default:
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "GetData" }, { "reason", "dbFlavor missing. Should be Access, Sql or MySql" } });
                    return null;
            }
        }

        /// <summary>
        /// Saves DataTable changes and returns new current DataTable
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dtUpdated">A 1-record <see cref="DataTable"/> containing changes made.</param>
        /// <param name="dtOriginal">A 1-record <see cref="DataTable"/> containing original record before changes made.</param>
        /// <param name="orderBy">Fields to order records by. Ex: Col1, Col3 Desc.</param>
        /// <returns>A 1-record <see cref="DataTable"/> containing current record after changes made.</returns>
        public static DataTable UpdateData(string clientIp, DbFlavors dbFlavor, DataTable dtUpdated, DataTable dtOriginal, string orderBy)
        {
            connString = ConfigurationManager.AppSettings[$"{dbFlavor.ToString()}Connection"];
            dbName = Get_String_Segment(connString, "database");
            List<string> colsAndVals = Get_Update_Field_List(dbFlavor, dtUpdated, dtOriginal);

            bool isTest = false;
            if (isTest)
            {
                json = SerializeDictionary(new Dictionary<string, string> { { "columns", string.Join(", ", colsAndVals.ToArray()) } });
                json = InsertJsonProperties(json, Formatting.None, "status", "testing", "method", "UpdateData");
                LastError = json;
                return null;
            }

            #region verify any columns were changed
            if (colsAndVals.Count == 0)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                        { { "status", "error" }, { "method", "UpdateData" }, { "Error: ", "no data was changed" } });
                return null;
            }
            #endregion

            switch (dbFlavor)
            {
                case DbFlavors.Access:
                    return Update_Data_Access(clientIp, dbFlavor, dtUpdated, colsAndVals, orderBy);
                case DbFlavors.MySql:
                //return Select_Data_MySql(dbFlavor, dataSourceType, dataSourceName, filters, orderBy);
                case DbFlavors.Sql:
                //return Select_Data_Sql(dbFlavor, dataSourceType, dataSourceName, filters, orderBy);
                default:
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Update_Data" }, { "reason", "dbFlavor missing. Should be Access, Sql or MySql" } });
                    return null;
            }
        }

        /// <summary>
        /// Insert record into database
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="table">A 1-record <see cref="DataTable"/> containing the record to be inserted.</param>
        /// <returns>ID of new record, or -1 if not auto-number ID, or 0 if insert fails.</returns>
        public static Int64 InsertData(DbFlavors dbFlavor, DataTable table)
        {
            connString = ConfigurationManager.AppSettings[$"{dbFlavor.ToString()}Connection"];
            dbName = Get_String_Segment(connString, "database");

            switch (dbFlavor)
            {
                case DbFlavors.Access:
                    return Insert_Data_Access(dbFlavor, table);
                case DbFlavors.Sql:
                //return Insert_Data_MySql(dbFlavor, table);
                case DbFlavors.MySql:
                //return Insert_Data_Sql(dbFlavor, table);
                default:
                    break;
            }
            return 0;
        }

        /// <summary>
        /// Convert a list of dictionary items to valid Json.
        /// </summary>
        /// <param name="values">A dictionary of <see cref="KeyValuePair{TKey, TValue}"/> containing Json item and value.</param>
        /// <param name="formatting">A Newtonsoft.Json.Formatting member. Ex: Formatting.None / Formatting.Indented.</param>
        /// <returns></returns>
        public static string SerializeDictionary(Dictionary<string, string> values, Formatting formatting = Formatting.None)
        {
            return JsonConvert.SerializeObject(values, formatting);
        }

        /// <summary>
        /// Insert KeyValuePairs of string,Object at beginning of Json string.</string>
        /// </summary>
        /// <param name="jsonText">Json text to add properties to.</param>
        /// <param name="formatting">A Newtonsoft.Json.Formatting member. Ex: Formatting.None / Formatting.Indented.</param>
        /// <param name="propNamesAndVals">Object parameter array of Name, Value, Name, Value... to add.</param>
        /// <remarks>Parameter array must be even number of segments.</remarks>
        /// <returns></returns>
        public static string InsertJsonProperties(string jsonText, Formatting formatting, params object[] propNamesAndVals)
        {
            if (propNamesAndVals.Length % 2 != 0)
                return string.Empty;

            var jObj = JObject.Parse(jsonText);
            for (int i = propNamesAndVals.GetUpperBound(0); i >= 0; i -= 2)
            {
                jObj.AddFirst(new JProperty((string)propNamesAndVals[i - 1], propNamesAndVals[i]));
            }
            return jObj.ToString(formatting);
        }

        #endregion

        #region private business methods

        /// <summary>
        /// Return a list of items where columns/parameters don't match filters
        /// </summary>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="itemNames">A list of column or parameter names.</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for parameters / where clause.</param>
        /// <returns></returns>
        /// <remarks>
        /// Verify that each filter has a corresponding column/parameter.
        /// Verify each item in list of columns/parameters have a corresponding filter.
        /// - SQL queries & stored procedures need a 1-to-1 relationship between columns/parameters and filters.
        /// - Tables can have more columns than filters supplied.
        /// </remarks>
        private static List<string> Cross_Ref_Filters_And_List(DataSourceTypes dataSourceType, List<string> itemNames, List<Filter> filters)
        {
            var exceptionList = new List<string>();

            // identify items in filters but not in list of columns/parameters
            filters.ForEach(f =>
            {
                if (!itemNames.Any(i => f.Field == i))
                {
                    string source = dataSourceType == DataSourceTypes.procedure ? "parameter" : "column";
                    exceptionList.Add($"No {source} is present for filter '{f.Field}'");
                }
            });

            // identify items in list of columns/parameters but not in filters (except for tables)
            if (dataSourceType != DataSourceTypes.table)
            {
                itemNames.ForEach(i =>
                {
                    if (!filters.Any(f => i == f.Field))
                    {
                        string source = dataSourceType == DataSourceTypes.procedure ? "parameter" : "column";
                        exceptionList.Add($"No filter was supplied for {source} '{i}'");
                    }
                });
            }

            return exceptionList;
        }

        /// <summary>
        /// Fix Access table columns AutoIncrement property if needed.
        /// </summary>
        /// <param name="dataTable">A <see cref="DataTable"/> object.</param>
        /// <param name="command">The <see cref="OleDbCommand"/> used to get the table data.</param>
        /// <returns></returns>
        private static bool Fix_Access_AutoIncrement_Columns(DataTable dataTable, OleDbCommand command)
        {
            try
            {
                using (OleDbDataReader dataReader = command.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    using (var schemaTable = dataReader.GetSchemaTable())
                    {
                        foreach (DataRow tableColumn in schemaTable.Rows)
                        {
                            if ((bool)tableColumn["IsAutoIncrement"])
                            {
                                dataTable.Columns[tableColumn["ColumnName"].ToString()].AutoIncrement = true;
                                dataTable.Columns[tableColumn["ColumnName"].ToString()].AutoIncrementSeed = 1;
                                dataTable.Columns[tableColumn["ColumnName"].ToString()].AutoIncrementStep = 1;
                            }
                        }
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", Status }, { "method", "FixAutoIncrementColumns" }, { "Error: ", ex.Message } });
                return false;
            }
        }

        /// <summary>
        /// Fix Access table PrimaryKey property.
        /// </summary>
        /// <param name="dataTable">A <see cref="DataTable"/> object.</param>
        /// <param name="command">The <see cref="OleDbCommand"/> used to get the table data.</param>
        /// <returns></returns>
        private static bool Fix_Access_PrimaryKey_Property(DataTable dataTable, OleDbCommand command)
        {
            try
            {
                using (OleDbDataReader dataReader = command.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    var pkCols = new List<DataColumn>();
                    using (var schemaTable = dataReader.GetSchemaTable())
                    {
                        foreach (DataRow tableColumn in schemaTable.Rows)
                        {
                            if ((bool)tableColumn["IsKey"])
                                pkCols.Add(dataTable.Columns[tableColumn["ColumnName"].ToString()]);
                        }
                        if (pkCols.Count > 0)
                            dataTable.PrimaryKey = pkCols.ToArray();
                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", Status }, { "method", "FixAccessPrimaryKeyProperty" }, { "Error: ", ex.Message } });
                return false;
            }
        }

        /// <summary>
        /// Get a string of filters: field:operator:value, field:operator:value,...
        /// </summary>
        /// <param name="filters">A list of <see cref="Filter"/> objects.</param>
        /// <returns></returns>
        private static string Get_Filter_String(List<Filter> filters)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var filter in filters)
                sb.Append($", {filter.Field}:{filter.Operator}:{filter.Value.ToString()}");
            if (sb.Length > 0)
                sb.Remove(0, 2);
            return sb.ToString();
        }

        /// <summary>
        /// Get a list of Filter objects from <see cref="DataTable"/> properties</see>/>
        /// </summary>
        /// <param name="dbFlavor"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        private static List<Filter> Get_Filters_From_DataTable(DbFlavors dbFlavor, DataTable table)
        {
            var filters = new List<Filter>();
            if (table.PrimaryKey.Length > 0)
            {
                foreach (DataColumn col in table.PrimaryKey)
                {
                    filters.Add(new Filter
                    {
                        Field = col.ColumnName,
                        Operator = FilterTypes.equal,
                        Value = Sqlize_Value(dbFlavor, col.DataType.ToString(), table.Rows[0][col.Ordinal])
                    });
                }
            }
            return filters;
        }

        /// <summary>
        /// Get comma-delimited lists of column names and values for INSERT.
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/MySql/Sql.</param>
        /// <param name="dataTable">A <see cref="DataTable"/> object to insert values from into database table.</param>
        /// <param name="columns">Output. A comma-delimited string of column names.</param>
        /// <param name="values">Output. A comma-delimited string of SQLized column values.</param>
        /// <returns></returns>
        private static bool Get_Insert_Field_List(DbFlavors dbFlavor, DataTable dataTable, out String columns, out String values)
        {
            columns = string.Empty;
            values = string.Empty;
            StringBuilder sbCols = new StringBuilder();
            StringBuilder sbVals = new StringBuilder();
            foreach (DataColumn col in dataTable.Columns)
            {
                if (!col.AutoIncrement)
                {
                    sbCols.Append(", ");
                    sbCols.Append(col.ColumnName);
                    sbVals.Append(", ");
                    sbVals.Append(Sqlize_Value(dbFlavor, col.DataType.ToString(), dataTable.Rows[0][col.Ordinal]));
                }
            }
            if (sbCols.Length > 0 && sbVals.Length > 0)
            {
                sbCols.Remove(0, 2);
                sbVals.Remove(0, 2);
                columns = sbCols.ToString();
                values = sbVals.ToString();
                return true;
            }
            return false;
        }

        /// <summary>
        /// Get a string of parameterName=parameterValue, parameterName=parameterValue,...
        /// </summary>
        /// <param name="cmd">Command object.</param>
        /// <returns></returns>
        private static string Get_Parameter_String(IDbCommand cmd)
        {
            StringBuilder sb = new StringBuilder();
            foreach (IDbDataParameter parm in cmd.Parameters)
                sb.Append($", {parm.ParameterName}={parm.Value}");
            if (sb.Length > 0)
                sb.Remove(0, 2);
            return sb.ToString();
        }

        /// <summary>
        /// Get a list of stored procedure parameters or table columns
        /// </summary>
        /// <param name="conn">An open <see cref="IDbConnection"/> to the database</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex table/procedure</param>
        /// <param name="dataSourceName">Table or procedure name</param>
        /// <returns></returns>
        private static List<string> Get_Parameters_Or_Columns(IDbConnection conn, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName)
        {
            var myList = new List<string>();
            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    string cmdText = string.Empty;

                    if (dbFlavor == DbFlavors.Access)
                    {
                        if (dataSourceType == DataSourceTypes.table)
                            cmdText = $"select * from {dataSourceName} where 1 = 0";
                        else
                            return null;

                        cmd.CommandText = cmdText;
                        cmd.CommandType = CommandType.Text;
                        using (var dt = new DataTable())
                        {
                            dt.Load(cmd.ExecuteReader());
                            foreach (DataColumn col in dt.Columns)
                            {
                                myList.Add(col.ColumnName);
                            }
                        }
                    }
                    else
                    {
                        if (dataSourceType == DataSourceTypes.table)
                            cmdText = $"select column_name from information_schema.columns where table_schema = '{dbName}' and table_name = '{dataSourceName}'";
                        else
                            cmdText = $"select parameter_name from information_schema.parameters where specific_schema = '{dbName}' and specific_name = '{dataSourceName}'";

                        cmd.CommandText = cmdText;
                        cmd.CommandType = CommandType.Text;

                        using (var dt = new DataTable())
                        {
                            dt.Load(cmd.ExecuteReader());
                            foreach (DataRow row in dt.Rows)
                            {
                                myList.Add(row[0].ToString());
                            }
                        }
                    }
                    return myList;
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Get_Parameters_Or_Columns" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        ///// <summary>
        ///// Return procedure parameters / table columns and variable types
        ///// </summary>
        ///// <param name="conn">An open <see cref="IDbConnection"/> to the database</param>
        ///// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql</param>
        ///// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex table/procedure</param>
        ///// <param name="dataSourceName">Table or procedure name</param>
        ///// <returns></returns>
        //private static Dictionary<string, string> Get_Parameters_Or_Columns(IDbConnection conn, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName)
        //{
        //    var myDict = new Dictionary<string, string>();
        //    try
        //    {
        //        using (var cmd = conn.CreateCommand())
        //        {
        //            string cmdText = string.Empty;

        //            if (dbFlavor == DbFlavors.Access)
        //            {
        //                if (dataSourceType == DataSourceTypes.table)
        //                    cmdText = $"select * from {dataSourceName} where 1 = 0";
        //                else
        //                    return null;

        //                cmd.CommandText = cmdText;
        //                cmd.CommandType = CommandType.Text;
        //                using (var dt = new DataTable())
        //                {
        //                    dt.Load(cmd.ExecuteReader());
        //                    foreach (DataColumn col in dt.Columns)
        //                    {
        //                        myDict.Add(col.ColumnName, col.DataType.ToString());
        //                    }
        //                }
        //            }
        //            else
        //            {
        //                if (dataSourceType == DataSourceTypes.table)
        //                    cmdText = $"select column_name, data_type from information_schema.columns where table_schema = '{dbName}' and table_name = '{dataSourceName}'";
        //                else
        //                    cmdText = $"select parameter_name, data_type from information_schema.parameters where specific_schema = '{dbName}' and specific_name = '{dataSourceName}'";

        //                cmd.CommandText = cmdText;
        //                cmd.CommandType = CommandType.Text;

        //                using (var dt = new DataTable())
        //                {
        //                    dt.Load(cmd.ExecuteReader());
        //                    foreach (DataRow row in dt.Rows)
        //                    {
        //                        myDict.Add(row[0].ToString(), row[1].ToString());
        //                    }
        //                }
        //            }
        //            return myDict;
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        LastError = SerializeDictionary(new Dictionary<string, string>
        //                    { { "status", "error" }, { "method", "Get_Parameters_Or_Columns" }, { "Error: ", ex.Message } });
        //        return null;
        //    }
        //}

        /// <summary>
        /// Return a value from a string based on key supplied.
        /// </summary>
        /// <param name="delimitedString">Delimited string to parse. Ex: Item1=one;Item2=two...</param>
        /// <param name="delimiter">Character separating segments in string</param>
        /// <param name="separator">Character separating key and value in string segments</param>
        /// <returns></returns>
        private static string Get_String_Segment(string delimitedString, string keyValue, string delimiter = ";", string separator = "=")
        {
            var items = delimitedString.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
            foreach (var item in items)
            {
                var keyValuePair = item.Split(separator, StringSplitOptions.RemoveEmptyEntries);
                if (keyValuePair[0] == keyValue)
                    return keyValuePair[1];
            }
            return string.Empty;
        }

        /// <summary>
        /// Get a list of column names and values for UPDATE statement.
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/MySql/Sql.</param>
        /// <param name="newTable">A 1-row <see cref="DataTable"/>containing new values.</param>
        /// <param name="oldTable">A 1-row <see cref="DataTable"/>containing original values.</param>
        /// <returns></returns>
        private static List<string> Get_Update_Field_List(DbFlavors dbFlavor, DataTable newTable, DataTable oldTable)
        {
            List<string> colsAndVals = new List<string>();
            foreach (DataColumn col in newTable.Columns)
            {
                bool isTest = false;
                if (isTest)
                    Console.WriteLine($"col={col.ColumnName}, old={(oldTable.Rows[0][col.Ordinal]) ?? "null"}, new={(newTable.Rows[0][col.Ordinal]) ?? "null"}");

                if (!col.AutoIncrement)
                    if (!newTable.Rows[0][col.Ordinal].Equals(oldTable.Rows[0][col.Ordinal]))
                        colsAndVals.Add($"{col.ColumnName} = {Sqlize_Value(dbFlavor, col.DataType.ToString(), newTable.Rows[0][col.Ordinal])}");
            }
            return colsAndVals;
        }

        /// <summary>
        /// Return a list of column names from SQL statement
        /// </summary>
        /// <param name="sqlStatement"></param>
        /// <returns></returns>
        private static List<string> Get_Where_Columns(string sqlStatement)
        {
            var myList = new List<string>();
            if (sqlStatement.Contains("where", StringComparison.CurrentCultureIgnoreCase))
            {
                var whereConditions = sqlStatement.Substring(sqlStatement.IndexOf("where", StringComparison.CurrentCultureIgnoreCase) + 6).ToLower();
                var ar = whereConditions.Split(new string[] { "and", "and ", " and", " and ", "or", "or ", " or", " or " }, StringSplitOptions.None);
                foreach (var fieldExpression in ar)
                {
                    var fieldName = fieldExpression.Split(new string[] { "=", "<", ">", "in", "between" }, StringSplitOptions.None)[0].Trim();
                    myList.Add(fieldName);
                }
            }
            return myList;
        }

        /// <summary>
        /// Convert a column value to a string which works in a SQL statement
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/MySql/Sql.</param>
        /// <param name="dataType">The type of value. Ex: String, Int32, ...</param>
        /// <param name="val">Value to be SQLized.</param>
        /// <returns></returns>
        private static string Sqlize_Value(DbFlavors dbFlavor, String dataType, Object val)
        {
            string myVal;
            switch (dataType)
            {
                case "System.Bool":
                case "System.Byte":
                case "System.SByte":
                case "System.Int16":
                case "System.Int32":
                case "System.Int64":
                case "System.IntPtr":
                case "System.Single":
                case "System.DBNull":
                case "System.Decimal":
                case "System.Double":
                case "System.Char":
                case "System.Guid":
                case "System.UInt16":
                case "System.UInt32":
                case "System.UInt64":
                case "System.UIntPtr":
                    return val.ToString();

                case "System.DateTime":
                case "System.DateTimeOffset":
                    if (dbFlavor == DbFlavors.Access)
                    {
                        myVal = ((DateTime)val).ToString("MM/dd/yyyy HH:mm:ss");
                        return $"#{myVal}#";
                    }
                    else
                    {
                        myVal = ((DateTime)val).ToString("yyyy-MM-dd HH:mm:ss");
                        return $"'{myVal}'";
                    }

                case "System.String":
                case "System.Text":
                    myVal = val.ToString();
                    myVal = myVal.Replace("'", "''");
                    return $"'{myVal}'";

                default:
                    return null;
            }

        }

        /// <summary>
        /// This overload validates filters against the WHERE clause of SQL statement
        /// </summary>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="sqlStatement"></param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for parameters / where clause.</param>
        /// <returns></returns>
        private static bool Valid_Filters(DataSourceTypes dataSourceType, string sqlStatement, List<Filter> filters)
        {
            // get list of column names in WHERE clause
            List<string> where_columns = Get_Where_Columns(sqlStatement);
            if (where_columns.Count == 0 & filters.Count == 0)
                return true;

            // return any mis-matches between list and filters
            var exceptions = Cross_Ref_Filters_And_List(dataSourceType, where_columns, filters);
            if (exceptions.Count > 0)
            {
                json = $"{{\"errors\":{JsonConvert.SerializeObject(exceptions)}}}";
                json = InsertJsonProperties(json, Formatting.None, "status", "error", "method", "Valid_Filters", "reason", "mismatch between filters & WHERE clause");
                LastError = json;
                return false;
            }
            return true;
        }

        /// <summary>
        /// This overload validates filters against columns/parameters
        /// </summary>
        /// <param name="conn">DbConnection object</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="dataSourceName">SqlQuery / StoredProcedure to select the data from.</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for parameters / where clause.</param>
        /// <returns></returns>
        private static bool Valid_Filters(IDbConnection conn, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName, List<Filter> filters)
        {
            // get list of parameters/columns
            List<string> parms_or_columns = Get_Parameters_Or_Columns(conn, dbFlavor, dataSourceType, dataSourceName);
            if (parms_or_columns.Count == 0 & filters.Count == 0)
                return true;

            // return any mis-matches between list and filters
            var exceptions = Cross_Ref_Filters_And_List(dataSourceType, parms_or_columns, filters);
            if (exceptions.Count > 0)
            {
                json = $"{{\"errors\":{JsonConvert.SerializeObject(exceptions)}}}";
                json = InsertJsonProperties(json, Formatting.None, "status", "error", "method", "Valid_Filters", "reason", "mismatch between filters & columns/parameters");
                LastError = json;
                return false;
            }
            return true;
        }

        #endregion

        #region private generic data access methods

        /// <summary>
        /// Convert a list of Filter objects to a valid WHERE clause
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/MySql/Sql.</param>
        /// <param name="filters">A list of <see cref="Filter"/> objects to evaluate.</param>
        /// <param name="result">Output. A WHERE clause.</param>
        /// <returns></returns>
        private static string Get_Where_Clause_From_Filters(DbFlavors dbFlavor, List<Filter> filters)
        {
            StringBuilder sbWhere = new StringBuilder();
            if (filters != null && filters.Count > 0)
            {
                Status = "Building Where Clause";
                var wildcard = dbFlavor == DbFlavors.Access ? "*" : "%";
                var parmChar = dbFlavor == DbFlavors.Access ? "?" : "@";
                foreach (var filter in filters)
                {
                    sbWhere.Append(" AND ");
                    sbWhere.Append(filter.Field);
                    switch (filter.Operator)
                    {
                        case FilterTypes.less:
                            sbWhere.Append(" < ");
                            break;
                        case FilterTypes.lessorequal:
                            sbWhere.Append(" <= ");
                            break;
                        case FilterTypes.equal:
                            sbWhere.Append(" = ");
                            break;
                        case FilterTypes.notequal:
                            sbWhere.Append(" <> ");
                            break;
                        case FilterTypes.greaterorequal:
                            sbWhere.Append(" >= ");
                            break;
                        case FilterTypes.greater:
                            sbWhere.Append(" > ");
                            break;
                        case FilterTypes.contains:
                            sbWhere.Append($" like '{wildcard}'+");
                            break;
                        case FilterTypes.notcontains:
                            sbWhere.Append($" not like '{wildcard}'+");
                            break;
                        case FilterTypes.starts:
                            sbWhere.Append(" like ");
                            break;
                        case FilterTypes.ends:
                            sbWhere.Append($" like '{wildcard}'+");
                            break;
                        //todo: figure out BETWEEN
                        default:
                            LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "reason", $"filter '{filter.Field}' has no operator" } });
                            sbWhere.Clear();
                            break;
                    }

                    // add parameter character (access: ? / others: @)
                    sbWhere.Append(parmChar);

                    // if not access, add field name to parameter character
                    if (dbFlavor != DbFlavors.Access)
                        sbWhere.Append(filter.Field);

                    // add wildcard for "contains", "starts with", "does not contain"
                    if (filter.Operator == FilterTypes.contains | filter.Operator == FilterTypes.starts | filter.Operator == FilterTypes.notcontains)
                        sbWhere.Append($"+'{wildcard}'");
                }
                sbWhere.Remove(0, 5);
                sbWhere.Insert(0, " WHERE ");
            }
            else
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "reason", "filters not supplied" } });

            return sbWhere.ToString();
        }

        /// <summary>
        /// Insert a data row into Access table
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="table">A <see cref="DataTable"/> object containing the record to be inserted.</param>
        /// <returns>ID of new record, or -1 if not auto-number ID, or 0 if insert fails.</returns>
        private static Int64 Insert_Data_Access(DbFlavors dbFlavor, DataTable table)
        {
            try
            {
                using (var conn = new OleDbConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Insert_Data_Access" }, { "reason", "could not create connection" } });
                        return 0;
                    }
                    conn.Open();
                    return Insert_To_Database_Table(dbFlavor, conn, table);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Insert_Data_Access" }, { "Error: ", ex.Message } });
                return 0;
            }
        }

        private static Int64 Insert_To_Database_Table(DbFlavors dbFlavor, IDbConnection conn, DataTable dataTable)
        {
            Status = SerializeDictionary(new Dictionary<string, string>
                                 { { "status", "testing" }, { "method", "Insert_To_Database_Table" } });
            //LastError = Status;
            //return 0;

            try
            {
                using (var cmd = conn.CreateCommand())
                {
                    // identify auto-increment column if present
                    bool isAutoIncrement = dataTable.PrimaryKey.Length == 1 && dataTable.PrimaryKey[0].AutoIncrement;

                    // get lists of columns and values to insert
                    string colsList;
                    string valsList;
                    if (Get_Insert_Field_List(dbFlavor, dataTable, out colsList, out valsList))
                    {
                        // execute command
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = $"Insert into {dataTable.TableName} ({colsList}) Values ({valsList})";

                        // testing
                        //LastError = GetJson(new Dictionary<string, string>
                        //            { { "status", "testing" }, { "method", "Insert_To_Database_Table" }, { "sql", cmd.CommandText } });
                        //return 0;

                        cmd.ExecuteNonQuery();

                        // now get last inserted ID if autonumber
                        if (dataTable.PrimaryKey.Length == 1 && dataTable.PrimaryKey[0].AutoIncrement)
                        {
                            switch (dbFlavor)
                            {
                                case DbFlavors.Access:
                                    cmd.CommandText = "Select @@Identity";
                                    break;
                                case DbFlavors.Sql:
                                    cmd.CommandText = "SELECT SCOPE_IDENTITY()";
                                    break;
                                case DbFlavors.MySql:
                                    cmd.CommandText = "Select LAST_INSERT_ID()";
                                    break;
                                default:
                                    LastError = SerializeDictionary(new Dictionary<string, string>
                                                { { "status", "error" }, { "method", "Insert_To_Database_Table" }, { "Error: ", "Invalid dbFlavor. Need: Access/MySql/Sql." } });
                                    return 0;
                            }
                            var lastID = cmd.ExecuteScalar();
                            return Convert.ToInt64(lastID);
                        }
                    }
                    else
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "Error: ", "Could not get lists of fields and values to insert" } });
                        return 0;
                    }
                }
                return 0;
            }
            catch (MySqlException ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Database_Table" }, { "MySql Error: ", ex.Message } });
                return 0;
            }
            catch (OleDbException ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Database_Table" }, { "OleDb Error: ", ex.Message } });
                return 0;
            }
            catch (SqlException ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Database_Table" }, { "Sql Error: ", ex.Message } });
                return 0;
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Database_Table" }, { "Error: ", ex.Message } });
                return 0;
            }
        }

        /// <summary>
        /// Load supplied filters as parameters into Command object
        /// </summary>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/MySql/Sql.</param>
        /// <param name="filters">A list of <see cref="Filter"/> objects to load into command as parameters.</param>
        /// <param name="command">The command to load with parameters</param>
        /// <returns></returns>
        private static bool Load_Parameters(DbFlavors dbFlavor, List<Filter> filters, IDbCommand command)
        {
            if (filters != null && filters.Count > 0)
            {

                try
                {
                    Status = "Loading Parameters";

                    foreach (var filter in filters)
                    {
                        switch (dbFlavor)
                        {
                            case DbFlavors.Access:
                                Status = $"Adding Parameter: @{filter.Field}";
                                ((OleDbCommand)command).Parameters.AddWithValue(filter.Field, filter.Value);
                                break;

                            case DbFlavors.Sql:
                                ((SqlCommand)command).Parameters.AddWithValue($"@{filter.Field}", filter.Value);
                                break;

                            case DbFlavors.MySql:
                                ((MySqlCommand)command).Parameters.AddWithValue($"@{filter.Field}", filter.Value);
                                break;

                            default:
                                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "reason", "dbFlavor not supplied" } });
                                return false;
                        }
                    }

                }
                catch (Exception ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "reason", ex.Message } });
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// This overload selects Access data from table.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="dataSourceName">Table / StoredProcedure to select the data from.</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects to narrow selection.</param>
        /// <param name="orderBy">Fields to order records by. Ex: Col1, Col3 Desc,...</param>
        /// <returns></returns>
        private static DataTable Select_Data_Access(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName, string dbColumns, List<Filter> filters, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_Data_Access");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_Data_Access" }, { "dbFlavor", dbFlavor.ToString() }, { "dataSourceType", dataSourceType.ToString() }, { "dataSourceName", dataSourceName } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new OleDbConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "Select_Data_Access" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Select_To_Data_Table(clientIp, conn, dbFlavor, dataSourceType, dataSourceName, dbColumns, filters, orderBy);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Select_Data_Access" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        /// <summary>
        /// This overload selects Access data using SQL statement
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourcetype">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="sql"></param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for WHERE clause.</param>
        /// <returns></returns>
        private static DataTable Select_Data_Access(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourcetype, string sql, List<Filter> filters)
        {
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Sql_Select_Access");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_Data_Access" }, { "dbFlavor", dbFlavor.ToString() }, { "sql", sql } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new OleDbConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "Select_Data_Access" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Select_To_Data_Table(clientIp, conn, dbFlavor, dataSourcetype, sql, filters);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Select_Data_Access" }, { "Error: ", ex.Message } });
                return null;
            }

        }

        /// <summary>
        /// This overload selects data from MySql table or stored procedure.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="dataSourceName">Table / StoredProcedure to select the data from.</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for parameters / where clause.</param>
        /// <param name="orderBy">Fields to order records by. Ex: Col1, Col3 Desc.</param>
        /// <returns></returns>
        private static DataTable Select_Data_MySql(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName, string dbColumns, List<Filter> filters, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_Data_MySql");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_Data_MySql" }, { "dbFlavor", dbFlavor.ToString() }, { "dataSourceType", dataSourceType.ToString() }, { "dataSourceName", dataSourceName } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new MySqlConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_Data_MySql" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Select_To_Data_Table(clientIp, conn, dbFlavor, dataSourceType, dataSourceName, dbColumns, filters, orderBy);
                }
            }
            catch (MySqlException ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", Status }, { "method", "Select_Data_MySql" }, { "MySql Error: ", ex.Message } });
                return null;

            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", Status }, { "method", "Select_Data_MySql" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        /// <summary>
        /// This overload selects data from MySql using SQL statement
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourcetype">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="sql"></param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for WHERE clause.</param>
        /// <returns></returns>
        private static DataTable Select_Data_MySql(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourcetype, string sql, List<Filter> filters)
        {
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_Data_MySql");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_Data_MySql" }, { "dbFlavor", dbFlavor.ToString() }, { "sql", sql } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new MySqlConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "Select_Data_MySql" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Select_To_Data_Table(clientIp, conn, dbFlavor, dataSourcetype, sql, filters);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Select_Data_MySql" }, { "Error: ", ex.Message } });
                return null;
            }

        }

        /// <summary>
        /// This overload selects data from SQL Server table or stored procedure.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="dataSourceName">Table / StoredProcedure to select the data from.</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects to build Where clause.</param>
        /// <param name="orderBy">Fields to order records by. Ex: Col1, Col3 Desc.</param>
        /// <returns></returns>
        private static DataTable Select_Data_Sql(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName, string dbColumns, List<Filter> filters, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_Data_Sql");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_Data_Sql" }, { "dbFlavor", dbFlavor.ToString() }, { "dataSourceType", dataSourceType.ToString() }, { "dataSourceName", dataSourceName } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new SqlConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "Select_Data_Sql" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Select_To_Data_Table(clientIp, conn, dbFlavor, dataSourceType, dataSourceName, dbColumns, filters, orderBy);
                }
            }
            catch (SqlException ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", Status }, { "method", "Select_Data_Sql" }, { "SQL Error: ", ex.Message } });
                return null;
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", Status }, { "method", "Select_Data_Sql" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        /// <summary>
        /// Select data from Sql Server using SQL statement
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourcetype">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="sql"></param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for WHERE clause.</param>
        /// <returns></returns>
        private static DataTable Select_Data_Sql(string clientIp, DbFlavors dbFlavor, DataSourceTypes dataSourcetype, string sql, List<Filter> filters)
        {
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_Data_Sql");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_Data_Sql" }, { "dbFlavor", dbFlavor.ToString() }, { "sql", sql } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new SqlConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "Select_Data_Sql" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Select_To_Data_Table(clientIp, conn, dbFlavor, dataSourcetype, sql, filters);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Select_Data_Sql" }, { "Error: ", ex.Message } });
                return null;
            }

        }

        /// <summary>
        /// This overload selects from table or stored procedure.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="conn">Open connection object to use.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="dataSourceType">A <see cref="DataSourceTypes"/> enumeration member. Ex: table/procedure.</param>
        /// <param name="dataSourceName">Table / StoredProcedure to select the data from.</param>
        /// <param name="dbColumns">The columns to return</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for Where clause and parameters.</param>
        /// <param name="orderBy">Fields to order records by. Ex: Col1, Col3 Desc.</param>
        /// <returns>A DataTable</returns>
        private static DataTable Select_To_Data_Table(string clientIp, IDbConnection conn, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string dataSourceName, string dbColumns, List<Filter> filters, string orderBy)
        {
            Logger.LogMethod();
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_To_Data_Table");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_To_Data_Table" }, { "dbFlavor", dbFlavor.ToString() }, { "dataSourceType", dataSourceType.ToString() },
                                  { "dataSourceName", dataSourceName }, {"dbColumns", dbColumns }, {"filters", Get_Filter_String(filters) } });
                LastError = Status;
                return null;
            }

            if (Valid_Filters(conn, dbFlavor, dataSourceType, dataSourceName, filters))
            {
                isTest = false;
                if (isTest)
                    Console.WriteLine("valid filters");

                try
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        // load command parameters
                        if (!Load_Parameters(dbFlavor, filters, cmd))
                            return null;

                        // setup command properties
                        switch (dataSourceType)
                        {
                            case DataSourceTypes.table:

                                cmd.CommandType = CommandType.Text;
                                var whereClause = Get_Where_Clause_From_Filters(dbFlavor, filters);
                                var orderByClause = orderBy == string.Empty ? string.Empty : $" ORDER BY {orderBy}";
                                var cols = dbColumns == string.Empty ? "*" : dbColumns;
                                var sql = $"SELECT {cols} FROM {dataSourceName}{whereClause}{orderByClause}";
                                cmd.CommandText = sql;
                                break;

                            case DataSourceTypes.procedure:

                                cmd.CommandType = CommandType.StoredProcedure;
                                Status = SerializeDictionary(new Dictionary<string, string>
                                     { { "status", "testing" }, { "method", "Select_To_Data_Table" }, { "info", "got procedure name" }, { "procedure", dataSourceName } });
                                isTest = false;
                                if (isTest)
                                {
                                    LastError = Status;
                                    return null;
                                }
                                cmd.CommandText = dataSourceName;
                                break;

                            default:

                                LastError = SerializeDictionary(new Dictionary<string, string>
                                        { { "status", "error" }, { "reason", "no data source type. need table/procedure" }, { "method", "Select_To_Data_Table" } });
                                return null;
                        }

                        // testing
                        isTest = false;
                        if (isTest)
                        {
                            json = SerializeDictionary(new Dictionary<string, string> { { "parameters", Get_Parameter_String(cmd) } });
                            json = InsertJsonProperties(json, Formatting.None, "status", "testing", "method", "Select_To_Data_Table", "commandText", cmd.CommandText);
                            Console.WriteLine(json + "\n\nPress any key to exit...");
                            LastError = json;
                            return null;
                        }

                        using (var dt = new DataTable(dataSourceName))
                        {
                            dt.Load(cmd.ExecuteReader());

                            // for Access, fix PrimaryKey and column IsKey properties
                            if (dbFlavor == DbFlavors.Access)
                            {
                                if (!Fix_Access_PrimaryKey_Property(dt, (OleDbCommand)cmd))
                                    return null;
                                if (!Fix_Access_AutoIncrement_Columns(dt, (OleDbCommand)cmd))
                                    return null;
                            }

                            // save DataTable if single record
                            if (dt.Rows.Count == 1)
                            {
                                savedData[clientIp + dt.TableName] = dt;
                                Logger.LogInfo($"IP {clientIp} saved table \"{dt.TableName}\"");
                                Console.WriteLine($"IP {clientIp} saved table \"{dt.TableName}\", {DateTime.Now.ToString()}.\n\nPress any key to exit");
                            }
                            return dt;
                        }
                    }
                }
                catch (OleDbException ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "OleDb Error: ", ex.Message } });
                    return null;
                }
                catch (MySqlException ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "MySql Error: ", ex.Message } });
                    return null;
                }
                catch (SqlException ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "Sql Error: ", ex.Message } });
                    return null;
                }
                catch (Exception ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "Error: ", ex.Message } });
                    return null;
                }
            }
            else
                return null;
        }

        /// <summary>
        /// This overload selects data using SQL query.
        /// </summary>
        /// <param name="clientIp">IP address of calling client.</param>
        /// <param name="conn">Open connection object to use.</param>
        /// <param name="dbFlavor">A <see cref="DbFlavors"/> enumeration member. Ex: Access/Sql/MySql.</param>
        /// <param name="sql">A valid SQL Select command.</param>
        /// <param name="filters">A Key-Value list of <see cref="Filter"/> objects for loading parameters.</param>
        /// <returns>A DataTable</returns>
        private static DataTable Select_To_Data_Table(string clientIp, IDbConnection conn, DbFlavors dbFlavor, DataSourceTypes dataSourceType, string sql, List<Filter> filters)
        {
            Logger.LogMethod();
            bool isTest = false;
            if (isTest)
            {
                Console.WriteLine("In Select_To_Data_Table");
                Status = SerializeDictionary(new Dictionary<string, string>
                                { { "status", "testing" }, { "method", "Select_To_Data_Table" }, { "dbFlavor", dbFlavor.ToString() },
                                  {"sql", sql }, {"filters", Get_Filter_String(filters) } });
                LastError = Status;
                return null;
            }

            if (Valid_Filters(conn, dbFlavor, dataSourceType, sql, filters))
            {
                isTest = false;
                if (isTest)
                    Console.WriteLine("valid filters");

                try
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        // load command parameters
                        if (!Load_Parameters(dbFlavor, filters, cmd))
                            return null;

                        // setup command properties
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sql;

                        // testing
                        isTest = false;
                        if (isTest)
                        {
                            json = SerializeDictionary(new Dictionary<string, string> { { "parameters", Get_Parameter_String(cmd) } });
                            json = InsertJsonProperties(json, Formatting.None, "status", "testing", "method", "Select_To_Data_Table", "commandText", cmd.CommandText);
                            Console.WriteLine(json + "\n\nPress any key to exit...");
                            LastError = json;
                            return null;
                        }

                        using (var dt = new DataTable("SelectResult"))
                        {
                            dt.Load(cmd.ExecuteReader());
                            return dt;
                        }
                    }
                }
                catch (OleDbException ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "OleDb Error: ", ex.Message } });
                    return null;
                }
                catch (MySqlException ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "MySql Error: ", ex.Message } });
                    return null;
                }
                catch (SqlException ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "Sql Error: ", ex.Message } });
                    return null;
                }
                catch (Exception ex)
                {
                    LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Select_To_Data_Table" }, { "Error: ", ex.Message } });
                    return null;
                }
            }
            else
                return null;
        }

        private static DataTable Update_Data_Access(string clientIp, DbFlavors dbFlavor, DataTable dtUpdated, List<string> colsAndValsToUpdate, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Status = SerializeDictionary(new Dictionary<string, string>
                        { { "status", "testing" }, { "method", "Update_Data_Access" }, { "dbFlavor", dbFlavor.ToString() } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new OleDbConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Update_Data_Access" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Update_To_Database_Table(clientIp, conn, dbFlavor, dtUpdated, colsAndValsToUpdate, orderBy);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Update_Data_Access" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        private static DataTable Update_Data_MySql(string clientIp, DbFlavors dbFlavor, DataTable dtUpdated, List<string> colsAndValsToUpdate, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Status = SerializeDictionary(new Dictionary<string, string>
                        { { "status", "testing" }, { "method", "Update_Data_MySql" }, { "dbFlavor", dbFlavor.ToString() } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new MySqlConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Update_Data_MySql" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Update_To_Database_Table(clientIp, conn, dbFlavor, dtUpdated, colsAndValsToUpdate, orderBy);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Update_Data_MySql" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        private static DataTable Update_Data_Sql(string clientIp, DbFlavors dbFlavor, DataTable dtUpdated, List<string> colsAndValsToUpdate, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Status = SerializeDictionary(new Dictionary<string, string>
                        { { "status", "testing" }, { "method", "Update_Data_Sql" }, { "dbFlavor", dbFlavor.ToString() } });
                LastError = Status;
                return null;
            }

            try
            {
                using (var conn = new SqlConnection(connString))
                {
                    if (conn == null)
                    {
                        LastError = SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "error" }, { "method", "Update_Data_Sql" }, { "reason", "could not create connection" } });
                        return null;
                    }
                    conn.Open();
                    return Update_To_Database_Table(clientIp, conn, dbFlavor, dtUpdated, colsAndValsToUpdate, orderBy);
                }
            }
            catch (Exception ex)
            {
                LastError = SerializeDictionary(new Dictionary<string, string>
                            { { "status", "error" }, { "method", "Update_Data_Sql" }, { "Error: ", ex.Message } });
                return null;
            }
        }

        private static DataTable Update_To_Database_Table(string clientIp, IDbConnection conn, DbFlavors dbFlavor, DataTable dtUpdated, List<string> colsAndValsToUpdate, string orderBy)
        {
            bool isTest = false;
            if (isTest)
            {
                Status = SerializeDictionary(new Dictionary<string, string>
                        { { "status", "testing" }, { "method", "Update_To_Database_Table" }, { "table", dtUpdated.TableName } });
                LastError = Status;
                return null;
            }

            try
            {
                List<Filter> filters = Get_Filters_From_DataTable(dbFlavor, dtUpdated);
                if (filters.Count > 0)
                {
                    string whereClause = Get_Where_Clause_From_Filters(dbFlavor, filters);
                    using (var cmd = conn.CreateCommand())
                    {
                        // load command parameters
                        if (!Load_Parameters(dbFlavor, filters, cmd))
                            return null;

                        // for update, must have parameters
                        if (cmd.Parameters.Count == 0)
                        {
                            json = JsonConvert.SerializeObject(filters);
                            json = InsertJsonProperties(json, Formatting.None, "error", "no parameters supplied for update");
                        }

                        string columnChanges = string.Join(", ", colsAndValsToUpdate.ToArray());
                        cmd.CommandText = $"UPDATE {dtUpdated.TableName} SET {columnChanges}{whereClause}";
                        cmd.CommandType = CommandType.Text;

                        isTest = false;
                        if (isTest)
                        {
                            Console.WriteLine($"command text: {cmd.CommandText}");
                            foreach (OleDbParameter parm in cmd.Parameters)
                            {
                                Console.WriteLine($"parm: {parm.ParameterName}, value: {parm.Value}");
                            }
                        }

                        isTest = false;
                        if (isTest)
                        {
                            json = SerializeDictionary(new Dictionary<string, string> { { "parameters", Get_Parameter_String(cmd) } });
                            json = InsertJsonProperties(json, Formatting.None, "status", "testing", "method", "Update_To_Data_Table", "commandText", cmd.CommandText);
                            LastError = json;
                            return null;
                        }

                        cmd.ExecuteNonQuery();

                        // now get updated version of DataTable
                        var dt = Select_To_Data_Table(clientIp, conn, dbFlavor, DataSourceTypes.table, dtUpdated.TableName, string.Empty, filters, orderBy);
                        return dt;
                    }


                }
                LastError = SerializeDictionary(new Dictionary<string, string>
                        { { "status", "error" }, { "method", "Update_To_Database_Table" }, { "reason", "couldn't build WHERE clause" } });
                return null;

            }
            catch (Exception)
            {

                throw;
            }




        }
        #endregion
    }

    public class Filter
    {
        public string Field { get; set; }
        public FilterTypes Operator { get; set; }
        public Object Value { get; set; }
    }

    public class DataItem
    {
        public string FieldName { get; set; }
        public string FieldType { get; set; }
        public Object FieldValue { get; set; }
    }
}
