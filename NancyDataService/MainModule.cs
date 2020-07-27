using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Nancy;
using Newtonsoft.Json;

namespace NancyDataService
{
    public class MainModule : NancyModule
    {
        public MainModule()
        {
            #region general instructions
            /* Uses NancyFx 2.0 (new syntax)
             * 
             * Create a GET method similar to:
             * Get("/path/path...", parms =>
             * {
             *      string jsonText = "";
             *      // do logic here
             *      // serialize desired result to json format
             *      return (Response)jsonText;
             * });
             * 
             * 
             * To put parameters in path segments:
             * Surround path segments with braces:
             *  ex: Get["/path/{aaa}/{bbb}"] = parms => {....}
             *  In code, refer to parms.aaa, parms.bbb, etc.
             * 
             * To put parameters at end of url:
             * Add to url: ?xxx=yyy&aaa=bbb, etc.
             *  ex: Get["/path?cust=aaa&order=bbb"] = parms => {....}
             *  In code, refer to Request.Query.cust, Request.Query.order, etc.
             *  (if specific item requested is not in the url, it will be null)
             *  You can also verify by Request.Query.item.HasValue, which returns true or false.
             *  You can also iterate these parameters:
             *  foreach (var parm in Request.Query)
             *   In this case, "parm" will contain the name of the parameter
             *   and Request.Query[parm] will be the value of the parameter.
             * 
             * All parameters passed in (except for ip=, db=, table=, procedure=, orderby= and columns=) are considered
             * to be filters used to select data. They are converted to Filter objects (see Filter class in DataAccess.cs).
             * they are collected in a List<Filter> so they can be passed into the DataAccess.GetData method.
             * 
             * To SELECT from table or stored procedure, create url similar to:
             * http://localhost:8080/select/?ip=192.168.1.18&amp;db=Access&amp;table=tblNAMembers&amp;NAID=equal:431
             * Call select function like this:
             * using (var client = new HttpClient())
             *   {
             *       var json = await client.GetStringAsync(url);
             *       DataTable dt = DeserializeDataTable(json);
             *   }
             * Expect a serialized DataTable to be returned, containing record(s) requested.
             * Note: This code uses Newtonsoft Json.Net for serialization/deserialization.
             *       Feel free to use another technique if wanted. If you use Json.Net,
             *       make sure you include the DataTableConverter class in Converters.cs
             *       as I have made some changes to the Newtonsoft version to fix issues.
             *       
             *       
             * To INSERT, UPDATE or DELETE, created a url similar to:
             * http://localhost:8080/insert?ip=192.168.1.18&amp;db=Access
             * http://localhost:8080/update?ip=192.168.1.18&amp;db=Access
             * http://localhost:8080/delete?ip=192.168.1.18&amp;db=Access
             * You must also include a serialized 1-record DataTable as payload, containing the particular
             * record to be INSERTed, UPDATEd or DELETEd.
             * Call insert, update or delete methods like this:
             * Assume you have a 1-record DataTable (dt), with the record being worked on.
             *   var json = SerializeDataTable(dt);
             *   var data = new StringContent(json, Encoding.UTF8, "application/json");
             *   var response = await client.PostAsync(url, data);
             *   string result = response.Content.ReadAsStringAsync().Result;
             *   dt = DeserializeDataTable(result);
             * For insert or update, dt will contain the record updated or inserted,
             *  including new AutoIncrement key value if appropriate.
             * For delete, (NEED TO FIGURE THIS OUT)
             */
            #endregion

            string json;
            DbFlavors dbFlavor;
            DataSourceTypes dsType = DataSourceTypes.table;
            List<Filter> filters = new List<Filter>();
            string dbColumns = string.Empty;
            string dsName = string.Empty;
            string orderBy = string.Empty;
            string clientIp = string.Empty;
            string msg = string.Empty;

            #region handle missing or invalid path
            Get("/", parms =>
            {
                msg = $"Missing path in URL: {Request.Url.ToString()}";
                json = GetJsonHelp(Formatting.Indented);
                json = DataAccess.InsertJsonProperties(json, Formatting.Indented, "status", "error", "reason", msg);
                Logger.LogError($"Missing path in URL: {Request.Url.ToString()}", "MainModule Routing");
                return (Response)json;
            });
            Get("/{parm1}", parms =>
            {
                var path = Request.Path.Substring(1);
                msg = $"Invalid path '{path}' in URL: {Request.Url.ToString()}";
                if (path != "favicon.ico")
                    Logger.LogError(msg, "MainModule Routing");
                json = GetJsonHelp(Formatting.Indented);
                json = DataAccess.InsertJsonProperties(json, Formatting.Indented, "status", "error", "reason", msg);
                return (Response)json;
            });
            #endregion

            #region help
            Get("help", parms =>
            {
                Logger.LogInfo("Help requested", "MainModule.Get(\"help\")");
                json = GetJsonHelp(Formatting.Indented);
                return (Response)json;
            });
            #endregion

            #region SELECT instructions
            /* 
             * Select from table or stored procedure
             * Ex: Access request: http://localhost:8080/select/?ip=192.168.1.18&amp;db=Access&amp;table=tblNAMembers&amp;NAID=equal:431
             *      MySql request: http://localhost:8080/select/?ip=192.168.1.18&amp;db=MySql&amp;procedure=get_members&amp;ID=eq:431
             * - path must be: select
             * - must have ?ip=
             * - must have &amp;db=[Access|Sql|MySql]
             * - must have &amp;table= or &amp;procedure=
             *   - note: stored procedures don't work well with Access
             *   - if table, may have orderby=col[desc][,col[desc] ...] (ignored for stored procedures)
             *   - if table, may have columns=col,col... (ignored for stored procedures)
             *     - if columns parameter has column names, those columns will be selected
             *     - if missing or no column names, all columns will be selected
             * - filter parameters are optional
             *    - if table, filter format is : colName=operator:value,...
             *    - if table, operators are: less, lessorequal, equal, notequal, greaterorequal, greater, contains, notcontains, starts, ends
             *       - multiple filters imply AND
             *    - if procedure, filter format is: parameterName=value,...
             *       - you may also use format: parameterName=equal:value,... (operator MUST be "equal")
             *       - must supply a filter for each stored procedure parameter
             */
            #endregion

            Get("select", parms =>
            {
                Logger.LogInfo($"SELECT requested: {Request.Url.ToString()}", "MainModule.Get(\"select\")");
                bool isTest = false;
                if (isTest)
                {
                    json = DataAccess.SerializeDictionary(new Dictionary<string, string> { { "status", "testing" }, { "result", "in select" } });
                    return (Response)json;
                }

                #region verify valid parameters

                if (!Request.Query.db.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "missing parameter: db=Access|Sql|MySql" } });

                bool isOK = Enum.IsDefined(typeof(DbFlavors), Request.Query.db.ToString());
                if (!isOK)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "invalid parameter: db=Access|Sql|MySql" } });

                if (!Request.Query.table.HasValue & !Request.Query.procedure.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'table' or 'procedure' missing" } });

                if (Request.Query.table.HasValue & Request.Query.procedure.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Can't use both 'table' and 'procedure' parameters" } });

                if (Request.Query.table.HasValue)
                    dsType = DataSourceTypes.table;
                else if (Request.Query.procedure.HasValue)
                    dsType = DataSourceTypes.procedure;

                if (dsType == DataSourceTypes.table && Request.Query.table == string.Empty)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'table' has no value" } });

                else if (dsType == DataSourceTypes.procedure && Request.Query.procedure == string.Empty)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'procedure' has no value" } });

                if (!Request.Query.ip.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'ip' missing, should be ip=<myIpAddress>" } });
                if (Request.Query.ip == string.Empty)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'ip' has no value, should be ip=<myIpAddress>" } });

                if (Request.Query.columns.HasValue)
                {
                    dbColumns = Request.Query.columns.ToString();
                    if (string.IsNullOrEmpty(dbColumns))
                        return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                { { "status", "fail" }, { "reason", "Parameter 'columns' must have column names: colName,colName..." } });

                    dbColumns = dbColumns.Replace(",", ", ", StringComparison.CurrentCultureIgnoreCase);
                }

                #endregion

                #region verify order by
                if (Request.Query.orderby.HasValue)
                {
                    if (Request.Query.orderby == string.Empty)
                        return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                        { { "status", "fail" }, { "reason", "Parameter 'orderby' has no value" } });
                    else
                        orderBy = Request.Query.orderby;
                }
                else
                    orderBy = string.Empty;
                #endregion

                clientIp = Request.Query.ip.ToString();
                dbFlavor = (DbFlavors)Enum.Parse(typeof(DbFlavors), Request.Query.db);
                dsName = dsType == DataSourceTypes.table ? Request.Query.table : Request.Query.procedure;

                #region get list of filters for query

                filters = new List<Filter>();
                foreach (var parm in Request.Query)
                {
                    if (parm != "db" & parm != "table" & parm != "procedure" & parm != "orderby" & parm != "ip" & parm != "columns")
                    {
                        // convert parm code (Ex: "greater:25") to array (Ex: [greater, 25])
                        string[] vals = Request.Query[parm].ToString().Split(":");

                        // if no operator (array has 1 segment), use "equal" for the operator and value provided
                        if (vals.Length == 1)
                        {
                            filters.Add(new Filter
                            {
                                Field = parm.ToString(),
                                Operator = (FilterTypes)Enum.Parse(typeof(FilterTypes), "equal"),
                                Value = vals[0]
                            }); ;
                        }
                        
                        // if operator (array has 2 segments), use operator and value provided
                        else if (vals.Length == 2)
                        {
                            if (Enum.IsDefined(typeof(FilterTypes), vals[0].ToLower(System.Globalization.CultureInfo.CurrentCulture)))
                            {
                                filters.Add(new Filter
                                {
                                    Field = parm.ToString(),
                                    Operator = (FilterTypes)Enum.Parse(typeof(FilterTypes), vals[0].ToLower(System.Globalization.CultureInfo.CurrentCulture)),
                                    Value = vals[1]
                                });
                            }
                            else
                            {
                                json = DataAccess.SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "fail" }, { "reason", $"Parameter '{parm.ToString()}': Incorrect Operator, need {DataAccess.opers}" } });
                                return (Response)json;
                            }
                        }
                        
                        // if array has other than 1 or 2 segments, note error and exit
                        else
                        {
                            json = DataAccess.SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "fail" }, { "reason", $"Bad parameter '{parm.ToString()}': Need Operator(equal,less,greater,etc):Value" } });
                            return (Response)json;
                        }
                    }
                }

                #endregion

                isTest = false;
                if (isTest)
                {
                    if (filters.Count == 0)
                        json = DataAccess.SerializeDictionary(new Dictionary<string, string> { { "filters", "''" } });
                    else
                        json = $"{{\"filters\":{JsonConvert.SerializeObject(filters, Formatting.None)}}}";

                    json = DataAccess.InsertJsonProperties(json, Formatting.Indented, "status", "testing", "dbFlavor", dbFlavor.ToString(), "data source type", dsType.ToString(), "dsName", dsName, "dbColumns", dbColumns, "order by", $"'{orderBy}'");
                    Console.WriteLine(json);
                    Console.WriteLine("\nPress any key to exit...");
                    return (Response)json;
                }

                #region get data
                using (var dt = DataAccess.GetData(clientIp, dbFlavor, dsType, dsName, dbColumns, filters, orderBy))
                {
                    if (dt == null)
                        json = DataAccess.LastError;
                    else
                    {
                        //Console.WriteLine("Got data table");
                        var settings = new JsonSerializerSettings() { Formatting = Formatting.Indented };
                        settings.Converters.Add(new Converters.DataTableConverter());
                        json = JsonConvert.SerializeObject(dt, settings);
                    }

                    return (Response)json;
                }
                #endregion
            });

            #region SQLSELECT instructions
            /* 
             * Select data using a SQL command
             * Ex: Access request: http://localhost:8080/sqlselect/?ip=192.168.1.18&amp;db=Access&amp;NAID=equal:431
             *      MySql request: http://localhost:8080/sqlselect/?ip=192.168.1.18&amp;db=MySql&amp;StudentID=equal:43
             * - path must be: sqlselect
             * - must have ?ip=
             * - must have &amp;db=[Access|Sql|MySql]
             * - SQL statement MUST begin with "select" (case-insensitive)
             * - must include filter parameters if SQL statement has a WHERE clause
             *   - must have a parameter for each WHERE clause column
             *   - filter format is: parameterName=value,...
             *   - you may also use format: parameterName=equal:value,... (operator MUST be "equal")
             */
            #endregion

            Post("sqlselect", parms =>
            {
                Logger.LogInfo($"SQL SELECT requested: {Request.Url.ToString()}", "MainModule.Get(\"selectsql\")");
                bool isTest = false;
                if (isTest)
                {
                    json = DataAccess.SerializeDictionary(new Dictionary<string, string> { { "status", "testing" }, { "result", "in selectsql" } });
                    return (Response)json;
                }

                #region verify valid parameters

                if (!Request.Query.db.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "missing parameter: db=Access|Sql|MySql" } });

                bool isOK = Enum.IsDefined(typeof(DbFlavors), Request.Query.db.ToString());
                if (!isOK)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "invalid parameter: db=Access|Sql|MySql" } });

                if (!Request.Query.ip.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'ip' missing, should be ip=<myIpAddress>" } });
                if (Request.Query.ip == string.Empty)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'ip' has no value, should be ip=<myIpAddress>" } });

                #endregion

                dsType = DataSourceTypes.sql;
                clientIp = Request.Query.ip.ToString();
                dbFlavor = (DbFlavors)Enum.Parse(typeof(DbFlavors), Request.Query.db);

                #region get list of filters for query
                filters = new List<Filter>();
                foreach (var parm in Request.Query)
                {
                    if (parm != "db" & parm != "table" & parm != "procedure" & parm != "orderby" & parm != "ip" & parm != "columns")
                    {
                        string[] vals = Request.Query[parm].ToString().Split(":");
                        if (vals.Length == 2)
                        {
                            if (Enum.IsDefined(typeof(FilterTypes), vals[0].ToLower()))
                            {
                                filters.Add(new Filter
                                {
                                    Field = parm.ToString(),
                                    Operator = (FilterTypes)Enum.Parse(typeof(FilterTypes), vals[0].ToLower()),
                                    Value = vals[1]
                                });
                            }
                            else
                            {
                                json = DataAccess.SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "fail" }, { "reason", $"Parameter '{parm.ToString()}': Incorrect Operator, need {DataAccess.opers}" } });
                                return (Response)json;
                            }
                        }
                        else
                        {
                            json = DataAccess.SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "fail" }, { "reason", $"Bad parameter '{parm.ToString()}': Need Operator(equal,less,greater,etc):Value" } });
                            return (Response)json;
                        }
                    }
                }
                #endregion

                // get SQL statement from request body
                var sql = new StreamReader(Request.Body).ReadToEnd();
                if (!sql.StartsWith("select", StringComparison.CurrentCultureIgnoreCase))
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", $"invalid SQL statement: {sql}" } });

                isTest = false;
                if (isTest)
                {
                    if (filters.Count == 0)
                        json = DataAccess.SerializeDictionary(new Dictionary<string, string> { { "filters", "''" } });
                    else
                        json = $"{{\"filters\":{JsonConvert.SerializeObject(filters, Formatting.None)}}}";

                    json = DataAccess.InsertJsonProperties(json, Formatting.Indented, "status", "testing", "dbFlavor", dbFlavor.ToString());
                    Console.WriteLine(json);
                    Console.WriteLine("\nPress any key to exit...");
                    return (Response)json;
                }

                #region get data
                using (var dt = DataAccess.GetData(clientIp, dbFlavor, dsType, sql, filters))
                {
                    if (dt == null)
                        json = DataAccess.LastError;
                    else
                    {
                        //Console.WriteLine("Got data table");
                        var settings = new JsonSerializerSettings() { Formatting = Formatting.Indented };
                        settings.Converters.Add(new Converters.DataTableConverter());
                        json = JsonConvert.SerializeObject(dt, settings);
                    }

                    return (Response)json;
                }
                #endregion

                json = "";
                return (Response)json;

            });

            #region INSERT, UPDATE, DELETE instructions
            // Ex: Access request: http://localhost:8080/insert?ip=192.168.1.18&amp;db=Access
            // - path must be: [insert|update|delete]
            // - must have ip=
            // - must have db=Access|Sql|MySql
            // - must have a serialized 1-record DataTable as payload for request.
            #endregion

            Post("update", parms =>
            {
                clientIp = Request.Query.ip.ToString();

                #region verify valid parameters
                if (!Request.Query.db.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Missing parameter: db=Access|Sql|MySql" } });
                dbFlavor = (DbFlavors)Enum.Parse(typeof(DbFlavors), Request.Query.db);
                if (!Enum.IsDefined(typeof(DbFlavors), dbFlavor))
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Invalid parameter: db=Access|Sql|MySql" } });

                if (!Request.Query.ip.HasValue)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'ip' missing, should be ip=<myIpAddress>" } });
                if (Request.Query.ip == string.Empty)
                    return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                            { { "status", "fail" }, { "reason", "Parameter 'ip' has no value, should be ip=<myIpAddress>" } });
                #endregion

                // get updated table from request body
                json = new StreamReader(Request.Body).ReadToEnd();
                using (DataTable dtUpdated = Deserialize_Table_Json(json))
                {
                    #region verify payload was deserialized into a 1-record DataTable
                    if (dtUpdated == null)
                        return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "reason", "could not deserialize payload into DataTable" }, { "payload", json } });
                    if (dtUpdated.Rows.Count != 1)
                        return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "table rows", dtUpdated.Rows.Count.ToString() }, { "reason", "must supply a 1-row DataTable for update" } });
                    #endregion

                    #region verify order by
                    if (Request.Query.orderby.HasValue)
                    {
                        if (Request.Query.orderby == string.Empty)
                            return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                    { { "status", "fail" }, { "reason", "Parameter 'orderby' has no value" } });
                        else
                            orderBy = Request.Query.orderby;
                    }
                    else
                        orderBy = string.Empty;
                    #endregion

                    #region verify the original record was saved
                    if (!DataAccess.savedData.ContainsKey(clientIp + dtUpdated.TableName))
                        return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "reason", "no saved record exists" }, {"ip address", clientIp }, {"table name", dtUpdated.TableName } });
                    #endregion

                    clientIp = Request.Query.ip.ToString();
                    try
                    {
                        using (DataTable dtSaved = DataAccess.savedData[clientIp + dtUpdated.TableName])
                        {
                            #region verify saved DataTable contains a single record
                            if (dtSaved.Rows.Count != 1)
                                return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "table rows", dtSaved.Rows.Count.ToString() }, { "reason", "saved DataTable must have exactly 1 DataRow" } });
                            #endregion

                            bool isTest = false;
                            if (isTest)
                            {
                                Console.WriteLine($"\nOriginal: {string.Join(", ", dtSaved.Rows[0].ItemArray)}");
                                Console.WriteLine($"\nUpdated : {string.Join(", ", dtUpdated.Rows[0].ItemArray)}");
                            }

                            // update data and get new updated record
                            using (var dt = DataAccess.UpdateData(clientIp, dbFlavor, dtUpdated, dtSaved, orderBy))
                            {
                                if (dt == null)
                                    json = DataAccess.LastError;
                                else
                                {
                                    var settings = new JsonSerializerSettings() { Formatting = Formatting.Indented };
                                    settings.Converters.Add(new Converters.DataTableConverter());
                                    json = JsonConvert.SerializeObject(dt, settings);
                                }

                                return (Response)json;
                            }


                            // insert record. result: 0 = failure, >0 = ID of new record, -1 = no auto-number field
                            //var newID = DataAccess.InsertData(dbFlavor, dtUpdated);

                            // loop thru columns, build sql statement, insert row
                            // ignore auto-increment columns in field list and value list
                            // "insert into {table} (xx,xx,xx) values (xx, xx, xx,...)"

                            // for SQL, select SCOPE_IDENTITY for last auto-increment ID
                            // for MySql, select LAST_INSERT_ID
                            // for Access???

                            // then get the new row, serialize and return

                        }
                    }
                    catch (Exception ex)
                    {
                        return (Response)DataAccess.SerializeDictionary(new Dictionary<string, string>
                                { { "status", "error" }, { "method", "MainModule" }, { "error", ex.Message } });
                    }
                }
            });

        }

        public string GetJsonHelp(Formatting formatting)
        {
            var jsonText = DataAccess.SerializeDictionary(new Dictionary<string, string> {
                { "HELP", "Building HTTP requests" },
                { " SELECT", "http://localhost:8080/select/ip=#.#.#.#&amp;db=Access (or MySql or Sql)" },
                { "  - SELECT requires", "&amp;table=TableName | &amp;procedure=ProcedureName"},
                { "  - option: ORDER BY (table only)", "&amp;orderby=colName[ desc],colName[ desc]..." },
                { "  - option: COLUMNS  (table only)", "&amp;columns=colName, colName... (if missing, selects all columns)" },
                { "  - option: FILTERS (for table)", "&amp;colName=<operator>:value,..." },
                { "     - operators (for table only)", "less/lessorEqual/equal/notequal/greaterorequal/greater/contains/notcontains/starts/ends" },
                { "  - option: FILTERS (for stored procedure)", "&amp;parameterName=value,..."},
                { "     - FILTER operators", "don't use operator for stored procedure, only parameterName=value." },
                { "     - filter values", "Values only, no quotes or apostrophes for strings or dates" },
                { "  - FILTER notes", "Multiple filters imply AND. No OR filtering allowed." },
                { " SQL SELECT", "http://localhost:8080/sqlselect/ip=#.#.#.#&amp;db=Access (or MySql or Sql)" },
                { "  - SQL SELECT requires", "SQL statement sent as payload of request"},
                { "  - SQL SELECT note", "SQL statement MUST begin with \"select\" (case-insensitive)" },
                { " INSERT", "http://localhost:8080/insert/ip=#.#.#.#&amp;db=Access (or MySql or Sql)" },
                { " UPDATE", "http://localhost:8080/update/ip=#.#.#.#&amp;db=Access (or MySql or Sql)" },
                { " DELETE", "http://localhost:8080/delete/ip=#.#.#.#&amp;db=Access (or MySql or Sql)" },
                { "  - DELETE note", "these require a serialized 1-record DataTable as payload containing the record to be inserted, updated or deleted." }
                }, formatting);
            return jsonText;
        }

        private DataTable Deserialize_Table_Json(string json)
        {
            try
            {
                var settings = new JsonSerializerSettings();
                settings.Converters.Add(new Converters.DataTableConverter());
                using (DataTable dt = JsonConvert.DeserializeObject<DataTable>(json, settings))
                {
                    return dt;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        //private List<Filter> Collect_Filters(dynamic query)
        //{
        //    var lf = new List<Filter>();

        //    foreach (var parm in query)
        //    {
        //        if (parm != "db" & parm != "table" & parm != "procedure" & parm != "orderby" & parm != "ip" & parm != "columns")
        //        {
        //            string[] vals = query[parm].ToString().Split(":");
        //            if (vals.Length == 2)
        //            {
        //                if (Enum.IsDefined(typeof(FilterTypes), vals[0].ToLower()))
        //                {
        //                    lf.Add(new Filter
        //                    {
        //                        Field = parm.ToString(),
        //                        Operator = (FilterTypes)Enum.Parse(typeof(FilterTypes), vals[0].ToLower()),
        //                        Value = vals[1]
        //                    });
        //                }
        //                else
        //                {
        //                    var json = DataAccess.SerializeDictionary(new Dictionary<string, string>
        //                            { { "status", "fail" }, { "reason", $"Parameter '{parm.ToString()}': Incorrect Operator, need {DataAccess.opers}" } });
        //                    return (Response)json;
        //                }
        //            }
        //            else
        //            {
        //                var json = DataAccess.SerializeDictionary(new Dictionary<string, string>
        //                            { { "status", "fail" }, { "reason", $"Bad parameter '{parm.ToString()}': Need Operator(equal,less,greater,etc):Value" } });
        //                return (Response)json;
        //            }
        //        }
        //    }
        //    return lf;
        //}
    }
}
