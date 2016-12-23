using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Threading;


namespace GenericSQLEntityHandler
{
    public  class GenericSQLEntityHandler
    {
        #region Private Members

        private bool useSqlTransaction = false;

        private int timeOut = 40;
        private int connectionRetries = 5;
        private string reconnectSqlConnectionString = "";

        private SqlTransaction sqlTransaction = null;
        private SqlConnection sqlConnection = null;

        private Dictionary<Type, Dictionary<string, string>> columnInformationList = new Dictionary<Type, Dictionary<string, string>>();

        #endregion Private Members

        #region Public Memebers

        public SqlConnection SqlConnection { get; private set; } = null;

        #endregion Public Members

        #region Events
        public event ErrorHandler ErrorOccurred;
        public delegate void ErrorHandler(Exception ex);
        #endregion Events

        public GenericSQLEntityHandler(SqlConnection sqlConnection)
        {
            SqlConnection = sqlConnection;
        }


        #region Save Methods

        /// <summary>
        /// Saves a single entity to the database.
        /// </summary>
        /// <param name="entity">The entity to save.</param>
        /// <param name="table">The name of the table to save to.</param>
        /// <param name="identityColumns">Only used for Update and InsertOrUpdate. Must contain the columns that identify the entity (Case sensitive).</param>
        /// <param name="saveType">To save to or update the database, InsertOrUpdate handles a mixed list, but has a little overhead. FastInsert will not select the autogen column after an insert.</param>
        /// <param name="autoGenIdColumn">If an autoincrement column is in the table, on insert it sets the new id to the inserted entitys corresponding property (Case sensitive).</param>
        /// <returns>Returns true if the entity is saved/updated, else false.</returns>
        public bool SaveEntity<T>(T entity, string table, string[] identityColumns, SaveType saveType, string autoGenIdColumn) where T : class
        {
            return SaveEntityList(new List<T>(new[] { entity }), table, identityColumns, saveType, autoGenIdColumn, false, null);
        }

        /// <summary>
		/// Saves a list of entities to the database.
		/// </summary>
		/// <param name="entityList">List of entities to save.</param>
		/// <param name="table">The name of the table to save to.</param>
		/// <param name="identityColumns">Only used for Update and InsertOrUpdate. Must contain the columns that identify the entity (Case sensitive).</param>
        /// <param name="saveType">To save or update the database, InsertOrUpdate handles a mixed list, but has a little overhead. FastInsert will not select the autogen column after an insert.</param>
		/// <param name="autoGenIdColumn">If an autoincrement column is in the table, on insert it sets the new id to the inserted entitys corresponding property (Case sensitive).</param>
		/// <returns>Returns true if all entities are saved/updated, else false.</returns>
        public bool SaveEntities<T>(List<T> entityList, string table, string[] identityColumns, SaveType saveType, string autoGenIdColumn) where T : class
        {
            return SaveEntityList(entityList, table, identityColumns, saveType, autoGenIdColumn, false, null);
        }

        /// <summary>
        /// Saves a list of entities to the database.
        /// </summary>
        /// <param name="entityList">List of entities to save.</param>
        /// <param name="table">The name of the table to save to.</param>
        /// <param name="identityColumns">Only used for Update and InsertOrUpdate. Must contain the columns that identify the entity (Case sensitive).</param>
        /// <param name="saveType">To save or update the database, InsertOrUpdate handles a mixed list, but has a little overhead. FastInsert will not select the autogen column after an insert.</param>
        /// <param name="autoGenIdColumn">If an autoincrement column is in the table, on insert it sets the new id to the inserted entitys corresponding property (Case sensitive).</param>
        /// <param name="fetchAutoGenIdColumnOnUpdate">True if the auto gen id column should be fetched on update, on insert it already does</param>
        /// <param name="propertiesToIgnore">Specify an array of property names, that should not be inserted/updated in the DB</param>
        /// <returns>Returns true if all entities are saved/updated, else false.</returns>
        public bool SaveEntities<T>(List<T> entityList, string table, string[] identityColumns, SaveType saveType, string autoGenIdColumn, bool fetchAutoGenIdColumnOnUpdate, string[] propertiesToIgnore) where T : class
        {
            return SaveEntityList(entityList, table, identityColumns, saveType, autoGenIdColumn, fetchAutoGenIdColumnOnUpdate, propertiesToIgnore);
        }

        private bool SaveEntityList<T>(List<T> entities, string table, string[] identityColumns, SaveType saveType,
            string autoGenIdColumn, bool fetchAutoGenIdColumnOnUpdate, string[] propertiesToIgnore) where T : class
        {
            try
            {
                Type entityType = typeof(T);
                List<T> entitiesToUpdate = new List<T>();
                List<T> entitiesToInsert = new List<T>();
                SqlCommand cmd = null;

                if (entities.Count == 0) //no entities was given, return true since nothing needs processed
                {
                    return true;
                }

                List<string> propsToIgnore = null;
                if (propertiesToIgnore?.Length > 0)
                {
                    propsToIgnore = new List<string>();
                    propsToIgnore.AddRange(propsToIgnore);
                }

                //initialize sql command
                cmd = GetSqlCommand();

                Dictionary<string, int> idMap = new Dictionary<string, int>();

                if (fetchAutoGenIdColumnOnUpdate)
                {
                    if (identityColumns.Length != 1)
                        throw new Exception("Only one identityColumn supported when using fetchAutoGenIdColumnOnUpdate");
                    cmd.Parameters.Clear();
                    cmd.CommandText = "SELECT " + identityColumns[0] + ", " + autoGenIdColumn + " FROM " + table;
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            idMap[reader[0].ToString()] = (int)reader[1];
                        }
                    }
                    cmd.CommandText = "";
                }

                //fetch table columns
                Dictionary<string, string> tableColumns = GetTableColumns(cmd, table, entityType);


                if (saveType == SaveType.Insert)
                {
                    entitiesToInsert = entities;
                }
                else if (saveType == SaveType.Update && identityColumns != null)
                {
                    entitiesToUpdate = entities;
                }
                // if "insertOrUpdate" and identities[] contains id columns - check if we should update or insert
                else if (identityColumns != null && identityColumns.Length > 0)
                {
                    // - generate the SQL
                    cmd.Parameters.Clear();
                    if (!string.IsNullOrEmpty(autoGenIdColumn))
                        cmd.CommandText = "SELECT TOP 1 [" + autoGenIdColumn + "] FROM [" + table + "] WHERE ";
                    else
                        cmd.CommandText = "SELECT TOP 1 [" + identityColumns[0] + "] FROM [" + table + "] WHERE ";
                    foreach (string idColumn in identityColumns)
                    {
                        cmd.CommandText += idColumn + " = @" + idColumn + " AND ";
                    }
                    cmd.CommandText = cmd.CommandText.Substring(0, cmd.CommandText.Length - 4);

                    foreach (T entity in entities)
                    {
                        cmd.Parameters.Clear();

                        foreach (string idColumn in identityColumns) // add parameters for each entitys identity columns
                        {
                            PropertyInfo propertyInfo = entityType.GetProperty(idColumn);
                            object fieldObj = propertyInfo.GetValue(entity, null);
                            AddParameterToCmd(fieldObj, cmd, idColumn);
                        }
                        object result = cmd.ExecuteScalar();
                        if (result == null)
                        {
                            // insert
                            entitiesToInsert.Add(entity);
                        }
                        else
                        { 
                            // update
                            if (!string.IsNullOrEmpty(autoGenIdColumn))
                            {
                                PropertyInfo propertyInfo = entityType.GetProperty(autoGenIdColumn);
                                propertyInfo.SetValue(entity, result, null);
                            }
                            entitiesToUpdate.Add(entity);
                        }
                    }
                }
                else
                {
                    // we could not determine if we should update or insert
                    return false;
                }

                List<string> identityColumnsList = new List<string>();
                if (identityColumns != null)
                {
                    foreach (string column in identityColumns)
                    {
                        identityColumnsList.Add(column.ToLower()); // to lower case, to prevent casesensitivity
                    }
                }

                #region Insert

                // Insert
                if (entitiesToInsert.Count > 0)
                {
                    autoGenIdColumn = autoGenIdColumn + "";
                    bool autoGenIdIsGuid = false;
                    List<PropertyInfo> propertiesToInsert = new List<PropertyInfo>();

                    string sqlInsertInto = "INSERT INTO [" + table + "](";
                    string sqlParameters = "VALUES(";
                    // find the columns to insert into
                    PropertyInfo[] propertyInfos = entityType.GetProperties();
                    foreach (PropertyInfo propertyInfo in propertyInfos)
                    {
                        if (propsToIgnore != null && propsToIgnore.Contains(propertyInfo.Name))
                            continue;

                        if (tableColumns.ContainsKey(propertyInfo.Name.ToLower()) && autoGenIdColumn.ToLower() != propertyInfo.Name.ToLower())
                        {
                            sqlInsertInto += propertyInfo.Name + ", ";
                            sqlParameters += "@" + propertyInfo.Name + ", ";
                            propertiesToInsert.Add(propertyInfo);
                        }
                        else if (autoGenIdColumn.ToLower() == propertyInfo.Name.ToLower() && propertyInfo.PropertyType == typeof(Guid))
                        {
                            autoGenIdIsGuid = true;
                            sqlInsertInto = "Declare @ins as table(newId uniqueidentifier) " + sqlInsertInto;
                            sqlParameters = " Output inserted." + autoGenIdColumn + " into @ins " + sqlParameters;
                        }
                    }

                    sqlInsertInto = sqlInsertInto.Substring(0, sqlInsertInto.Length - 2) + ") ";
                    sqlInsertInto += sqlParameters.Substring(0, sqlParameters.Length - 2) + ")";
                    cmd.CommandText = sqlInsertInto;


                    if (saveType != SaveType.FastInsert && !string.IsNullOrEmpty(autoGenIdColumn))
                    {
                        if (!autoGenIdIsGuid)
                            cmd.CommandText += "; SELECT SCOPE_IDENTITY();";
                        else
                        {
                            cmd.CommandText += " Select newId from @ins";
                        }
                    }
                    else
                    {
                        cmd.CommandText += ";";
                    }


                    // insert each entity
                    foreach (T entity in entitiesToInsert)
                    {
                        cmd.Parameters.Clear();

                        foreach (PropertyInfo propertyInfo in propertiesToInsert)
                        {
                            object fieldObj = propertyInfo.GetValue(entity, null);
                            AddParameterToCmd(fieldObj, cmd, propertyInfo.Name);
                        }


                        if (!string.IsNullOrEmpty(autoGenIdColumn))
                        {
                            if (!autoGenIdIsGuid)
                            {
                                int autoGenId = (int)(decimal)cmd.ExecuteScalar();
                                entityType.GetProperty(autoGenIdColumn).SetValue(entity, autoGenId, null);
                            }
                            else
                            {
                                Guid autoGenId = (Guid)cmd.ExecuteScalar();
                                entityType.GetProperty(autoGenIdColumn).SetValue(entity, autoGenId, null);
                            }
                        }
                        else
                        {
                            cmd.ExecuteNonQuery();
                        }

                    }
                }

                #endregion Insert

                #region Update

                // Update
                if (entitiesToUpdate.Count > 0)
                {
                    cmd.Parameters.Clear();
                    string sqlUpdate = "UPDATE [" + table + "] SET ";
                    string sqlWhere = " WHERE ";
                    List<PropertyInfo> whereProperties = new List<PropertyInfo>();
                    List<PropertyInfo> setProperties = new List<PropertyInfo>();


                    // find the columns to update and to filter by
                    PropertyInfo[] propertyInfos = entityType.GetProperties();
                    bool nothingToUpdate = true;
                    foreach (PropertyInfo propertyInfo in propertyInfos)
                    {
                        if (propsToIgnore != null && propsToIgnore.Contains(propertyInfo.Name))
                            continue;

                        if (identityColumnsList.IndexOf(propertyInfo.Name.ToLower()) >= 0)
                        {  // it's an index
                            sqlWhere += " " + propertyInfo.Name + " = @" + propertyInfo.Name + " AND ";
                            whereProperties.Add(propertyInfo);
                        }
                        else
                        {
                            if (tableColumns.ContainsKey(propertyInfo.Name.ToLower()) && propertyInfo.Name != autoGenIdColumn)
                            {
                                nothingToUpdate = false;
                                sqlUpdate += propertyInfo.Name + " = @" + propertyInfo.Name + ", ";
                                setProperties.Add(propertyInfo);
                            }
                        }
                    }
                    if (!nothingToUpdate)//skip this if not columns are specified in the update. It happens if the table contains only a few columns and they are key columns
                    {
                        cmd.CommandText = sqlUpdate.Substring(0, sqlUpdate.Length - 2) + sqlWhere.Substring(0, sqlWhere.Length - 4);

                        foreach (T entity in entitiesToUpdate)
                        {
                            cmd.Parameters.Clear();
                            foreach (PropertyInfo propertyInfo in setProperties)
                            {
                                object fieldObj = propertyInfo.GetValue(entity, null);
                                AddParameterToCmd(fieldObj, cmd, propertyInfo.Name);
                            }
                            foreach (PropertyInfo propertyInfo in whereProperties)
                            {
                                object fieldObj = propertyInfo.GetValue(entity, null);
                                AddParameterToCmd(fieldObj, cmd, propertyInfo.Name);
                            }


                            int affectedCount = cmd.ExecuteNonQuery();
                            if (affectedCount == 0)
                                return false;

                            if (fetchAutoGenIdColumnOnUpdate)
                            {
                                entityType.GetProperty(autoGenIdColumn).SetValue(entity, idMap[entityType.GetProperty(identityColumns[0]).GetValue(entity, null).ToString()], null);
                            }
                        }
                    }
                }

                #endregion Update

            }
            catch (Exception exception)
            {
                string errorMessage = GenerateErrorMessage<T>(entities.Count > 0 ? entities[0].GetType() : null, table, saveType, exception.Message,
                    exception.StackTrace);

                Debug.WriteLine(errorMessage);
                ErrorOccurred?.Invoke(exception);
                return false;
            }
            return true;
        }

        #endregion Save Methods


        #region Helper Methods

        private string GenerateErrorMessage<T>(Type entityType, string table, SaveType saveType, string exceptionMessage, string stacktrace) where T : class 
        {
            string message = "";
            message += "Entity type: " + entityType ??  "NULL" + Environment.NewLine;

            message += "Table name: " + table + Environment.NewLine;
            message += "Save type: " + saveType + Environment.NewLine;
            message += "Error message: " + exceptionMessage + Environment.NewLine;
            message += "Stacktrace: " + stacktrace + Environment.NewLine;

            return message;
        }

        private SqlCommand GetSqlCommand()
        {
            SqlCommand sqlCommand;
            if (sqlTransaction != null && sqlTransaction.Connection != null)
                sqlCommand = new SqlCommand("", sqlConnection, sqlTransaction);
            else
                sqlCommand = new SqlCommand("", sqlConnection);

            sqlCommand.CommandTimeout = timeOut;
            TryConnectToDatabase(sqlCommand);

            return sqlCommand;
        }

        private void TryConnectToDatabase(SqlCommand sqlCommand)
        {
            for (int tryCount = 0; tryCount < connectionRetries; tryCount++)
            {
                try
                {
                    sqlCommand.CommandText = "SELECT @@version";
                    sqlCommand.ExecuteScalar();
                    return;
                }
                catch (Exception)
                {
                    if (sqlTransaction != null && sqlTransaction.Connection != null)
                        throw new Exception("Unable to connect to database.");

                    if (tryCount == (connectionRetries -1))
                        throw new Exception("Unable to connect to database. Number of retries: " + connectionRetries);

                    if (string.IsNullOrEmpty(reconnectSqlConnectionString))
                        throw new Exception("Unable to connect to database.");

                    sqlConnection = new SqlConnection(reconnectSqlConnectionString);
                    sqlConnection.Open();
                    sqlCommand.Connection = sqlConnection;
                    Thread.Sleep(1000);
                }
            }
        }

        public Dictionary<string, string> GetTableColumns(SqlCommand cmd, string table, Type type)
        {
            Dictionary<string, string> tableColumns = null; //[Coloumn Name], [DataType]

            if (!columnInformationList.TryGetValue(type, out tableColumns))
            {
                tableColumns = new Dictionary<string, string>();
                cmd.CommandText = "SELECT TOP 1 * FROM [" + table + "]";
                SqlDataReader reader = cmd.ExecuteReader();
                DataTable dataTable = reader.GetSchemaTable();
                reader.Close();

                // add column names to list
                if (dataTable != null)
                {
                    foreach (DataRow row in dataTable.Rows)
                    {
                        tableColumns[(row["ColumnName"] + "").ToLower()] = "" + row["DataType"];
                    }
                    columnInformationList[type] = tableColumns;
                    dataTable.Dispose();
                }
            }
            return tableColumns;
        }

        private void AddParameterToCmd(object fieldObj, SqlCommand cmd, string idColumn)
        {
            cmd.Parameters.AddWithValue("@" + idColumn, fieldObj ?? DBNull.Value);
        }

        #endregion Helper Methods
    }
}
