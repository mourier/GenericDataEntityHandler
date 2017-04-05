using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;


namespace EntityHandler
{
    public  class GenericSQLEntityHandler
    {
        #region Private Members

        private bool useSqlTransaction = false;

        private int timeOut = 40;
        private int connectionRetries = 5;
        private string reconnectSqlConnectionString = "";

        private SqlTransaction sqlTransaction = null;
        

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

        #region Bulk Save
        public bool BulkInsert<T>(ICollection<T> entityList, string tableName, string[] columnNames) where T : class
        {
            return BulkInsert(entityList, tableName, columnNames, null);
        }

        public bool BulkInsert<T>(ICollection<T> entityList, string tableName, string[] columnNames, string identityColumn) where T : class
        {
            return BulkSave(entityList, tableName, columnNames, identityColumn, false);
        }

        public bool BulkUpdate<T>(ICollection<T> entityList, string tableName, string[] columnNames, string identityColumn) where T : class
        {
            return BulkSave(entityList, tableName, columnNames, identityColumn, true);
        }
        public bool BulkSave<T>(ICollection<T> entities, string tableName, string[] columnNames, string identityColumn,
            bool update, int bulkCopyTimeout = 600) where T : class
        {
            bool succes = false;

            try
            {
                string destinationTableName;
                bool identity = !string.IsNullOrEmpty(identityColumn);
                if (identity)
                    destinationTableName = "#tmp_" + tableName;
                else
                    destinationTableName = tableName;

                Type entityType = typeof(T);
                List<PropertyInfo> propertyInfos = new List<PropertyInfo>();
                if(columnNames != null && columnNames.Length > 0)
                {
                    foreach (string columnName in columnNames)
                    {
                        propertyInfos.Add(entityType.GetProperty(columnName));
                    }
                }
                else
                {
                    SqlCommand tempCommand = GetSqlCommand();
                    Dictionary<string, string> tableColumns = GetTableColumns(tempCommand, tableName, entityType);
                    tempCommand.Dispose();
                    PropertyInfo[] allPropertyInfoss = entityType.GetProperties();
                    foreach (PropertyInfo propertyInfo in allPropertyInfoss)
                    {
                        if (tableColumns.ContainsKey(propertyInfo.Name.ToLower()))
                        {
                            propertyInfos.Add(propertyInfo);
                        }
                    }
                    tempCommand.Dispose();
                }



                SqlCommand cmd = GetSqlCommand();
                SqlBulkCopy copy = new SqlBulkCopy(SqlConnection, update || identity ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default, sqlTransaction);
                copy.BulkCopyTimeout = bulkCopyTimeout;

                try
                {

                    DataTable table = new DataTable();
                    string columnNamesForQuery = "";
                    foreach (PropertyInfo property in propertyInfos)
                    {
                        if (!identity || property.Name.ToLower() != identityColumn.ToLower())
                        {
                            if (!update)
                                columnNamesForQuery += "[" + property.Name + "],";
                            else
                                columnNamesForQuery += "[" + tableName + "].[" + property.Name + "]=" + destinationTableName + ".[" + property.Name + "],";
                        }
                        if (property.PropertyType == typeof(int?))
                        {
                            DataColumn col = new DataColumn(property.Name, typeof(int));
                            col.AllowDBNull = true;
                            table.Columns.Add(col);
                        }
                        else
                            table.Columns.Add(property.Name, property.PropertyType);
                        copy.ColumnMappings.Add(property.Name, property.Name);
                    }
                    columnNamesForQuery = columnNamesForQuery.TrimEnd(',');
                    DataRow row;
                    int rowNumber = 1000;
                    foreach (T entity in entities)
                    {
                        row = table.NewRow();
                        int index = 0;
                        foreach (PropertyInfo property in propertyInfos)
                        {
                            if (!update && identity && property.Name.ToLower() == identityColumn.ToLower())
                            {
                                property.SetValue(entity, rowNumber, null);
                            }
                            object val = property.GetValue(entity, null);
                            if (val == null)
                                row[index] = DBNull.Value;
                            else
                                row[index] = val;
                            index++;
                        }
                        table.Rows.Add(row);
                        rowNumber++;
                    }

                    copy.DestinationTableName = destinationTableName;
                    copy.BatchSize = entities.Count;

                    if (identity)
                    {
                        //Check if the temp table exists for some reason and then just reuse it if it does otherwise create it
                        cmd.CommandText = "IF OBJECT_ID('tempdb.." + destinationTableName + "') IS NULL BEGIN SELECT 0 END ELSE SELECT 1";
                        if ((int)cmd.ExecuteScalar() == 0)
                            CreateTableClone(tableName, destinationTableName, entityType);
                        else
                            cmd.CommandText = "TRUNCATE TABLE " + destinationTableName;

                        copy.WriteToServer(table);
                        if (!update)
                        {
                            cmd.CommandText = "DECLARE @tmpIdTable TABLE (id INT) ";
                            cmd.CommandText += "INSERT INTO [" + tableName + "] (" + columnNamesForQuery + ") ";
                            cmd.CommandText += "OUTPUT inserted.[" + identityColumn + "] INTO @tmpIdTable ";
                            cmd.CommandText += "SELECT " + columnNamesForQuery + " FROM " + destinationTableName;
                            cmd.CommandText += " ORDER BY [" + identityColumn + "] ";
                            cmd.CommandText += "SELECT [" + identityColumn + "] FROM @tmpIdTable ORDER BY [" + identityColumn + "]";
                            using (SqlDataReader reader = cmd.ExecuteReader())
                            {
                                IEnumerator<T> enumerator = entities.GetEnumerator();
                                while (reader.Read())
                                {
                                    enumerator.MoveNext();
                                    entityType.GetProperty(identityColumn).SetValue(enumerator.Current, reader[0], null);
                                }
                            }
                        }
                        else
                        {
                            cmd.CommandText = "UPDATE [" + tableName + "] SET " + columnNamesForQuery + " FROM " + destinationTableName;
                            cmd.CommandText += " WHERE " + destinationTableName + ".[" + identityColumn + "]=[" + tableName + "].[" + identityColumn + "]";
                            succes = cmd.ExecuteNonQuery() == entities.Count;
                        }
                        cmd.CommandText = "DROP TABLE " + destinationTableName;
                        cmd.ExecuteNonQuery();
                        cmd.Dispose();
                    }
                    else
                        copy.WriteToServer(table);

                    succes = true;
                }
                finally
                {
                    copy.Close();
                }
            }
            catch (Exception exception)
            {
                string errorMessage = GenerateErrorMessage<T>(entities.Count > 0 ? entities.First().GetType() : null, tableName, (update ? SaveType.Update : SaveType.Insert), exception.Message,
                    exception.StackTrace);



                Debug.WriteLine(errorMessage);
                ErrorOccurred?.Invoke(exception);
            }

            return succes;
        }

        #endregion Bulk Save

            /// <summary>
            /// Saves a single entity to the database.
            /// </summary>
            /// <param name="entity">The entity to save.</param>
            /// <param name="table">The name of the table to save to.</param>
            /// <param name="identityColumns">Only used for Update and InsertOrUpdate. Must contain the columns that identify the entity (Case sensitive).</param>
            /// <param name="saveType">To save to or update the database, InsertOrUpdate handles a mixed list, but has a little overhead. FastInsert will not select the autogen column after an insert.</param>
            /// <param name="autoGenIdColumn">If an autoincrement column is in the table, on insert it sets the new id to the inserted entitys corresponding property (Case sensitive).</param>
            /// <returns>Returns true if the entity is saved/updated, else false.</returns>
        public bool SaveEntity<T>(T entity, string tableName, string[] identityColumns, SaveType saveType, string autoGenIdColumn) where T : class
        {
            return SaveEntityList(new List<T>(new[] { entity }), tableName, identityColumns, saveType, autoGenIdColumn, false, null);
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
        public bool SaveEntities<T>(List<T> entityList, string tableName, string[] identityColumns, SaveType saveType, string autoGenIdColumn) where T : class
        {
            return SaveEntityList(entityList, tableName, identityColumns, saveType, autoGenIdColumn, false, null);
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
        public bool SaveEntities<T>(List<T> entityList, string tableName, string[] identityColumns, SaveType saveType, string autoGenIdColumn, bool fetchAutoGenIdColumnOnUpdate, string[] propertiesToIgnore) where T : class
        {
            return SaveEntityList(entityList, tableName, identityColumns, saveType, autoGenIdColumn, fetchAutoGenIdColumnOnUpdate, propertiesToIgnore);
        }

        private bool SaveEntityList<T>(List<T> entities, string tableName, string[] identityColumns, SaveType saveType,
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
                    cmd.CommandText = "SELECT " + identityColumns[0] + ", " + autoGenIdColumn + " FROM " + tableName;
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
                Dictionary<string, string> tableColumns = GetTableColumns(cmd, tableName, entityType);


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
                        cmd.CommandText = "SELECT TOP 1 [" + autoGenIdColumn + "] FROM [" + tableName + "] WHERE ";
                    else
                        cmd.CommandText = "SELECT TOP 1 [" + identityColumns[0] + "] FROM [" + tableName + "] WHERE ";
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

                    string sqlInsertInto = "INSERT INTO [" + tableName + "](";
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
                    string sqlUpdate = "UPDATE [" + tableName + "] SET ";
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
                string errorMessage = GenerateErrorMessage<T>(entities.Count > 0 ? entities[0].GetType() : null, tableName, saveType, exception.Message,
                    exception.StackTrace);

                Debug.WriteLine(errorMessage);
                ErrorOccurred?.Invoke(exception);
                return false;
            }
            return true;
        }

        #endregion Save Methods

        #region Load Methods
        /// <summary>
        /// Tries to load a single entity from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="entity">When this method returns, contains the loaded value, if successful; otherwise, the value will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadSingleEntity<T>(string table, Dictionary<string, object> filterDictionary, out T entity) where T : class
        {
            entity = null;
            List<T> entityList;
            if (TryLoadEntities(table, filterDictionary, true, null, -1, out entityList))
            {
                if (entityList.Count != 0)
                {
                    entity = entityList[0];
                    return true;
                }
            }
            return false;
        }

        /// <summary>
		/// Load a single entity from the database.
		/// </summary>
		/// <param name="table">The name of the table to load from.</param>
		/// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
		/// Filter may be null if no search criteria!</param>
		/// <returns>Returns the loaded entity if successful, else null.</returns>
        public T LoadSingleEntity<T>(string table, Dictionary<string, object> filterDictionary) where T : class
        {
            T entity;
            TryLoadSingleEntity(table, filterDictionary, out entity);
            return entity;
        }

        /// <summary>
        /// Tries to load a list of entities from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="entityList">When this method returns, contains the loaded values, if successful; otherwise, the list will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadEntities<T>(string table, Dictionary<string, object> filterDictionary, out List<T> entityList) where T : class
        {
            return TryLoadEntities(table, filterDictionary, false, null, -1, out entityList);
        }

        /// <summary>
        /// Loads a list of entities from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <returns>Returns a list of loaded entities if successful, else null.</returns>
        public List<T> LoadEntities<T>(string table, Dictionary<string, object> filterDictionary) where T : class
        {
            List<T> entityList;
            TryLoadEntities(table, filterDictionary, false, null, -1, out entityList);
            return entityList;
        }

        /// <summary>
        /// Tries to load a single entity from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Order by string: ie: 'id DESC, name'.</param>
        /// <param name="entity">When this method returns, contains the loaded value, if successful; otherwise, the value will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadSingleEntity<T>(string table, Dictionary<string, object> filterDictionary, string orderBy, out T entity) where T : class
        {
            entity = null;
            List<T> entityList;
            if (TryLoadEntities(table, filterDictionary, true, orderBy, -1, out entityList))
            {
                if (entityList.Count != 0)
                {
                    entity = entityList[0];
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Load a single entity from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Order by string: ie: 'id DESC, name'.</param>
        /// <returns>Returns the loaded entity if successful, else null.</returns>
        public T LoadSingleEntity<T>(string table, Dictionary<string, object> filterDictionary, string orderBy) where T : class
        {
            T entity;
            TryLoadSingleEntity(table, filterDictionary, orderBy, out entity);
            return entity;
        }

        /// <summary>
        /// Tries to load a list of entities from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'.</param>
        /// <param name="entityList">When this method returns, contains the loaded values, if successful; otherwise, the list will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadEntities<T>(string table, Dictionary<string, object> filterDictionary, string orderBy, out List<T> entityList) where T : class
        {
            return TryLoadEntities(table, filterDictionary, false, orderBy, -1, out entityList);
        }

        /// <summary>
        /// Loads a list of entities from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'.</param>
        /// <returns>Returns a list of loaded entities if successful, else null.</returns>
        public List<T> LoadEntities<T>(string table, Dictionary<string, object> filterDictionary, string orderBy) where T : class
        {
            List<T> entityList;
            TryLoadEntities(table, filterDictionary, orderBy, out entityList);
            return entityList;
        }

        /// <summary>
        /// Tries to load a list of entities from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'</param>
        /// <param name="maxRowCount">Max row count. A value of -1 means all.</param>
        /// <param name="entityList">When this method returns, contains the loaded values, if successful; otherwise, the list will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadEntities<T>(string table, Dictionary<string, object> filterDictionary, string orderBy, int maxRowCount, out List<T> entityList) where T : class
        {
            return TryLoadEntities(table, filterDictionary, false, orderBy, maxRowCount, out entityList);
        }

        /// <summary>
        /// Loads a list of entities from the database.
        /// </summary>
        /// <param name="table">The name of the table to load from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'</param>
        /// <param name="maxRowCount">Max row count. A value of -1 means all.</param>
        /// <returns>Returns a list of loaded entities if successful, else null.</returns>
        public List<T> LoadEntities<T>(string table, Dictionary<string, object> filterDictionary, string orderBy, int maxRowCount) where T : class
        {
            List<T> entityList;
            TryLoadEntities(table, filterDictionary, orderBy, maxRowCount, out entityList);
            return entityList;
        }

        /// <summary>
        /// Tries to load a list of entities from the database.
        /// </summary>
        /// <param name="query">The sql query to execute.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'.</param>
        /// <param name="entityList">When this method returns, contains the loaded values, if successful; otherwise, the list will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadEntitiesByQuery<T>(string query, ICollection<KeyValuePair<string, object>> filterDictionary, string orderBy, out List<T> entityList) where T : class
        {
            return TryLoadEntitiesByQuery(null, query, filterDictionary, orderBy, out entityList);
        }

        /// <summary>
        /// Loads a list of entities from the database.
        /// </summary>
        /// <param name="query">The sql query to execute.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'.</param>
        /// <returns>Returns a list of loaded entities if successful, else null.</returns>
        public List<T> LoadEntitiesByQuery<T>(string query, ICollection<KeyValuePair<string, object>> filterDictionary, string orderBy) where T : class
        {
            return LoadEntitiesByQuery<T>(null, query, filterDictionary, orderBy);
        }

        /// <summary>
        /// Tries to load a list of entities from the database.
        /// </summary>
        /// <param name="objectType">Alternative object type</param>
        /// <param name="query">The sql query to execute.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'.</param>
        /// <param name="entityList">When this method returns, contains the loaded values, if successful; otherwise, the list will be null. This parameter is passed uninitialized.</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool TryLoadEntitiesByQuery<T>(Type objectType, string query, ICollection<KeyValuePair<string, object>> filterDictionary, string orderBy, out List<T> entityList) where T : class
        {
            SqlDataReader reader = null;
            SqlCommand cmd = GetSqlCommand();
            Dictionary<string, string> columnsInBoth = new Dictionary<string, string>();
            entityList = null;
            Type entityType = null;

            try
            {
                cmd.Parameters.Clear();
                string sqlSelect = "";
                sqlSelect = query;
                if (filterDictionary != null && filterDictionary.Count > 0)
                {
                    if (!query.ToLower().Contains("where "))
                    {
                        sqlSelect += " WHERE ";
                    }
                    else if (!query.ToLower().EndsWith(" where "))
                    {
                        sqlSelect += " AND ";
                    }

                    int i = 0;
                    foreach (KeyValuePair<string, object> conditionDE in filterDictionary)
                    {
                        sqlSelect += conditionDE.Key + " AND ";
                        if (conditionDE.Value != null)
                        {
                            AddParameterToCmd(conditionDE.Value, cmd, "" + i++);
                        }
                    }
                    sqlSelect = sqlSelect.Substring(0, sqlSelect.Length - 4);
                }
                // OrderBy part
                if (!string.IsNullOrEmpty(orderBy))
                {
                    sqlSelect += " ORDER BY " + orderBy;
                }

                // retreive the data
                cmd.CommandText = sqlSelect;
                reader = cmd.ExecuteReader();
                entityType = null;
                if (objectType != null)
                    entityType = objectType;
                else
                    entityType = typeof(T);
                bool firstRecord = true;
                entityList = new List<T>();
                while (reader.Read())
                {
                    if (firstRecord)
                    {
                        Dictionary<string, string> tableColumns = GetTableColumns(reader);

                        PropertyInfo[] propertyInfos = entityType.GetProperties();
                        foreach (PropertyInfo propertyInfo in propertyInfos)
                        {
                            if (tableColumns.ContainsKey(propertyInfo.Name.ToLower()))
                            {
                                columnsInBoth.Add(propertyInfo.Name.ToLower(), propertyInfo.Name);
                            }
                        }
                        firstRecord = false;
                    }

                    T entityObj = (T)Activator.CreateInstance(entityType);
                    foreach (string columnName in columnsInBoth.Values)
                    {
                        object dbObj = reader[columnName];
                        if (dbObj is DBNull)
                        {
                            // only handle properties, that can be set to null
                            entityType.GetProperty(columnName).SetValue(entityObj, null, null);
                        }
                        else
                        {
                            entityType.GetProperty(columnName).SetValue(entityObj, dbObj, null);
                        }
                    }
                    entityList.Add(entityObj);
                }
                reader.Close();
                return true;
            }
            catch (Exception ex)
            {
                string message = "";
                message += "Entity type: " + (entityType != null ? entityType.ToString() : "NULL") + Environment.NewLine;
                message += "Query: " + query + Environment.NewLine;
                if (filterDictionary != null && filterDictionary.Count != 0)
                {
                    message += "Filter: " + Environment.NewLine;
                    foreach (KeyValuePair<string, object> pair in filterDictionary)
                    {
                        message += "\tKey: " + pair.Key + "\tValue: " + pair.Value + Environment.NewLine;
                    }
                }
                message += "Error message: " + ex.Message + Environment.NewLine;
                message += "Stacktrace: " + ex.StackTrace + Environment.NewLine;

                Debug.WriteLine(message);

                if (reader != null)
                    reader.Close();
                if (ErrorOccurred != null)
                    ErrorOccurred(ex);
            }
            entityList = null;
            return false;
        }

        /// <summary>
        /// Loads a list of entities from the database.
        /// </summary>
        /// <param name="objectType">Alternative object type</param>
        /// <param name="query">The sql query to execute.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="orderBy">Sort by string: ie: 'id DESC, name'.</param>
        /// <returns>Returns a list of loaded entities if successful, else null.</returns>
        public List<T> LoadEntitiesByQuery<T>(Type objectType, string query, ICollection<KeyValuePair<string, object>> filterDictionary, string orderBy) where T : class
        {
            List<T> entityList;
            TryLoadEntitiesByQuery(objectType, query, filterDictionary, orderBy, out entityList);
            return entityList;
        }

        private bool TryLoadEntities<T>(string table, ICollection<KeyValuePair<string, object>> filterDictionary, bool singleEntity, string orderBy, int maxRowCount, out List<T> entityList) where T : class
        {
            SqlDataReader reader = null;
            SqlCommand cmd = GetSqlCommand();
            Dictionary<string, string> columnsInBoth = new Dictionary<string, string>();
            entityList = null;
            Type entityType = null;

            try
            {
                // find columns both in the table and the type
                entityType = typeof(T);
                Dictionary<string, string> tableColumns = GetTableColumns(cmd, table, entityType);

                PropertyInfo[] propertyInfos = entityType.GetProperties();
                foreach (PropertyInfo propertyInfo in propertyInfos)
                {
                    if (tableColumns.ContainsKey(propertyInfo.Name.ToLower()))
                    {
                        columnsInBoth.Add(propertyInfo.Name.ToLower(), propertyInfo.Name);
                    }
                }

                if (columnsInBoth.Count == 0) // nothing fits
                    return false;

                cmd.Parameters.Clear();
                // generate the select statement
                // - Select part
                string sqlSelect = "SELECT ";
                if (singleEntity)
                    sqlSelect += "TOP 1 ";
                else if (maxRowCount != -1)
                {
                    sqlSelect += "TOP " + maxRowCount + " ";
                }
                foreach (string col in columnsInBoth.Values)
                {
                    sqlSelect += col + ", ";
                }
                sqlSelect = sqlSelect.Substring(0, sqlSelect.Length - 2);
                sqlSelect += " FROM [" + table + "] ";
                // - Where part
                if (filterDictionary != null && filterDictionary.Count > 0)
                {
                    int i = 0;
                    sqlSelect += " WHERE ";
                    foreach (KeyValuePair<string, object> pair in filterDictionary)
                    {
                        sqlSelect += pair.Key + " AND ";
                        if (pair.Value != null)
                        {
                            AddParameterToCmd(pair.Value, cmd, "" + i++);
                        }
                    }
                    sqlSelect = sqlSelect.Substring(0, sqlSelect.Length - 4);
                }
                // OrderBy part
                if (!string.IsNullOrEmpty(orderBy))
                {
                    sqlSelect += " ORDER BY " + orderBy;
                }

                // retreive the data
                cmd.CommandText = sqlSelect;
                reader = cmd.ExecuteReader();
                entityList = new List<T>();
                while (reader.Read())
                {
                    T entityObj = (T)Activator.CreateInstance(entityType);
                    foreach (string columnName in columnsInBoth.Values)
                    {
                        object dbObj = reader[columnName];
                        if (dbObj is DBNull)
                        {
                            // only handle properties, that can be set to null
                            entityType.GetProperty(columnName).SetValue(entityObj, null, null);
                        }
                        else
                        {
                            entityType.GetProperty(columnName).SetValue(entityObj, dbObj, null);
                        }
                    }
                    entityList.Add(entityObj);
                }
                reader.Close();

                return true;
            }
            catch (Exception ex)
            {
                string message = "";
                message += "Entity type: " + (entityType != null ? entityType.ToString() : "NULL") + Environment.NewLine;
                if (filterDictionary != null && filterDictionary.Count != 0)
                {
                    message += "Filter: " + Environment.NewLine;
                    foreach (KeyValuePair<string, object> pair in filterDictionary)
                    {
                        message += "\tKey: " + pair.Key + "\tValue: " + pair.Value + Environment.NewLine;
                    }
                }
                message += "Error message: " + ex.Message + Environment.NewLine;
                message += "Stacktrace: " + ex.StackTrace + Environment.NewLine;

                Debug.WriteLine(message);

                if (reader != null)
                    reader.Close();
                if (ErrorOccurred != null)
                    ErrorOccurred(ex);
            }
            entityList = null;
            return false;
        }
        #endregion Load Methods

        #region Delete

        /// <summary>
        /// Deletes an entity from the database.
        /// </summary>
        /// <param name="entity">The entity to delete.</param>
        /// <param name="table">The name of the table to delete from.</param>
        /// <param name="identityColumns">Must contain the columns that identify the entity (Case sensitive).</param>
        /// <returns>Returns true if successful, else false.</returns>
        public bool DeleteEntity<T>(T entity, string tableName, string[] identityColumns) where T : class
        {
            return DeleteEntitiesFromList(new List<T> { entity }, tableName, identityColumns);
        }

        /// <summary>
        /// Deletes a list of entities from the database.
        /// </summary>
        /// <param name="entityList">List of entities to delete.</param>
        /// <param name="table">The name of the table to delete from.</param>
        /// <param name="identityColumns">Must contain the columns that identify the entity (Case sensitive).</param>
        /// <returns>Returns true if successful, else false. If an entity fails, the rest of the list will not be deleted.</returns>
        public bool DeleteEntities<T>(List<T> entityList, string tableName, string[] identityColumns) where T : class
        {
            return DeleteEntitiesFromList(entityList, tableName, identityColumns);
        }

        /// <summary>
        /// Deletes from a table using conditions.
        /// </summary>
        /// <param name="table">The name of the table to delete from.</param>
        /// <param name="filterDictionary">Search filters. Each entry in the Dictionary contains: 
        /// Key: SQL string, ie. 'id > @id', 'birth = @birth' or 'name like '%br%' '.
        /// Value: The object to replace @. If it's null, it will not be added to parameters there should not be a @ in the key.
        /// Filter may be null if no search criteria!</param>
        /// <param name="affectedRows">The number of deleted rows.</param>
        /// <returns>True if the query succeeded - no matter if anything was deleted.</returns>
        public bool DeleteViaConditions(string tableName, Dictionary<string, object> filterDictionary, out int affectedRows)
        {
            affectedRows = 0;

            try
            {
                SqlCommand cmd = GetSqlCommand();
                string sqlDelete = "DELETE FROM [" + tableName + "] WHERE ";
                if (filterDictionary != null && filterDictionary.Count > 0)
                {
                    int i = 0;
                    foreach (KeyValuePair<string, object> pair in filterDictionary)
                    {

                        sqlDelete += pair.Key + " AND ";
                        if (pair.Value != null)
                        {
                            AddParameterToCmd(pair.Value, cmd, "" + i++);
                        }
                    }
                }
                else
                {
                    sqlDelete = sqlDelete.Substring(0, sqlDelete.Length - 2);
                }
                cmd.CommandText = sqlDelete.Substring(0, sqlDelete.Length - 4);
                affectedRows = cmd.ExecuteNonQuery();
                return true;
            }
            catch (Exception ex)
            {
                string message = "";
                message += "Table name: " + tableName + Environment.NewLine;
                if (filterDictionary != null && filterDictionary.Count != 0)
                {
                    message += "Filter: " + Environment.NewLine;
                    foreach (KeyValuePair<string, object> pair in filterDictionary)
                    {
                        message += "\tKey: " + pair.Key + "\tValue: " + pair.Value + Environment.NewLine;
                    }
                }
                message += "Error message: " + ex.Message + Environment.NewLine;
                message += "Stacktrace: " + ex.StackTrace + Environment.NewLine;

                Debug.WriteLine(message);
                if (ErrorOccurred != null)
                    ErrorOccurred(ex);
                return false;
            }
        }

        private bool DeleteEntitiesFromList<T>(List<T> entityList, string tableName, IEnumerable<string> identityColumns) where T : class
        {
            Type entityType = null;
            try
            {
                if (entityList.Count == 0) // no entities... returns true
                    return true;

                SqlCommand cmd = GetSqlCommand();
                entityType = entityList[0].GetType();

                // - generate the SQL
                cmd.Parameters.Clear();
                cmd.CommandText = "DELETE FROM [" + tableName + "] WHERE ";
                foreach (string idColumn in identityColumns)
                {
                    cmd.CommandText += idColumn + " = @" + idColumn + " AND ";
                }
                cmd.CommandText = cmd.CommandText.Substring(0, cmd.CommandText.Length - 4);

                foreach (T entity in entityList)
                {
                    cmd.Parameters.Clear();
                    foreach (string idColumn in identityColumns) // add parameters for each entitys identity columns
                    {
                        PropertyInfo propertyInfo = entityType.GetProperty(idColumn);
                        object fieldObj = propertyInfo.GetValue(entity, null);
                        AddParameterToCmd(fieldObj, cmd, idColumn);
                    }
                    int affectedRows = cmd.ExecuteNonQuery();
                    if (affectedRows == 0) // error - nothing deleted
                        return false;
                }
            }
            catch (Exception exception)
            {
                string errorMessage = GenerateErrorMessage<T>(entityList.Count > 0 ? entityList[0].GetType() : null, tableName, exception.Message,
                  exception.StackTrace);

                Debug.WriteLine(errorMessage);
                ErrorOccurred?.Invoke(exception);
                return false;
            }
            return true;
        }

        #endregion Delete


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

        private string GenerateErrorMessage<T>(Type entityType, string table, string exceptionMessage, string stacktrace) where T : class
        {
            string message = "";
            message += "Entity type: " + entityType ?? "NULL" + Environment.NewLine;

            message += "Table name: " + table + Environment.NewLine;
            message += "Save type: " + "DELETE" + Environment.NewLine;
            message += "Error message: " + exceptionMessage + Environment.NewLine;
            message += "Stacktrace: " + stacktrace + Environment.NewLine;

            return message;
        }

        private SqlCommand GetSqlCommand()
        {
            SqlCommand sqlCommand;
            if (sqlTransaction != null && sqlTransaction.Connection != null)
                sqlCommand = new SqlCommand("", SqlConnection, sqlTransaction);
            else
                sqlCommand = new SqlCommand("", SqlConnection);

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

                    SqlConnection = new SqlConnection(reconnectSqlConnectionString);
                    SqlConnection.Open();
                    sqlCommand.Connection = SqlConnection;
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

        public Dictionary<string, string> GetTableColumns(SqlDataReader reader)
        {
            Dictionary<string, string> tableColumns = new Dictionary<string, string>();
            DataTable dataTable = reader.GetSchemaTable();
            // Put the column names in list, to make them easier to work with
            if (dataTable != null)
            {
                foreach (DataRow row in dataTable.Rows)
                {
                    tableColumns[(row["ColumnName"] + "").ToLower()] = "" + row["DataType"];
                }
            }
            return tableColumns;
        }

        private void AddParameterToCmd(object fieldObj, SqlCommand cmd, string idColumn)
        {
            cmd.Parameters.AddWithValue("@" + idColumn, fieldObj ?? DBNull.Value);
        }

        public bool CreateTableClone(string sourceTableName, string newName, Type entityType)
        {
            SQLTableCreator creator = new SQLTableCreator(SqlConnection, sqlTransaction);
            SqlCommand cmd = GetSqlCommand();

            cmd.CommandText = "SELECT TOP 1 * FROM [" + sourceTableName + "]";
            SqlDataReader reader = cmd.ExecuteReader();
            DataTable dataTable = reader.GetSchemaTable();
            reader.Close();
            cmd.Dispose();
            creator.Create(dataTable, newName, entityType);
            return true;
        }
        #endregion Helper Methods
    }
}
