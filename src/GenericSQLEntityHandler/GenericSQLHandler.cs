using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GenericSQLEntityHandler
{
    public static class GenericSQLHandler
    {
        #region Save Methods
        /// <summary>
        /// Save a single entity to the database
        /// </summary>
        /// <param name="connString">connection string</param>
        /// <param name="entity">entity that needs to be committed to the database</param>
        /// <param name="keys"></param>
        /// <param name="identity">coloumn in database the represents the id</param>
        /// <param name="tableName">name of sqltable - if null, will use entitytype name as tablename</param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>true or false for success status</returns>
        public static bool Save<T>(string connString, T entity, string[] keys = null, string identity = "Id", string tableName = null) where T : class
        {
            if (keys == null || keys.Length == 0)
                keys = new[] { identity };

            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                return entityDataHandler.SaveEntity(entity, tableName ?? typeof(T).Name, keys, SaveType.InsertOrUpdate, identity);
            }
        }


        /// <summary>
        /// Save a list of entities to the database
        /// </summary>
        /// <param name="connString">connection string</param>
        /// <param name="entities">list of entities that needs to be committed to the database</param>
        /// <param name="keys"></param>
        /// <param name="identity">coloumn in database the represents the id</param>
        /// <param name="tableName">name of sqltable - if null, will use entitytype name as tablename</param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>true or false for success status</returns>
        public static bool Save<T>(string connString, List<T> entities, string[] keys = null, string identity = "Id", string tableName = null) where T : class
        {
            if (keys == null || keys.Length == 0)
                keys = new[] { identity };

            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                return entityDataHandler.SaveEntities(entities, tableName ?? typeof(T).Name, keys, SaveType.InsertOrUpdate, identity);
            }
        }
        #endregion Save Methods

        #region Load Methods

        /// <summary>
        /// Load single entity from database.
        /// </summary>
        /// <param name="connString">connection string to database</param>
        /// <param name="keyValue"></param>
        /// <param name="identity">coloumn in database the represents the id</param>
        /// <param name="tableName">name of sqltable - if null, will use entitytype name as tablename</param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>found entity of the specified type or null if nothing is found or caused an exception</returns>
        public static T Load<T>(string connString, object keyValue, string identity = "Id", string tableName = null) where T : class
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                try
                {
                    connection.Open();
                    Dictionary<string, object> filter = new Dictionary<string, object>();
                    filter[identity += "=@0"] = keyValue;
                    GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                    return entityDataHandler.LoadSingleEntity<T>(tableName ?? typeof(T).Name, filter);
                }
                catch (SqlException ex)
                {
                    Debug.WriteLine(ex.ToString());
                    return null;
                }
            }
        }

        /// <summary>
        /// Load single entity from database.
        /// </summary>
        /// <param name="connString">connection string to database</param>
        /// <param name="filter"></param>
        /// <param name="identity">coloumn in database the represents the id</param>
        /// <param name="tableName">name of sqltable - if null, will use entitytype name as tablename</param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>found entity of the specified type or null if nothing is found or caused an exception</returns>
        public static T Load<T>(string connString, Dictionary<string, object> filter = null, string identity = "Id", string tableName = null) where T : class
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                try
                {
                    connection.Open();
                    GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                    return entityDataHandler.LoadSingleEntity<T>(tableName ?? typeof(T).Name, filter);
                }
                catch (SqlException ex)
                {
                    Debug.WriteLine(ex.ToString());
                    return null;
                }
            }
        }

        /// <summary>
        /// Get list of entities from database
        /// </summary>
        /// <param name="connString">connection string to database</param>
        /// <param name="tableName">name of sqltable - if null, will use entitytype name as tablename</param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>list of found entities - null if none are found or caused an exception</returns>
        public static List<T> LoadList<T>(string connString, string tableName = null) where T : class
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                    connection.Open();
                    return entityDataHandler.LoadEntities<T>(tableName ?? typeof(T).Name, new Dictionary<string, object>());
                }
            }
            catch (SqlException ex)
            {
                Debug.Write(ex.ToString());
                return null;
            }
        }

        /// <summary>
        /// Get list of enities from database, based on filter
        /// </summary>
        /// <typeparam name="T">entity type</typeparam>
        /// <param name="connString">connectionstring</param>
        /// <param name="filter">filter made in [string key, object value]</param>
        /// <param name="tableName">name of table in db - if null uses entity name</param>
        /// <returns></returns>
        public static List<T> LoadList<T>(string connString, Dictionary<string, object> filter, string tableName = null) where T : class
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connString))
                {
                    connection.Open();

                    GenericSQLEntityHandler handler = new GenericSQLEntityHandler(connection);

                    return handler.LoadEntities<T>(tableName ?? typeof(T).Name, filter ?? new Dictionary<string, object>());

                }
            }
            catch (SqlException ex)
            {

                Debug.WriteLine(ex.ToString());
                return null;
            }
        }


        /// <summary>
        /// Get list of entities from database by query
        /// </summary>
        /// <param name="connString">connection string</param>
        /// <param name="query">sql query</param>
        /// <param name="keyValue"></param>
        /// <param name="identity">coloumn in database the represents the id</param>
        /// <param name="orderBy">how to order / sort the results</param>
        /// <typeparam name="T">enitity type</typeparam>
        /// <returns>list of specified entities if found</returns>
        public static List<T> LoadListByQuery<T>(string connString, string query, object keyValue, string identity = "Id", string orderBy = "") where T : class
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                try
                {
                    connection.Open();
                    Dictionary<string, object> filter = new Dictionary<string, object>();
                    if (keyValue != null)
                        filter[identity += "=@0"] = keyValue;
                    GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                    return entityDataHandler.LoadEntitiesByQuery<T>(query, filter, orderBy);
                }
                catch (SqlException ex)
                {
                    Debug.WriteLine(ex.ToString());
                    return null;
                }
            }
        }

        /// <summary>
        /// Get list of entities from database by query
        /// </summary>
        /// <param name="connString">connection string</param>
        /// <param name="query">sql query</param>
        /// <param name="filter"></param>
        /// <param name="orderBy">how to order / sort the results</param>
        /// <typeparam name="T">enitity type</typeparam>
        /// <returns>list of specified entities if found</returns>
        public static List<T> LoadListByQuery<T>(string connString, string query, Dictionary<string, object> filter, string orderBy = "") where T : class
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                try
                {
                    connection.Open();
                    GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                    return entityDataHandler.LoadEntitiesByQuery<T>(query, filter, orderBy);
                }
                catch (SqlException ex)
                {
                    Debug.WriteLine(ex.ToString());
                    return null;
                }
            }
        }


        /// <summary>
        /// Get a single antity from database, by custom query
        /// </summary>
        /// <param name="connString">connection string</param>
        /// <param name="query">sql query</param>
        /// <param name="keyValue"></param>
        /// <param name="identity">coloumn in database the represents the id</param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>single entity found be query</returns>
        public static T LoadByQuery<T>(string connString, string query, object keyValue, string identity = "Id") where T : class
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                Dictionary<string, object> filter = new Dictionary<string, object>();

                if (keyValue != null)
                    filter[identity += "=@0"] = keyValue;
                GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                var result = entityDataHandler.LoadEntitiesByQuery<T>(query, filter, "Id");

                if (result != null && result.Count > 0)
                    return result[0];
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Get a single antity from database, by custom query
        /// </summary>
        /// <param name="connString">connection string</param>
        /// <param name="query">sql query</param>
        /// <param name="filter"></param>
        /// <typeparam name="T">entity type</typeparam>
        /// <returns>single entity found be query</returns>
        public static T LoadByQuery<T>(string connString, string query, Dictionary<string, object> filter) where T : class
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();

                GenericSQLEntityHandler entityDataHandler = new GenericSQLEntityHandler(connection);
                var result = entityDataHandler.LoadEntitiesByQuery<T>(query, filter, "Id");

                if (result != null && result.Count > 0)
                    return result[0];
                else
                {
                    return null;
                }
            }
        }
        #endregion Load
    }
}
