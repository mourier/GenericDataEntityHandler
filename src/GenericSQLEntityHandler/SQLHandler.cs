using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GenericSQLEntityHandler
{
    public static class SQLHandler
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
        #endregion Save Methods
    }
}
