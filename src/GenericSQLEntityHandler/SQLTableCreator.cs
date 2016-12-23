using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace GenericSQLEntityHandler
{
    public class SQLTableCreator
    {
        #region Private Members
        private SqlConnection connection = null;
        private SqlTransaction transaction = null;
        
        
        #endregion Private Members

        #region Public Members

        public SqlConnection SqlConnection { get; set; }
        public SqlTransaction SqlTransaction { get; set; }
        public string DestinationTableName { get; set; }

        #endregion Public Members


        #region Constructor
        public SQLTableCreator() { }

        public SQLTableCreator(SqlConnection connection) : this(connection, null) { }

        public SQLTableCreator(SqlConnection sqlConnection, SqlTransaction sqlTransaction)
        {
            SqlConnection = sqlConnection;
            SqlTransaction = sqlTransaction;
        }

        #endregion Constructor

        #region Methods
        public bool Create(DataTable schema, int[] primaryKeys, string destinationTableName, Type entityType)
        {
            try
            {

                string sql = GetCreateSql(destinationTableName, schema, primaryKeys, entityType);
                SqlCommand cmd;
                if (transaction != null && transaction.Connection != null)
                    cmd = new SqlCommand(sql, connection, transaction);
                else
                    cmd = new SqlCommand(sql, connection);

                return cmd.ExecuteNonQuery() == 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.ToString());
                throw;
            }
        }


        private string GetCreateSql(string tableName, DataTable schema, int[] primaryKeys, Type entityType)
        {
            string sql = "CREATE TABLE [" + tableName + "] (\n";
            
            // columns
            foreach (DataRow column in schema.Rows)
            {

                if (!(schema.Columns.Contains("IsHidden") && column["IsHidden"] != DBNull.Value && (bool)column["IsHidden"]) && entityType.GetProperty(column["ColumnName"].ToString()) != null)
                {
                    sql += "\t[" + column["ColumnName"] + "] " + SQLGetType(column);
                    if (schema.Columns.Contains("IsIdentity") && column["IsIdentity"] != DBNull.Value && (bool)column["IsIdentity"])
                        sql += " IDENTITY(1,1)";
                    if (schema.Columns.Contains("AllowDBNull") && column["AllowDBNull"] != DBNull.Value && (bool)column["AllowDBNull"] == false)
                        sql += " NOT NULL";
                    sql += ",\n";
                }
            }
            sql = sql.TrimEnd(',', '\n') + "\n";

            // primary keys
            string pk = ", CONSTRAINT PK_" + tableName + " PRIMARY KEY CLUSTERED (";
            bool hasKeys = (primaryKeys != null && primaryKeys.Length > 0);
            if (hasKeys)
            {
                // user defined keys
                foreach (int key in primaryKeys)
                {
                    pk += schema.Rows[key]["ColumnName"] + ", ";
                }
            }
            else
            {
                // check schema for keys
                string keys = string.Join(", ", GetPrimaryKeys(schema));
                pk += keys;
                hasKeys = keys.Length > 0;
            }
            pk = pk.TrimEnd(',', ' ', '\n') + ")\n";

            if (hasKeys)
                sql += pk;

            sql += ")";
            return sql;
        }

        private string[] GetPrimaryKeys(DataTable schema)
        {
            List<string> keys = new List<string>();

            foreach (DataRow column in schema.Rows)
            {
                if (schema.Columns.Contains("IsKey") && column["IsKey"] != DBNull.Value && (bool)column["IsKey"])
                    keys.Add(column["ColumnName"].ToString());
            }

            return keys.ToArray();
        }

        // Overload based on row from schema table 
        private string SQLGetType(DataRow schemaRow)
        {
            string type = schemaRow["DataTypeName"].ToString();

            if (schemaRow["DataType"].ToString() == "System.String" || schemaRow["DataType"].ToString() == "System.Byte[]")
                type += "(" + (int.Parse(schemaRow["ColumnSize"].ToString()) == int.MaxValue ? "MAX" : schemaRow["ColumnSize"].ToString()) + ")";
            return type;
        }


        #endregion Methods





    }
}
