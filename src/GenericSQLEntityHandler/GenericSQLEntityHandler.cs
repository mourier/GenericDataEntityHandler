using System;
using System.Collections.Generic;
using System.Data.SqlClient;


namespace GenericSQLEntityHandler
{
    public  class GenericSQLEntityHandler
    {
        #region Private Members

        private bool useSqlTransaction = false;

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
    }
}
