// Copyright (c) Microsoft Corporation.  All rights reserved.

using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;

using System.Data;
using System.Data.SqlClient;
using FirebirdSql.Data.FirebirdClient;


namespace SyncApplication
{
    static class OfflineApp
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main(string[] args)
        {
			string firebirdConnectionString;
			if (args.Length != 1)
			{
				Console.WriteLine("usage: SyncApplication.exe FirebirdConnectionString");
				FbConnectionStringBuilder csb = new FbConnectionStringBuilder();
				csb.DataSource = "localhost";
				csb.Port = 3051;
				csb.Dialect = 3;
				csb.Charset = "NONE";
				csb.Role = "RDB$ADMIN";
				csb.UserID = "SYSDBA";
				csb.Password = "b4u2l7";
				csb.Database = @"c:\temp\peer3.fdb";
				csb.ServerType = FbServerType.Default;

				firebirdConnectionString = csb.ConnectionString;
			}
			else
			{
				firebirdConnectionString = args[0];
			}
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.Run(new CESharingForm(firebirdConnectionString));              
        }
    }
}