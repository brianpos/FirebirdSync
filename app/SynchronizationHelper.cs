using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Synchronization.Data;
using System.Data.SqlClient;
using System.Data;
using Microsoft.Synchronization.Data.SqlServerCe;
using System.Data.SqlServerCe;
using System.IO;
using System.Windows.Forms;
using Microsoft.Synchronization;
using FirebirdSql.Data.FirebirdClient;

namespace SyncApplication
{
    public class SynchronizationHelper
    {
        ProgressForm progressForm;
        CESharingForm ceSharingForm;

        public SynchronizationHelper(CESharingForm ceSharingForm)
        {
            this.ceSharingForm = ceSharingForm;
        }

        /// <summary>
        /// Utility function that will create a SyncOrchestrator and synchronize the two passed in providers
        /// </summary>
        /// <param name="localProvider">Local store provider</param>
        /// <param name="remoteProvider">Remote store provider</param>
        /// <returns></returns>
        public SyncOperationStatistics SynchronizeProviders(RelationalSyncProvider localProvider, RelationalSyncProvider remoteProvider)
        {
            SyncOrchestrator orchestrator = new SyncOrchestrator();
            orchestrator.LocalProvider = localProvider;
            orchestrator.RemoteProvider = remoteProvider;
			orchestrator.Direction = SyncDirectionOrder.UploadAndDownload;

            progressForm = new ProgressForm();
            progressForm.Show();

            //Check to see if any provider is a SqlCe provider and if it needs schema
            CheckIfProviderNeedsSchema(localProvider as SqlCeSyncProvider);
            CheckIfProviderNeedsSchema(remoteProvider as SqlCeSyncProvider);

            SyncOperationStatistics stats = orchestrator.Synchronize();
            progressForm.ShowStatistics(stats);
            progressForm.EnableClose();
            return stats;
        }

        /// <summary>
        /// Check to see if the passed in CE provider needs Schema from server.
        /// If it does, we'll need to do some extra manipulating of the DbSyncScopeDescription given from the Firebird provider, before
        /// we apply it to CE.
        /// </summary>
        /// <param name="localProvider"></param>
        private void CheckIfProviderNeedsSchema(SqlCeSyncProvider localProvider)
        {
            if (localProvider != null)
            {
				SqlCeConnection ceConn = (SqlCeConnection)localProvider.Connection;
				SqlCeSyncScopeProvisioning ceConfig = new SqlCeSyncScopeProvisioning(ceConn);
                string scopeName = localProvider.ScopeName;
                if (!ceConfig.ScopeExists(scopeName))
                {
                    DbSyncScopeDescription scopeDesc = ((DbSyncProvider)ceSharingForm.providersCollection["Server"]).GetScopeDescription();

                    // We have to manually fix up the Firebird types on the DbSyncColumnDescriptions. Alternatively we could just construct the DbScopeDescription
                    // from scratch.
                    for (int i = 0; i < scopeDesc.Tables.Count; i++)
                    {
                        for (int j = 0; j < scopeDesc.Tables[i].Columns.Count; j++)
                        {
                            // When grabbing the Firebird schema table the type field gets set to the index of that type in the FbDbType enumeration.
                            // We will have to change it to the actual name instead of the index.
                            scopeDesc.Tables[i].Columns[j].Type = ((FbDbType)Int32.Parse(scopeDesc.Tables[i].Columns[j].Type)).ToString().ToLower();
                            
                            // We also have to convert number to a decimal for CE
                            if (scopeDesc.Tables[i].Columns[j].Type == "number")
                            {
                                scopeDesc.Tables[i].Columns[j].Type = "decimal";
                            }

                            // Because the DbSyncColumnDescription only had a number for the Type (which it does not understand), it could not 
                            // auto-fill in the required attributes for that field type.  So in the case of a string field, we have to manually 
                            // set the length ourselves.  If we wanted to set scale and precision for the previous decimal field we need to do the same.
							if (scopeDesc.Tables[i].Columns[j].Type == "nvarchar" || scopeDesc.Tables[i].Columns[j].Type == "varchar")
                            {
                                scopeDesc.Tables[i].Columns[j].Size = "100";
                            }
                        }
                    }

                    ceConfig.PopulateFromScopeDescription(scopeDesc);
                    ceConfig.Apply();
                }
            }
        }

        /// <summary>
        /// Configure the Firebird DbSyncprovider. Usual configuration similar to OCS V2 samples.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        public FirebirdDbSyncProvider ConfigureDBSyncProvider(string connectionString)
        {
            FirebirdDbSyncProvider provider = new FirebirdDbSyncProvider();
            provider.ScopeName = SyncUtils.ScopeName;
            provider.Connection = new FbConnection();
            provider.Connection.ConnectionString = connectionString;

            //Register the BatchSpooled and BatchApplied events. These are fired when a provider is either enumerating or applying changes in batches.
            provider.BatchApplied += new EventHandler<DbBatchAppliedEventArgs>(provider_BatchApplied);
            provider.BatchSpooled += new EventHandler<DbBatchSpooledEventArgs>(provider_BatchSpooled);

            return provider;
        }

        /// <summary>
        /// Utility function that configures a CE provider
        /// </summary>
        /// <param name="sqlCeConnection"></param>
        /// <returns></returns>
        public SqlCeSyncProvider ConfigureCESyncProvider(SqlCeConnection sqlCeConnection)
        {
            SqlCeSyncProvider provider = new SqlCeSyncProvider();
            //Set the scope name
            provider.ScopeName = SyncUtils.ScopeName;
            //Set the connection.
            provider.Connection = sqlCeConnection;

            //Register event handlers

            //1. Register the BeginSnapshotInitialization event handler. Called when a CE peer pointing to an uninitialized
            // snapshot database is about to being initialization.
            provider.BeginSnapshotInitialization += new EventHandler<DbBeginSnapshotInitializationEventArgs>(provider_BeginSnapshotInitialization);

            //2. Register the EndSnapshotInitialization event handler. Called when a CE peer pointing to an uninitialized
            // snapshot database has been initialized for the given scope.
            provider.EndSnapshotInitialization += new EventHandler<DbEndSnapshotInitializationEventArgs>(provider_EndSnapshotInitialization);

            //3. Register the BatchSpooled and BatchApplied events. These are fired when a provider is either enumerating or applying changes in batches.
            provider.BatchApplied += new EventHandler<DbBatchAppliedEventArgs>(provider_BatchApplied);
            provider.BatchSpooled += new EventHandler<DbBatchSpooledEventArgs>(provider_BatchSpooled);

            //Thats it. We are done configuring the CE provider.
            return provider;
        }

        /// <summary>
        /// Called whenever an enumerating provider spools a batch file to the disk
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void provider_BatchSpooled(object sender, DbBatchSpooledEventArgs e)
        {
            this.progressForm.listSyncProgress.Items.Add("BatchSpooled event fired: Details");
            this.progressForm.listSyncProgress.Items.Add("\tSource Database :" + ((RelationalSyncProvider)sender).Connection.Database);
            this.progressForm.listSyncProgress.Items.Add("\tBatch Name      :" + e.BatchFileName);
            this.progressForm.listSyncProgress.Items.Add("\tBatch Size      :" + e.DataCacheSize);
            this.progressForm.listSyncProgress.Items.Add("\tBatch Number    :" + e.CurrentBatchNumber);
            this.progressForm.listSyncProgress.Items.Add("\tTotal Batches   :" + e.TotalBatchesSpooled);
            this.progressForm.listSyncProgress.Items.Add("\tBatch Watermark :" + ReadTableWatermarks(e.CurrentBatchTableWatermarks));
        }

        /// <summary>
        /// Calls when the destination provider successfully applies a batch file.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void provider_BatchApplied(object sender, DbBatchAppliedEventArgs e)
        {
            this.progressForm.listSyncProgress.Items.Add("BatchApplied event fired: Details");
            this.progressForm.listSyncProgress.Items.Add("\tDestination Database   :" + ((RelationalSyncProvider)sender).Connection.Database);
            this.progressForm.listSyncProgress.Items.Add("\tBatch Number           :" + e.CurrentBatchNumber);
            this.progressForm.listSyncProgress.Items.Add("\tTotal Batches To Apply :" + e.TotalBatchesToApply);
        }

        /// <summary>
        /// Reads the watermarks for each table from the batch spooled event. Denotes the max tickcount for each table in each batch
        /// </summary>
        /// <param name="dictionary">Watermark dictionary retrieved from DbBatchSpooledEventArgs</param>
        /// <returns>String</returns>
        private string ReadTableWatermarks(Dictionary<string, ulong> dictionary)
        {
            StringBuilder builder = new StringBuilder();
            Dictionary<string, ulong> dictionaryClone = new Dictionary<string, ulong>(dictionary);
            foreach (KeyValuePair<string, ulong> kvp in dictionaryClone)
            {
                builder.Append(kvp.Key).Append(":").Append(kvp.Value).Append(",");
            }
            return builder.ToString();
        }

        /// <summary>
        /// Snapshot intialization process completed. Database is now ready for sync with other existing peers in topology
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void provider_EndSnapshotInitialization(object sender, DbEndSnapshotInitializationEventArgs e)
        {
            this.progressForm.listSyncProgress.Items.Add("EndSnapshotInitialization Event fired.");
            this.progressForm.ShowSnapshotInitializationStatistics(e.InitializationStatistics, e.TableInitializationStatistics);
            this.progressForm.listSyncProgress.Items.Add("Snapshot Initialization Process Completed.....");
        }

        /// <summary>
        /// CE provider detected that the database was imported from a snapshot from another peer. Snapshot initialziation about to begin
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void provider_BeginSnapshotInitialization(object sender, DbBeginSnapshotInitializationEventArgs e)
        {
            this.progressForm.listSyncProgress.Items.Add("Snapshot Initialization Process Started.....");
            this.progressForm.listSyncProgress.Items.Add(
                string.Format("BeginSnapshotInitialization Event fired for Scope {0}", e.ScopeName)
                );
        }

        /// <summary>
        /// User called CreateSchema on the CE provider.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        void provider_CreatingSchema(object sender, CreatingSchemaEventArgs e)
        {
            this.progressForm.listSyncProgress.Items.Add("Full Initialization Process Started.....");
            this.progressForm.listSyncProgress.Items.Add(
                string.Format("CreatingSchame Event fired for Database {0}", e.Connection.Database)
                );
        }

        #region Static Helper Functions
        /// <summary>
        /// Static helper function to create an empty CE database
        /// </summary>
        /// <param name="client"></param>
        public static void CheckAndCreateCEDatabase(CEDatabase client)
        {
            if (!File.Exists(client.Location))
            {
                SqlCeEngine engine = new SqlCeEngine(client.Connection.ConnectionString);
                engine.CreateDatabase();
                engine.Dispose();
            }
        }

        #endregion
    }
}
