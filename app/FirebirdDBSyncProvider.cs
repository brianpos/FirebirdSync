using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Microsoft.Synchronization.Data;
using FirebirdSql.Data.FirebirdClient;

namespace SyncApplication
{
    /// <summary>
    /// Derived DbSyncProvider for Firebird.
    /// </summary>
    public class FirebirdDbSyncProvider : DbSyncProvider
    {
        public FirebirdDbSyncProvider()
        {
            // We need to modify this particular column name because the default is longer than Firebird allows.
            this.ScopeForgottenKnowledgeColName = "scope_forgotten_knowledge";

			for (int i = 0; i < SyncUtils.SyncAdapterTables.Length; i++)
			{
				//Add each table as a DbSyncAdapter to the provider
				DbSyncAdapter adapter = new DbSyncAdapter(SyncUtils.SyncAdapterTables[i]);
				adapter.RowIdColumns.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i]);

				// select incremental changes command
				FbCommand chgsOrdersCmd = new FbCommand();
				chgsOrdersCmd.CommandType = CommandType.StoredProcedure;
				chgsOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_selectchanges";
				chgsOrdersCmd.Parameters.Add(DbSyncSession.SyncMinTimestamp, FbDbType.Integer); // TODO: timestamp?
				chgsOrdersCmd.Parameters.Add(DbSyncSession.SyncMetadataOnly, FbDbType.Integer);
				chgsOrdersCmd.Parameters.Add(DbSyncSession.SyncScopeLocalId, FbDbType.Integer);
				// chgsOrdersCmd.Parameters.Add("sync_changes", FbDbType.Cursor).Direction = ParameterDirection.Output;
				adapter.SelectIncrementalChangesCommand = chgsOrdersCmd;


				// insert row command
				FbCommand insOrdersCmd = new FbCommand();
				insOrdersCmd.CommandType = CommandType.StoredProcedure;
				insOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_applyinsert";
				insOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				if (SyncUtils.SyncAdapterTables[i] == "orders")
				{
					insOrdersCmd.Parameters.Add("order_date", FbDbType.Date);
				}
				else
				{
					insOrdersCmd.Parameters.Add("product", FbDbType.VarChar, 100);
					insOrdersCmd.Parameters.Add("quantity", FbDbType.Integer);
					insOrdersCmd.Parameters.Add("order_id", FbDbType.Integer);
				}
				insOrdersCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
				adapter.InsertCommand = insOrdersCmd;


				// update row command
				FbCommand updOrdersCmd = new FbCommand();
				updOrdersCmd.CommandType = CommandType.StoredProcedure;
				updOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_applyupdate";
				if (SyncUtils.SyncAdapterTables[i] == "order_details")
					updOrdersCmd.Parameters.Add("order_id", FbDbType.Integer);
				updOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				if (SyncUtils.SyncAdapterTables[i] == "orders")
				{
					updOrdersCmd.Parameters.Add("order_date", FbDbType.Date);
				}
				else
				{
					updOrdersCmd.Parameters.Add("quantity", FbDbType.Integer);
					updOrdersCmd.Parameters.Add("product", FbDbType.VarChar, 100);
				}
				updOrdersCmd.Parameters.Add(DbSyncSession.SyncForceWrite, FbDbType.Integer);
				updOrdersCmd.Parameters.Add(DbSyncSession.SyncMinTimestamp, FbDbType.Integer);
				updOrdersCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
				adapter.UpdateCommand = updOrdersCmd;


				// delete row command
				FbCommand delOrdersCmd = new FbCommand();
				delOrdersCmd.CommandType = CommandType.StoredProcedure;
				delOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_applydelete";
				delOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				delOrdersCmd.Parameters.Add(DbSyncSession.SyncMinTimestamp, FbDbType.Integer);
				delOrdersCmd.Parameters.Add(DbSyncSession.SyncForceWrite, FbDbType.Integer);
				delOrdersCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
				adapter.DeleteCommand = delOrdersCmd;

				// get row command
				FbCommand selRowOrdersCmd = new FbCommand();
				selRowOrdersCmd.CommandType = CommandType.StoredProcedure;
				selRowOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_selectrow";
				selRowOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				selRowOrdersCmd.Parameters.Add(DbSyncSession.SyncScopeLocalId, FbDbType.Integer);
				// selRowOrdersCmd.Parameters.Add("selectedRow", FbDbType.Cursor).Direction = ParameterDirection.Output;
				adapter.SelectRowCommand = selRowOrdersCmd;


				// insert row metadata command
				FbCommand insMetadataOrdersCmd = new FbCommand();
				insMetadataOrdersCmd.CommandType = CommandType.StoredProcedure;
				insMetadataOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_insert_md";
				insMetadataOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncScopeLocalId, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowTimestamp, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCreatePeerKey, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCreatePeerTimestamp, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncUpdatePeerKey, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncUpdatePeerTimestamp, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowIsTombstone, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCheckConcurrency, FbDbType.Integer);
				insMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
				adapter.InsertMetadataCommand = insMetadataOrdersCmd;


				// update row metadata command       
				FbCommand updMetadataOrdersCmd = new FbCommand();
				updMetadataOrdersCmd.CommandType = CommandType.StoredProcedure;
				updMetadataOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_update_md";
				updMetadataOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncScopeLocalId, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowTimestamp, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCreatePeerKey, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCreatePeerTimestamp, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncUpdatePeerKey, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncUpdatePeerTimestamp, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowIsTombstone, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCheckConcurrency, FbDbType.Integer);
				updMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
				adapter.UpdateMetadataCommand = updMetadataOrdersCmd;

				// delete row metadata command
				FbCommand delMetadataOrdersCmd = new FbCommand();
				delMetadataOrdersCmd.CommandType = CommandType.StoredProcedure;
				delMetadataOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_delete_md";
				delMetadataOrdersCmd.Parameters.Add(SyncUtils.SyncAdapterTablePrimaryKeys[i], FbDbType.Integer);
				delMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncCheckConcurrency, FbDbType.Integer);
				delMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowTimestamp, FbDbType.Integer);
				delMetadataOrdersCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
				adapter.DeleteMetadataCommand = delMetadataOrdersCmd;


				// get tombstones for cleanup
				FbCommand selTombstonesOrdersCmd = new FbCommand();
				selTombstonesOrdersCmd.CommandType = CommandType.StoredProcedure;
				selTombstonesOrdersCmd.CommandText = "sp_" + SyncUtils.SyncAdapterTables[i] + "_select_ts";
				selTombstonesOrdersCmd.Parameters.Add("tombstone_aging_in_hours", FbDbType.Integer).Value = SyncUtils.TombstoneAgingInHours;
				selTombstonesOrdersCmd.Parameters.Add("sync_scope_local_id", FbDbType.Integer);
				adapter.SelectMetadataForCleanupCommand = selTombstonesOrdersCmd;

				SyncAdapters.Add(adapter);
			}

			// 2. Setup provider wide commands
			// There are few commands on the provider itself and not on a table sync adapter:
			// SelectNewTimestampCommand: Returns the new high watermark for current sync
			// SelectScopeInfoCommand: Returns sync knowledge, cleanup knowledge and scope version (timestamp)
			// UpdateScopeInfoCommand: Sets the new values for sync knowledge and cleanup knowledge             
			//

			FbCommand anchorCmd = new FbCommand();
			anchorCmd.CommandType = CommandType.StoredProcedure;
			anchorCmd.CommandText = "SP_GET_TIMESTAMP";  // for SQL Server 2005 SP2, use "min_active_rowversion() - 1"
			anchorCmd.Parameters.Add(DbSyncSession.SyncNewTimestamp, FbDbType.Integer).Direction = ParameterDirection.Output; // TODO: Should this be BigInt

			SelectNewTimestampCommand = anchorCmd;

			// 
			// Select local replica info
			//
			RecreateSelectScope();

			// 
			// Update local replica info
			//
			FbCommand updReplicaInfoCmd = new FbCommand();
			updReplicaInfoCmd.CommandType = CommandType.StoredProcedure;
			updReplicaInfoCmd.CommandText = "SP_UPDATE_SCOPE_INFO";
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncScopeKnowledge, FbDbType.Binary, 10000);
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncScopeId, FbDbType.Guid);
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncScopeCleanupKnowledge, FbDbType.Binary, 10000);
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncScopeName, FbDbType.VarChar, 100);
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncCheckConcurrency, FbDbType.Integer);
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncScopeTimestamp, FbDbType.Integer);
			updReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
			UpdateScopeInfoCommand = updReplicaInfoCmd;

			// 
			// Select overlapping scopes 
			//
			// get tombstones for cleanup
			FbCommand overlappingScopesCmd = new FbCommand();
			overlappingScopesCmd.CommandType = CommandType.StoredProcedure;
			overlappingScopesCmd.CommandText = "sp_select_shared_scopes";
			overlappingScopesCmd.Parameters.Add(DbSyncSession.SyncScopeName, FbDbType.VarChar, 100);
			SelectOverlappingScopesCommand = overlappingScopesCmd;

			// 
			// Update table cleanup info
			//
			FbCommand updScopeCleanupInfoCmd = new FbCommand();
			updScopeCleanupInfoCmd.CommandType = CommandType.Text;
			updScopeCleanupInfoCmd.CommandText = "update  scope_info set " +
											"scope_cleanup_timestamp = @" + DbSyncSession.SyncScopeCleanupTimestamp + " " +
											"where scope_name = @" + DbSyncSession.SyncScopeName + " and " +
											"(scope_cleanup_timestamp is null or scope_cleanup_timestamp <  @" + DbSyncSession.SyncScopeCleanupTimestamp + ");" +
											"@" + DbSyncSession.SyncRowCount + " = ROW_COUNT;";
			updScopeCleanupInfoCmd.Parameters.Add(DbSyncSession.SyncScopeCleanupTimestamp, FbDbType.Integer);
			updScopeCleanupInfoCmd.Parameters.Add(DbSyncSession.SyncScopeName, FbDbType.VarChar, 100);
			updScopeCleanupInfoCmd.Parameters.Add(DbSyncSession.SyncRowCount, FbDbType.Integer).Direction = ParameterDirection.Output;
			UpdateScopeCleanupTimestampCommand = updScopeCleanupInfoCmd;
		}

        /// <summary>
        /// The IsolationLevel is ReadCommitted, however executing "set transaction read only" guarantees transaction-level
        /// read consistency. 
        /// </summary>
        /// <returns></returns>
        protected override IDbTransaction CreateEnumerationTransaction()
        {
            FbTransaction trans = (FbTransaction)this.Connection.BeginTransaction();            
			// TODO: Review this commented out section was required for Oracle, not sure about here for Firebird
            // new FbCommand("set transaction read only", (FbConnection)this.Connection, trans).ExecuteNonQuery();
            return trans; 
        }

		public override void GetSyncBatchParameters(out uint batchSize, out Microsoft.Synchronization.SyncKnowledge knowledge)
		{
			RecreateSelectScope();
			base.GetSyncBatchParameters(out batchSize, out knowledge);
		}

		public override Microsoft.Synchronization.ChangeBatch GetChangeBatch(uint batchSize, Microsoft.Synchronization.SyncKnowledge destinationKnowledge, out object changeDataRetriever)
		{
			RecreateSelectScope();
			return base.GetChangeBatch(batchSize, destinationKnowledge, out changeDataRetriever);
		}

		public override void ProcessChangeBatch(Microsoft.Synchronization.ConflictResolutionPolicy resolutionPolicy, Microsoft.Synchronization.ChangeBatch sourceChanges, object changeDataRetriever, Microsoft.Synchronization.SyncCallbacks syncCallbacks, Microsoft.Synchronization.SyncSessionStatistics sessionStatistics)
		{
			RecreateSelectScope();
			base.ProcessChangeBatch(resolutionPolicy, sourceChanges, changeDataRetriever, syncCallbacks, sessionStatistics);
		}

		private void RecreateSelectScope()
		{
			// This is required as during the GetScope Calls that occur in the framework the Dispose is called on this command which clears
			// the command text, and thus it no longer works!
			if (SelectScopeInfoCommand != null && !string.IsNullOrEmpty(SelectScopeInfoCommand.CommandText))
				return;
			FbCommand selReplicaInfoCmd = new FbCommand();

			selReplicaInfoCmd.CommandType = CommandType.Text;
			selReplicaInfoCmd.CommandText = "select " +
											"scope_id, " +
											"scope_local_id, " +
											"scope_sync_knowledge, " +
											"scope_forgotten_knowledge, " +
											"scope_cleanup_timestamp, " +
											"scope_timestamp, " +
											"0 as scope_restore_count " +
											"from scope_info " +
											"where scope_name = @" + DbSyncSession.SyncScopeName;
			selReplicaInfoCmd.Parameters.Clear();
			selReplicaInfoCmd.Parameters.Add(DbSyncSession.SyncScopeName, FbDbType.VarChar).Direction = ParameterDirection.Input;
			SelectScopeInfoCommand = selReplicaInfoCmd;
		}
    }
}
