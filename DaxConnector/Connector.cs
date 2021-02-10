using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace DaxConnector
{
    public class Connector
    {
        private string _connectionString;

        public Connector(string connString)
        {
            _connectionString = connString;

        }

        public async Task<List<KeyValuePair<string, string>[]>> RunQueryAsync(string query)
        {
            Task<DataTable> results = ExecuteDaxQueryAsync(query);

            List<KeyValuePair<string, string>[]> rows = new List<KeyValuePair<string, string>[]>();

            await results;

            foreach (DataRow row in results.Result.Rows)
            {
                KeyValuePair<string, string>[] r = new KeyValuePair<string, string>[results.Result.Columns.Count];

                for (int i =0; i<results.Result.Columns.Count; i++)
                {
                    r[i] = new KeyValuePair<string, string>(results.Result.Columns[i].ColumnName, row[i].ToString());
                }
                rows.Add(r);
            }

            return rows;
        }

        private async Task<DataTable> ExecuteDaxQueryAsync(string query)
        {
            var tabularResults = new DataTable();

            using (var connection = new AdomdConnection(_connectionString))
            {
                connection.Open();

                var currentDataAdapter = new AdomdDataAdapter(query, connection);
                currentDataAdapter.Fill(tabularResults);
            }

            return tabularResults;
        }
    }
}
