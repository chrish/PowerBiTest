using DaxConnector;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;


namespace DaxTest
{
    [TestClass]
    public class TestTableDefinitions
    {
        string _connectionString;

        public TestTableDefinitions()
        {
            _connectionString = "DataSource=localhost:50484";
        }


        /// <summary>
        /// Check if a PBI splits postcode and location correctly. 
        /// Table has the following structure: 
        /// 
        /// fact_data[Item]
        /// fact_data[Value]
        /// fact_data[Postcode-Location]
        /// fact_data[Firstname]
        /// fact_data[Lastname]
        /// fact_data[Phone]
        /// fact_data[Country]
        /// fact_data[Postcode]
        /// 
        /// Where Postcode is the result of running the following DAX:
        /// left([Postcode-Location], search("-", [Postcode-Location])-1)
        /// 
        /// We want to verify that the result is correct for all cases.
        /// </summary>
        [TestMethod]
        public void TestPostCodeLocationSplit()
        {
            string query = $@"EVALUATE(VALUES('fact_data'))";

            var conn = new Connector(_connectionString);

            var data = conn.RunQueryAsync(query);

            data.Wait();
            
            // List<KeyValuePair<string, string>[]>
            foreach (var row in data.Result)
            {
                var postLoc = (from r in row where r.Key == "fact_data[Postcode-Location]" select r.Value).FirstOrDefault();
                var storedPostCode = (from r in row where r.Key == "fact_data[Postcode]" select r.Value).FirstOrDefault();

                // There are multiple ways to check, but we're lazy using linq above and split below.
                Assert.AreEqual(storedPostCode, postLoc.Split("-")[0]);
            }
        }

        /// <summary>
        /// The model contains dim_countries which is a unique collection of all countries in the Country column of fact_data.
        /// Check that it has a complete collection with no duplicates.
        /// 
        /// The DAX used to generate the dim_countries table is 
        /// dim_countries = distinct(fact_data[Country])
        /// </summary>
        public void TestUniqueCountriesTable()
        {
            string factDataQuery = $@"EVALUATE(VALUES('fact_data'))";
            string dimCountriesQuery = $@"EVALUATE(VALUES('dim_countries'))";

            var conn = new Connector(_connectionString);

            var factData = conn.RunQueryAsync(factDataQuery);
            var dimCountriesData = conn.RunQueryAsync(dimCountriesQuery);

            factData.Wait();
            dimCountriesData.Wait();

            //Get a list of countries from dim_countries first: 
            List<string> dimCountries = new List<string>();

            foreach(var row in dimCountriesData.Result)
            {
                dimCountries.Add(row[0].Value);
            }

            //Get the countries from the fact table:
            List<string> FactTableCountries = new List<string>();
            foreach (var row in factData.Result)
            {
                var ctry= (from r in row where r.Key == "fact_data[Country]" select r.Value).FirstOrDefault();
                FactTableCountries.Add(ctry);
            }

            // We now have two lists containing all the countries in the two tables. 
            // Check for uniqueness:
            List<string> uniqueDimCountries = dimCountries.Distinct().ToList<string>();

            // If the DAX is correct, the count of both should be identical:
            Assert.AreEqual(uniqueDimCountries.Count, dimCountries.Count);


            // Then; check if all countries in fact_data are in dim_countries:
            Assert.IsTrue(!FactTableCountries.Except(dimCountries).Any());
            
            // Just for fun, check that all the entries in dim_countries are also in the fact table:
            Assert.IsTrue(!dimCountries.Except(FactTableCountries).Any());
        }
    }
}
