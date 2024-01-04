using Microsoft.Data.Sqlite;
using System.Text;
using System.Collections.Concurrent;
namespace SP1
{
    class Program
    {
        private static void Main(string[] args)
        {
            /* -------------------------------------------------------------------------- */
            /*                                  DataBase                                  */
            /* -------------------------------------------------------------------------- */
            CreateDatabase();
            FillDatabase();
            ImportWeather();
            SumMonthly();
            DeleteReallyBadRecords();
        }

        // This function initializes and sets up a new SQLite database with specific tables.
        static void CreateDatabase()
        {
            // Initialize a SQLite connection string builder.
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            // Set the data source to 'db.db', which is the name of the SQLite database file.
            connectionStringBuilder.DataSource = "db.db";

            // Using the connection string, establish a connection to the SQLite database.
            using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
            // Open the connection to the database.
            connection.Open();

            // Create and execute SQL commands within this using block.
            using (var command = connection.CreateCommand())
            {
                // Create a 'dane' table if it doesn't exist, with specified columns and data types.
                command.CommandText = @"CREATE TABLE IF NOT EXISTS dane (
                                id INTEGER, 
                                data DATE, 
                                czas DATETIME,
                                pv INTEGER,
                                dpv INTEGER,
                                p INTEGER,
                                efekt DECIMAL)";
                // Execute the SQL command.
                command.ExecuteNonQuery();

                // Create an 'instalacje' table with a primary key and various data types.
                command.CommandText = @"CREATE TABLE IF NOT EXISTS instalacje (
                                id INTEGER PRIMARY KEY, 
                                moc INTEGER,
                                dlugosc FLOAT,
                                szerokosc FLOAT);";
                // Execute the SQL command.
                command.ExecuteNonQuery();

                // Create a 'dane_miesieczne' table with different columns.
                command.CommandText = @"CREATE TABLE IF NOT EXISTS dane_miesieczne (
                                id INTEGER,
                                miesiac STRING, 
                                dpv INTEGER,
                                p INTEGER,
                                efekt DECIMAL)";
                // Execute the SQL command.
                command.ExecuteNonQuery();

                // Create a 'pogoda' table with columns for weather-related data.
                command.CommandText = @"CREATE TABLE IF NOT EXISTS pogoda (
                                id INTEGER,
                                miesiac STRING,
                                naslonecznienie FLOAT,
                                temperatura FLOAT);";
                // Execute the SQL command.
                command.ExecuteNonQuery();
            }
        }
        // This function fills the database with data from various SQL and CSV files.
        static void FillDatabase()
        {
            // Fill the database with data from an SQL file for January 2020.
            Program.FillDataFromSQL("data/dane_2020-01-01.sql");
            // Repeat the process for each month of the year 2020.
            Program.FillDataFromSQL("data/dane_2020-02-01.sql");
            Program.FillDataFromSQL("data/dane_2020-03-01.sql");
            Program.FillDataFromSQL("data/dane_2020-04-01.sql");
            Program.FillDataFromSQL("data/dane_2020-05-01.sql");
            Program.FillDataFromSQL("data/dane_2020-06-01.sql");
            Program.FillDataFromSQL("data/dane_2020-07-01.sql");
            Program.FillDataFromSQL("data/dane_2020-08-01.sql");
            Program.FillDataFromSQL("data/dane_2020-09-01.sql");
            Program.FillDataFromSQL("data/dane_2020-10-01.sql");
            Program.FillDataFromSQL("data/dane_2020-11-01.sql");
            Program.FillDataFromSQL("data/dane_2020-12-01.sql");

            // Additionally, fill the database with data from a CSV file for 'instalacje'.
            Program.FillDataFromCSV("data/instalacje.csv");
        }

        // This function imports weather data for each installation and stores it in the 'pogoda' table.
        static void ImportWeather()
        {
            // Initialize a SQLite connection string builder.
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            // Set the data source to 'db.db', the SQLite database file.
            connectionStringBuilder.DataSource = "db.db";

            // Establish a connection to the SQLite database using the connection string.
            using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
            // Open the connection.
            connection.Open();

            // Define the base URL for the weather API.
            string url = "https://re.jrc.ec.europa.eu/api/v5_2/MRcalc?";

            // Create a list to hold all the threads that will be fetching weather data.
            List<Thread> threads = new List<Thread>();

            // Create a new command to select the ID, latitude, and longitude for each installation.
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"SELECT id, szerokosc, dlugosc FROM instalacje";
                // Execute the command and retrieve the data.
                var reader = command.ExecuteReader();

                // Create a concurrent bag to hold lists of strings.
                // Each list represents weather data for a specific installation.
                ConcurrentBag<List<String>> cb = new ConcurrentBag<List<String>>();

                // Iterate through each row in the result set.
                while (reader.Read())
                {
                    // Construct the full URL for the API call by appending parameters for latitude, longitude, and other details.
                    string url2 = url + "lat=" + reader.GetFloat(1) + "&lon=" + reader.GetFloat(2) + "&outputformat=json&startyear=2020&endyear=2020&avtemp=1&horirrad=1";
                    // Create a new GetFromAPI object, which presumably handles fetching weather data from the API.
                    GetFromAPI get = new GetFromAPI(reader.GetInt32(0), url2, cb);

                    // Start a new thread to fetch weather data asynchronously.
                    Thread t = new Thread(new ThreadStart(get.GetWeatherAsync));
                    t.Start();
                    // Add the thread to the list of threads.
                    threads.Add(t);
                    // Pause for 100 milliseconds before starting the next thread.
                    System.Threading.Thread.Sleep(100);
                }
                // Wait for all threads to complete their tasks.
                for (int i = 0; i < threads.Count; i++)
                {
                    threads[i].Join();
                }
                // Close the data reader.
                reader.Close();

                // Insert the fetched weather data into the 'pogoda' table.
                foreach (List<String> list in cb)
                {
                    using (var command2 = connection.CreateCommand())
                    {
                        command2.CommandText = @"INSERT or REPLACE INTO pogoda (id, miesiac, naslonecznienie, temperatura) VALUES (@id, @miesiac, @naslonecznienie, @temperatura)";
                        command2.Parameters.AddWithValue("@id", list[0]);
                        command2.Parameters.AddWithValue("@miesiac", list[1]);
                        command2.Parameters.AddWithValue("@naslonecznienie", list[2]);
                        command2.Parameters.AddWithValue("@temperatura", list[3]);
                        command2.ExecuteNonQuery();
                    }
                }
            }
        }

        // This function aggregates monthly data from the 'dane' table and stores it in the 'dane_miesieczne' table.
        static void SumMonthly()
        {
            // Initialize a SQLite connection string builder.
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            // Set the data source to 'db.db', the SQLite database file.
            connectionStringBuilder.DataSource = "db.db";

            // Establish a connection to the SQLite database using the connection string.
            using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
            // Open the connection.
            connection.Open();

            // Create a new SQL command to aggregate monthly data.
            using (var command = connection.CreateCommand())
            {
                // This SQL command selects and aggregates data by 'id' and 'miesiac' (month).
                // It calculates the sum of 'dpv', and the average of 'p' and 'efekt' for each month and id.
                command.CommandText = @"SELECT id, strftime('%m', data) as miesiac, SUM(dpv) as dpv, AVG(p) as p, AVG(efekt) as efekt FROM dane GROUP BY id, miesiac";
                // Execute the command and retrieve the aggregated data.
                using (var reader = command.ExecuteReader())
                {
                    // Iterate through each row in the result set.
                    while (reader.Read())
                    {
                        // Create a new command for each row to insert or replace data in 'dane_miesieczne'.
                        using (var command2 = connection.CreateCommand())
                        {
                            // Prepare an SQL INSERT command with parameters for id, miesiac (month), dpv, p, and efekt.
                            command2.CommandText = @"INSERT or REPLACE INTO dane_miesieczne (id, miesiac, dpv, p, efekt) VALUES (@id, @miesiac, @dpv, @p, @efekt)";
                            // Assign values to each parameter from the current row of the reader.
                            command2.Parameters.AddWithValue("@id", reader.GetInt32(0));
                            command2.Parameters.AddWithValue("@miesiac", reader.GetString(1));
                            command2.Parameters.AddWithValue("@dpv", reader.GetInt32(2));
                            command2.Parameters.AddWithValue("@p", reader.GetInt32(3));
                            command2.Parameters.AddWithValue("@efekt", reader.GetDecimal(4));
                            // Execute the SQL command.
                            command2.ExecuteNonQuery();
                        }
                    }
                }
            }
        }

        // This function reads SQL commands from a text file and executes them to fill data into the database.
        static void FillDataFromSQL(string textFile)
        {
            // Initialize a SQLite connection string builder.
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            // Set the data source to 'db.db', which is the name of the SQLite database file.
            connectionStringBuilder.DataSource = "db.db";

            // Using the connection string, establish a connection to the SQLite database.
            using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
            // Open the connection to the database.
            connection.Open();

            // Read the entire text file containing SQL commands.
            string text = File.ReadAllText(textFile);
            // Split the text into lines, each line presumably contains a single SQL command.
            string[] lines = text.Split("\n");

            // Iterate through each line (SQL command).
            for (int i = 0; i < lines.Length; i++)
            {
                // Execute each SQL command.
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = lines[i];
                    command.ExecuteNonQuery();
                }
            }
        }

        // This function reads data from a CSV file and inserts it into the 'instalacje' table in the database.
        static void FillDataFromCSV(string file)
        {
            // Initialize a SQLite connection string builder.
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            // Set the data source to 'db.db', the SQLite database file.
            connectionStringBuilder.DataSource = "db.db";

            // Establish a connection to the SQLite database using the connection string.
            using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
            // Open the connection.
            connection.Open();

            // Read the entire CSV file.
            string text = File.ReadAllText(file);
            // Split the text into lines, with each line representing a row of data in the CSV.
            string[] lines = text.Split("\n");

            // Iterate through each line in the CSV file.
            for (int i = 0; i < lines.Length; i++)
            {
                // Split each line by comma to separate the fields.
                string[] data = lines[i].Split(",");
                // If there are not enough data fields, skip this line.
                if (data.Length < 6) continue;

                // Construct and execute an SQL INSERT command for each line of data.
                using (var command = connection.CreateCommand())
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append("INSERT or REPLACE INTO instalacje (id, moc, dlugosc, szerokosc) VALUES (");
                    sb.Append(data[0]); // id
                    sb.Append(", ");
                    sb.Append(data[1]); // moc
                    sb.Append(", ");
                    sb.Append(data[4]); // dlugosc
                    sb.Append(", ");
                    sb.Append(data[5]); // szerokosc
                    sb.Append(");");

                    command.CommandText = sb.ToString();
                    command.ExecuteNonQuery();
                }
            }
        }

        // This function deletes records from 'dane_miesieczne' that do not meet certain criteria.
        static void DeleteReallyBadRecords()
        {
            // Initialize a SQLite connection string builder.
            var connectionStringBuilder = new SqliteConnectionStringBuilder();
            // Set the data source to 'db.db', the SQLite database file.
            connectionStringBuilder.DataSource = "db.db";

            // Establish a connection to the SQLite database using the connection string.
            using var connection = new SqliteConnection(connectionStringBuilder.ConnectionString);
            // Open the connection.
            connection.Open();

            // Start a transaction to ensure the deletion process is atomic.
            using (var transaction = connection.BeginTransaction())
            {
                // Create a new SQL command.
                using (var command = connection.CreateCommand())
                {
                    // Set the command's text to a DELETE SQL statement.
                    // This command deletes records from 'dane_miesieczne' where the 'id' does not match the criteria specified in the nested SELECT statement.
                    command.CommandText = @"
    DELETE FROM dane_miesieczne
        WHERE id NOT IN (
        SELECT DISTINCT d.id 
                FROM dane_miesieczne d
                WHERE  (
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 1 AND m.efekt > 5) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 2 AND m.efekt > 5)  AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 3 AND m.efekt > 5) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 4 AND m.efekt > 10) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 5 AND m.efekt > 10) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 6 AND m.efekt > 10) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 7 AND m.efekt > 10) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 8 AND m.efekt > 10) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 9 AND m.efekt > 10) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 10 AND m.efekt > 5) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 11 AND m.efekt > 5) AND
                EXISTS (SELECT * FROM dane_miesieczne m WHERE d.id=m.id AND m.miesiac = 12 AND m.efekt > 5)
            )
        )
    ";

                    // Execute the command and convert the result to an integer, which represents the number of rows to be deleted.
                    int deleteCount = Convert.ToInt32(command.ExecuteScalar());
                    // Print the number of rows to be deleted.
                    Console.WriteLine("Number of rows to be deleted: " + deleteCount);

                    // Execute the DELETE command.
                    command.ExecuteNonQuery();
                }
                // Print a message indicating completion.
                Console.WriteLine("Done");

                // Commit the transaction to finalize the changes.
                transaction.Commit();
            }
        }

    }
}
