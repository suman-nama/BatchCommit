using System;
using System.Collections.Generic;
using System.Data;
using Npgsql;

class Program
{
    static void Main()
    {
        string connectionString = "Host=localhost:5432;Database=Tejas;Username=postgres;Password=Tejas@2012";
        int batchSize = 100; // Number of records per batch

        // Example data to be inserted
        List<User> users = GetUsers();

        BatchInsertUsers(connectionString, users, batchSize);
    }

    static List<User> GetUsers()
    {
        // This method should return a list of users to be inserted.
        // For example, let's create some dummy data:
        List<User> users = new List<User>();
        for (int i = 1; i <= 1000; i++)
        {
            users.Add(new User { Id = i, Name = $"User{i}" });
        }
        return users;
    }

    static void BatchInsertUsers(string connectionString, List<User> users, int batchSize)
    {
        using (var conn = new NpgsqlConnection(connectionString))
        {
            conn.Open();

            for (int i = 0; i < users.Count; i += batchSize)
            {
                using (var transaction = conn.BeginTransaction())
                {
                    try
                    {
                        for (int j = i; j < i + batchSize && j < users.Count; j++)
                        {
                            string query = "INSERT INTO users (id, name) VALUES (@id, @name)";
                            using (var cmd = new NpgsqlCommand(query, conn))
                            {
                                cmd.Parameters.AddWithValue("@id", users[j].Id);
                                cmd.Parameters.AddWithValue("@name", users[j].Name);
                                cmd.ExecuteNonQuery();
                            }
                        }

                        transaction.Commit();
                        Console.WriteLine($"Batch {i / batchSize + 1} committed successfully.");
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        Console.WriteLine($"Error in batch {i / batchSize + 1}: {ex.Message}");
                    }
                }
            }
        }
    }
}

class User
{
    public int Id { get; set; }
    public string Name { get; set; }
}
