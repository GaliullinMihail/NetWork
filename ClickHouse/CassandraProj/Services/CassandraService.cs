using Cassandra.Entities;

namespace Cassandra.Services;

public class CassandraService : ICassandraService
{
    private ISession session;
    public CassandraService()
    {
        var cluster = Cluster.Builder()
            .AddContactPoints("localhost")
            .WithPort(9042)
            .Build();
        session = cluster.Connect("techframer");
        
    }
    public async Task AddAsync(Record record)
    {
        var query = await session.PrepareAsync("INSERT INTO benchmark (benchmark_id, benchmark_string) VALUES (?, ?)");
        var statement = query.Bind(record.Id, record.SomeString); 
        await session.ExecuteAsync(statement);
    }

    public async Task<Record> GetAsync(int id)
    {
        var query = await session.PrepareAsync("SELECT * FROM benchmark WHERE benchmark_id = ?");
        var statement = query.Bind(id);
        var rs = await session.ExecuteAsync(statement);
        
        var row = rs.FirstOrDefault();

        if (row != null)
        {
            return new Record
            {
                Id = row.GetValue<int>("benchmark_id"),
                SomeString = row.GetValue<string>("benchmark_string")
            };
        }

        return null;
    }

    public async Task DeleteStringAsync(int id)
    {
        var query = await session.PrepareAsync("DELETE FROM benchmark WHERE benchmark_id = ?");
        var statement = query.Bind(id);
        await session.ExecuteAsync(statement);
    }

    public async Task UpdateStringAsync(Record record)
    {
        var query = await session.PrepareAsync("UPDATE benchmark SET benchmark_string = ? WHERE benchmark_id = ?");
        var statement = query.Bind(record.SomeString, record.Id);
        await session.ExecuteAsync(statement);
    }

    public async Task<List<Record>> GetByIdOrStringAsync(int id, string someString)
    {
        var query = await session.PrepareAsync("SELECT * FROM benchmark WHERE benchmark_id = ? OR benchmark_string = ? ALLOW FILTERING");
        var statement = query.Bind(id, someString);
        var rs = await session.ExecuteAsync(statement);
        var result = rs.Select(rowSet => new Record
        {
            Id = rowSet.GetValue<int>("benchmark_id"),
            SomeString = rowSet.GetValue<string>("benchmark_string")
        }).ToList();
        return result;
    }

    public async Task<List<Record>> GetByIdAndStringAsync(int id, string someString)
    {
        var query = await session.PrepareAsync("SELECT * FROM benchmark WHERE benchmark_id = ? AND benchmark_string = ? ALLOW FILTERING");
        var statement = query.Bind(id, someString);
        var rs = await session.ExecuteAsync(statement);
        var result = rs.Select(rowSet => new Record
        {
            Id = rowSet.GetValue<int>("benchmark_id"),
            SomeString = rowSet.GetValue<string>("benchmark_string")
        }).ToList();
        return result;
    }
    
}