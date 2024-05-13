using Cassandra.Entities;

namespace Cassandra;

public interface ICassandraService
{
    public Task AddAsync(Record record);

    public Task<Record> GetAsync(int id);

    public Task DeleteStringAsync(int id);

    public Task UpdateStringAsync(Record record);

    public Task<List<Record>> GetByIdOrStringAsync(int id, string someString);

    public Task<List<Record>> GetByIdAndStringAsync(int id, string someString);
}