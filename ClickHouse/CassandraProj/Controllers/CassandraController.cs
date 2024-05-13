using System.Diagnostics;
using System.Text;
using Cassandra;
using Cassandra.Entities;
using Microsoft.AspNetCore.Mvc;

namespace ClickHouse.Controllers;



[Route("[controller]")]
public class CassandraController : Controller
{
    private ICassandraService _cassandraService;

    public CassandraController(ICassandraService cassandraService)
    {
        _cassandraService = cassandraService;
    }

    [HttpPost("AddToTable")]
    public async Task AddToTable(Record record)
    {
        var timer = new Stopwatch();
        timer.Start();
        await _cassandraService.AddAsync(record);
        timer.Stop();
        Console.WriteLine($"Cassandra Add to table time: {timer.ElapsedMilliseconds}");
    }
    
    [HttpPost("RemoveFromTable")]
    public async Task RemoveFromTable(int id)
    {
        var timer = new Stopwatch();
        timer.Start();
        await _cassandraService.DeleteStringAsync(id);
        timer.Stop();
        Console.WriteLine($"Cassandra Remove from table time: {timer.ElapsedMilliseconds}");

    }

    [HttpGet("GetById")]
    public async Task<Record> GetById(int id)
    {
        var timer = new Stopwatch();
        timer.Start();
        var result = await _cassandraService.GetAsync(id);
        timer.Stop();
        Console.WriteLine($"Cassandra Get from table time: {timer.ElapsedMilliseconds}");
        return result;
    }
    
    [HttpPost("Update")]
    public async Task UpdateRecord(Record record)
    {
        var timer = new Stopwatch();
        timer.Start();
         await _cassandraService.UpdateStringAsync(record);
        timer.Stop();
        Console.WriteLine($"Cassandra Update table time: {timer.ElapsedMilliseconds}");
    }
    
    [HttpGet("GetByIdAndString")]
    public async Task<List<Record>> GetValuesByIdAndString(int id, string searchString)
    {
        return await _cassandraService.GetByIdAndStringAsync(id, searchString);
    }
    
    [HttpGet("GetByIdOrString")]
    public async Task<List<Record>> GetValuesByIdOrString(int id, string searchString)
    {
        return await _cassandraService.GetByIdAndStringAsync(id, searchString);
    }
}