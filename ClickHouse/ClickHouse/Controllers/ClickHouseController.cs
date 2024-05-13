using System.Diagnostics;
using System.Text;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Utility;
using Microsoft.AspNetCore.Mvc;

namespace ClickHouse.Controllers;



[Route("[controller]")]
public class ClickHouseController : Controller
{
    [HttpPost("CreateTable")]
    public async Task CreateTable(string tableName)
    {
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
        await connection.ExecuteScalarAsync($"CREATE TABLE {tableName} (id INTEGER Primary key, someString String)");
    }
    
    
    [HttpPost("DeleteTable")]
    public async Task DeleteTable(string tableName)
    {
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
        await connection.ExecuteScalarAsync($"Drop TABLE {tableName}");
    }

    [HttpPost("AddToTable")]
    public async Task AddToTable( string tableName, int id, string addString)
    {
        var timer = new Stopwatch();
        timer.Start();
            using var connection = new ClickHouseConnection
                ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
            await connection.ExecuteScalarAsync($"Insert into {tableName} (id, someString) VALUES({id++}, '{addString}') ");
        timer.Stop();

        Console.WriteLine($"Add to table time: {timer.ElapsedMilliseconds}");
        
    }
        
    [HttpPost("RemoveFromTable")]
    public async Task RemoveFromTable(string tableName,int id)
    {
        var timer = new Stopwatch();
        timer.Start();
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
        await connection.ExecuteScalarAsync($"DELETE FROM {tableName} WHERE id = {id}");
        timer.Stop();
        Console.WriteLine($"Remove from table time: {timer.ElapsedMilliseconds}");
    }
    
    [HttpPost("UpdateInTable")]
    public async Task UpdateInTable(string tableName, int id, string newValue)
    {
        var timer = new Stopwatch();
        timer.Start();
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default");
        await connection.ExecuteScalarAsync($"ALTER TABLE {tableName} UPDATE someString = '{newValue}' WHERE id = {id}");
        timer.Stop();
        Console.WriteLine($"Update in table time: {timer.ElapsedMilliseconds}");
    }

    [HttpGet("GetById")]
    public async Task<string> GetValuesByID(string tableName, int id)
    {
        var timer = new Stopwatch();
        timer.Start();
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
        var result = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} WHERE id = {id}");
        var stringBuilder = new StringBuilder();

        while (result.Read())
        {
            stringBuilder.Append("id = "  + result[0] + " string = " + result[1] + "\n");
        }
        timer.Stop();
        Console.WriteLine($"Get by id time: {timer.ElapsedMilliseconds}");
        return stringBuilder.ToString();
    }

    [HttpGet("GetByIdAndString")]
    public async Task<string> GetValuestByIdAndString(string tableName, int id, string searchString)
    {
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
        var result = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} WHERE id = {id} AND someString = '{searchString}'");
        var stringBuilder = new StringBuilder();

        while (result.Read())
        {
            stringBuilder.Append("id = "  + result[0] + " string = " + result[1] + "\n");
        }
        return stringBuilder.ToString();
    }
    
    [HttpGet("GetByIdOrString")]
    public async Task<string> GetValuestByIdOrString(string tableName, int id, string searchString)
    {
        using var connection = new ClickHouseConnection
            ("Host=localhost;Protocol=http;Port=18123;Username=default"); 
        var result = await connection.ExecuteReaderAsync($"SELECT * FROM {tableName} WHERE id = {id} OR someString = '{searchString}'");
        var stringBuilder = new StringBuilder();

        while (result.Read())
        {
            stringBuilder.Append("id = "  + result[0] + " string = " + result[1] + "\n");
        }
        return stringBuilder.ToString();
    }
}