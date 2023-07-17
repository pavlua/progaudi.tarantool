

using ProGaudi.Tarantool.Client;
using ProGaudi.Tarantool.Client.Model;

namespace Sandbox
{
    public class Program
    {
        static async Task Main()
        {
            var box = new Box(new ClientOptions("admin:adminPassword@127.0.0.1:3320,admin:adminPassword@127.0.0.1:3321" 
                , log: new TextWriterLog(Console.Out)
                )
            {
                Strategy = ConnectorStrategy.RoundRobin
            });

            await box.Connect();

            var watch = System.Diagnostics.Stopwatch.StartNew();
    
            await Parallel.ForEachAsync(Enumerable.Range(1, 10), async (i, token) => 
            {
                for (var j = 1; j <= 10; j++)
                {
                    try
                    {
                        var res = await box.Eval<int>($"return {j}");
                        Console.WriteLine($"thread {i}: {res.Data[0]}");
                    }
                    catch (Exception ex) 
                    { 
                        Console.WriteLine(ex.ToString()); 
                    };
                }
            });

            watch.Stop();
            Console.Write("Elapsed: " + watch.ElapsedMilliseconds);

            while (true) 
            {
                var key = Console.ReadKey();
                if (key.KeyChar == 'q')
                {
                    break;
                }
                if (key.KeyChar == 'c')
                {
                    await box.Connect();
                }
            }
            Console.ReadKey();
        }
    }
}