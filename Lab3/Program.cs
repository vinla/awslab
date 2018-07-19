using System;
using System.Threading.Tasks;

namespace Lab3
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var labRunner = new LabRunner();
            await labRunner.Run();
            Console.WriteLine("Finished");
        }
    }
}
