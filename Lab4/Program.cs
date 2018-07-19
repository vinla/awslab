using System;
using System.Threading.Tasks;

namespace Lab4
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
