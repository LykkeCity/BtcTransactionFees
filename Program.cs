using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NBitcoin;
using Polly;
using QBitNinja.Client;
using QBitNinja.Client.Models;

namespace BtcTransactionFees
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.WaitAll(Proceed("35LF9cgNicZvcvJVFAs19bMV3fmf5CodW2"),
                Proceed("1PHQMMfEx6uZnRVxoCtXRopvnpzq9EiKSs"),
                Proceed("33u48gM2gdz9TU5QT95JhvEpBB7zTHWPy2"),
                Proceed("3KX28DsrYN3UUUdPVKa1UxDXWfhHW6Yfpk"),
                Proceed("34p8SsMbJzS48hkwEucdj6kEnZ2hiRX6Gv"));

            Console.WriteLine($"[{DateTime.UtcNow}] All done");

            Console.ReadLine();
        }


        private static async Task Proceed(string address)
        {
            var client = new CustomQBitNinjaClient(Network.Main);
            var addr = BitcoinAddress.Create(address, Network.Main);


            var listAllOperations = new List<BalanceOperation>();
            string continuation = null;

            do
            {
                Console.WriteLine($"Retrieving balance {addr}. Already retrieved {listAllOperations.Count} ops. Continuation - \"{continuation}\"");
                BalanceModel balance = await GetBalanceModel(client, addr, continuation);

                continuation = balance.Continuation;
                listAllOperations.AddRange(balance.Operations);
            } while (continuation != null);

            var result = new List<(string transactionId, decimal fee, bool isCashout, DateTime date, string address)>();
            var counter = 0;
            var validOps = listAllOperations.Where(p => p.BlockId != null).ToList();
            Task.WaitAll(validOps.ForEachAsyncSemaphore(8, async (op) =>
            {
                await Policy.Handle<Exception>().WaitAndRetryAsync(10,
                    retryAttempt => TimeSpan.FromSeconds(3), (ex, timeSpan) =>
                    {
                        Console.WriteLine($"Exception: {ex}");
                        Console.WriteLine($"Retrying in {timeSpan.Seconds} seconds");
                    }).ExecuteAsync(async () => { result.Add(await Execute(client, op, addr)); });

                Console.WriteLine($"Processed {Interlocked.Increment(ref counter)} of {validOps.Count} tx at {addr}");
            }));

            Console.WriteLine($"[{DateTime.UtcNow}] Retrieve data done for {addr}");

            var fl = $"result-{addr}-{DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss")}.csv";

            File.AppendAllText(fl,
                string.Join(";", "address", "transactionHash", "fee", "date", "in/out")
                + Environment.NewLine);

            foreach (var item in result.OrderByDescending(p => p.date))
            {
                File.AppendAllText(fl,
                    string.Join(";",
                        item.address,
                        item.transactionId,
                        item.fee.ToString(CultureInfo.InvariantCulture),
                        item.date.ToString("yyyy-MM-ddTHH:mm:ss.fff"),
                        item.isCashout ? "out" : "in")
                    + Environment.NewLine);
            }
        }
        private static async Task<(string transactionId, decimal fee, bool isCashout, DateTime date, string address)> Execute(CustomQBitNinjaClient client, BalanceOperation op, BitcoinAddress addr)
        {
            var getBlock = client.GetBlock(new BlockFeature(op.BlockId), true);
            var getTx = client.GetTransaction(op.TransactionId);
            
            var date = getBlock.Result.AdditionalInformation.BlockTime.DateTime;

            var fee = getTx.Result.Fees.ToUnit(MoneyUnit.BTC);

            var isCashout =
                getTx.Result.SpentCoins.Any(p => p.TxOut.ScriptPubKey.GetDestinationAddress(Network.Main) == addr);

            Console.WriteLine($"{addr} {op.TransactionId} {fee} {isCashout} {date}");

            return (transactionId: op.TransactionId.ToString(), fee: fee, isCashout: isCashout, date: date,
                address: addr.ToString());
        }

        private static async Task<BalanceModel> GetBalanceModel(CustomQBitNinjaClient client, BitcoinAddress addr, string continuation)
        {
            return await Policy.Handle<Exception>().WaitAndRetryAsync(100,
                retryAttempt => TimeSpan.FromSeconds(3), (ex, timeSpan) =>
                {
                    Console.WriteLine($"Exception: {ex}");
                    Console.WriteLine($"Retrying in {timeSpan.Seconds} seconds");
                }).ExecuteAsync(async () => await client.GetBalance(addr, false, continuation));
        }
    }
}
