using ByteSizeLib;
using System.Diagnostics;
using System.Reflection;
using xxHash3;

namespace FastFileCopy
{
    internal class Program
    {
        //dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true --self-contained true

        enum Operation
        {
            Move,
            Copy
        }

        enum Logging
        {
            No,
            Yes
        }

        private static bool is64bit;

        static async Task Main(string[] args)
        {
            is64bit = Environment.Is64BitOperatingSystem;

            if (args[0] == "?")
            {
                Console.Write("help.txt".GetEmbeddedResource("FastFileCopy"));
                Console.ReadKey();
                return;
            }


            var SourcePath = args[0];
            var DestinationPath = args[1];

            Operation flag2 = Operation.Copy;
            int flag3 = 10;
            Logging flag4 = Logging.Yes;
            int flag5 = 5;
            string? flag6 = null;
            int flag7 = 1;

            //Operation
            try { flag2 = (Operation)int.Parse(args[2]); } catch { }

            //MaxDegreeOfParallelism
            try { _ = int.TryParse(args[3], out flag3); } catch { }

            //Logging
            try { flag4 = (Logging)int.Parse(args[4]); } catch { }

            //abort
            try { _ = int.TryParse(args[5], out flag5); } catch { }

            //SearchPattern
            try { flag6 = args[6]; } catch { }

            //RecurseSubdirectories
            try { _ = int.TryParse(args[7], out flag7); } catch { }


            if (string.IsNullOrEmpty(SourcePath))
            {
                Console.WriteLine("ERROR SourcePath is IsNullOrEmpty");
                Console.ReadLine();
                return;
            }

            if (string.IsNullOrEmpty(DestinationPath))
            {
                Console.WriteLine("ERROR DestinationPath is IsNullOrEmpty");
                Console.ReadLine();
                return;
            }

            if (string.IsNullOrEmpty(flag6))
                flag6 = "*";

            Stopwatch sw1 = new();

            if (flag4 == Logging.Yes)
                sw1.Start();

            var enumerationOptions = new EnumerationOptions()
            {
                RecurseSubdirectories = Convert.ToBoolean(flag7),
                IgnoreInaccessible = true,
                ReturnSpecialDirectories = false,
                AttributesToSkip = default,
                MatchCasing = MatchCasing.CaseInsensitive
            };

            var files = Directory.EnumerateFiles(SourcePath, flag6, enumerationOptions);

            if (flag4 == Logging.Yes)
            {
                sw1.Stop();
                Console.WriteLine($"files {files.Count()} listing files took {sw1.ElapsedMilliseconds}ms ");

            }

            Stopwatch sw2 = new();


            if (flag4 == Logging.Yes)
                sw2.Start();


            await Parallel.ForEachAsync(files, new ParallelOptions()
            {
                MaxDegreeOfParallelism = flag3
            },
            async (Source, t) =>
            {
                //if (flag4 == Logging.Yes)
                //    Console.WriteLine($"START Source:{Source} SourcePath:{SourcePath} DestinationPath: {DestinationPath}");

                await Execute(Source, SourcePath, DestinationPath, flag2, flag4, flag5);
            });


            if (flag4 == Logging.Yes)
            {
                sw2.Stop();
                Console.WriteLine($"{(Convert.ToBoolean((int)flag2) ? "Copy" : "Move")} took {sw2.ElapsedMilliseconds}ms ");
            }


        }


        private static async Task Execute(string Source, string SourcePath, string DestinationPath, Operation flag2, Logging flag4, int flag5)
        {
            try
            {

                var dest = new List<string> { DestinationPath, Source.Replace(SourcePath, string.Empty) }.ForceCombine();

                string? path = Path.GetDirectoryName(dest);

                if (string.IsNullOrEmpty(path))
                    throw new Exception($"path IsNullOrEmpty");

                var tmpDest = $"{new List<string> { path, Path.GetFileName(dest) }.ForceCombine()}.tmp";

                string? targetFolder = Path.GetDirectoryName(tmpDest);

                if (!Directory.Exists(targetFolder) && !string.IsNullOrEmpty(targetFolder))
                    Directory.CreateDirectory(targetFolder);

                Stopwatch sw1 = new();

                if (flag4 == Logging.Yes)
                    sw1.Start();


                var array_length = 262144; //0.25mb

                int matchedCheckSums = 0;
                int chunkRetries = 0;
                double bytesRead = 0;

                int abort = 0;

                using (FileStream fsread = new(Source, FileMode.Open, FileAccess.Read, FileShare.None, array_length, FileOptions.Asynchronous))
                {
                    using (FileStream fswrite = new(tmpDest, FileMode.Create, FileAccess.Write, FileShare.Read, array_length, FileOptions.Asynchronous))
                    {
                        using (FileStream fsCheckRead = new(tmpDest, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, array_length, FileOptions.Asynchronous))
                        {

                            var dataArray = new byte[array_length];
                            var checkArray = new byte[array_length];

                            for (; ; )
                            {
                                var sourcePositon = fsread.Position;

                                int read = await fsread.ReadAsync(dataArray.AsMemory(0, array_length));
                                if (read == 0)
                                    break;

                                var sourceHash = is64bit ? xxHash64.Hash(dataArray) : xxHash32.Hash(dataArray);

                                bytesRead += read;

                                await fswrite.WriteAsync(dataArray.AsMemory(0, read));

                                //have to write to disk so that we can read and check the results
                                await fswrite.FlushAsync();

                                _ = await fsCheckRead.ReadAsync(checkArray.AsMemory(0, read));

                                var destHash = is64bit ? xxHash64.Hash(checkArray) : xxHash32.Hash(checkArray);

                                ////code to test chunk retries
                                //var rnd = new Random();
                                //var p = rnd.Next(1, 10000);

                                //if (p < 1000)
                                //    destHash += 1;


                                if (sourceHash == destHash)
                                {
                                    abort = 0;
                                    matchedCheckSums++;
                                }
                                else
                                {
                                    chunkRetries++;

                                    abort++;

                                    if (abort > flag5)
                                        throw new Exception($"xxHash not matching after {flag5} attempts!");

                                    fsread.Position = sourcePositon;
                                    fswrite.Position = sourcePositon;
                                    fsCheckRead.Position = sourcePositon;
                                    bytesRead -= read;

                                    Thread.Sleep(100);
                                }
                            }

                        };

                    };

                };


                if (File.Exists(tmpDest))
                    File.Move(tmpDest, dest, true);

                File.SetCreationTime(dest, File.GetCreationTime(Source));
                File.SetLastWriteTime(dest, File.GetLastWriteTime(Source));
                File.SetLastAccessTime(dest, File.GetLastAccessTime(Source));


                if (flag2 == Operation.Move)
                    if (File.Exists(Source))
                        File.Delete(Source);


                if (flag4 == Logging.Yes)
                {
                    sw1.Stop();
                    Console.WriteLine($"Source:{Source} Dest:{dest} Size:{ByteSize.FromBytes(bytesRead):0.00} Matched Checksum:{matchedCheckSums} Chunk Retries:{chunkRetries} Time:{sw1.ElapsedMilliseconds}ms");
                }


            }
            catch (Exception ex)
            {
                if (flag4 == Logging.Yes)
                    Console.WriteLine($"error:  {ex.Message} | {ex.InnerException} | {ex.StackTrace}");
            }

        }


    }


    public static class ExtentionMethods
    {
        public static string ForceCombine(this List<string> paths)
        {
            return paths.Aggregate((x, y) => Path.Combine(x, y.TrimStart('\\')));
        }

        public static string GetEmbeddedResource(this string filename, string ns)
        {

            using Stream? stream = Assembly.GetExecutingAssembly().GetManifestResourceStream($"{ns}.{filename}");

            if (stream == null)
                throw new Exception($"stream is null {filename} not found!");

            using StreamReader reader = new(stream);

            return reader.ReadToEnd();
        }

    }

}
