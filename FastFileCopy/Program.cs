using ByteSizeLib;
using ListShuffle;
using System.Diagnostics;
using System.Reflection;
using xxHash3;

namespace FastFileCopy
{
    internal class Program
    {
        //dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true --self-contained true
        //dotnet publish -c Release -r win-x86 -p:PublishSingleFile=true --self-contained true

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


            var sourceDirectory = args[0];
            var destinationDirectory = args[1];

            Operation flag2 = Operation.Copy;
            int flag3 = 10;
            Logging flag4 = Logging.Yes;
            int flag5 = 5;
            string? flag6 = null;
            int flag7 = 1;
            int flag8 = -1;

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

            //MaxGrab
            try { _ = int.TryParse(args[8], out flag8); } catch { }


            if (string.IsNullOrEmpty(sourceDirectory))
            {
                Console.WriteLine("ERROR sourceDirectory is IsNullOrEmpty");
                Console.ReadLine();
                return;
            }

            if (string.IsNullOrEmpty(destinationDirectory))
            {
                Console.WriteLine("ERROR destinationDirectory is IsNullOrEmpty");
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

            var files = Directory.EnumerateFiles(sourceDirectory, flag6, enumerationOptions);

            var lFiles = (flag8 > 0) ? files.Take(flag8).ToList() : files.ToList();

            lFiles.Shuffle();

            if (flag4 == Logging.Yes)
            {
                sw1.Stop();
                Console.WriteLine($"files {lFiles.Count()} listing files took {sw1.ElapsedMilliseconds}ms ");

            }

            Stopwatch sw2 = new();


            if (flag4 == Logging.Yes)
                sw2.Start();


            await Parallel.ForEachAsync(lFiles, new ParallelOptions()
            {
                MaxDegreeOfParallelism = flag3
            },
            async (file, t) =>
            {
                //if (flag4 == Logging.Yes)
                //    Console.WriteLine($"START Source:{Source} SourcePath:{SourcePath} DestinationPath: {DestinationPath}");

                await Execute(file, sourceDirectory, destinationDirectory, flag2, flag4, flag5);
            });


            if (flag4 == Logging.Yes)
            {
                sw2.Stop();
                Console.WriteLine($"{(Convert.ToBoolean((int)flag2) ? "Copy" : "Move")} took {sw2.ElapsedMilliseconds}ms ");
            }


        }


        private static async Task Execute(string file, string sourceDirectory, string destinationDirectory, Operation flag2, Logging flag4, int flag5)
        {
            try
            {
                var relativePath = file.Substring(sourceDirectory.Length + 1);
                var destinationFile = new List<string> { destinationDirectory, relativePath }.ForceCombine();
                var tmpDestinationFile = $"{destinationFile}.tmp";

                string? destinationFolder = Path.GetDirectoryName(destinationFile);

                if (string.IsNullOrEmpty(destinationFolder))
                    throw new Exception($"destinationFolder IsNullOrEmpty");

                if (!Directory.Exists(destinationFolder) && !string.IsNullOrEmpty(destinationFolder))
                    Directory.CreateDirectory(destinationFolder);

                Stopwatch sw1 = new();

                if (flag4 == Logging.Yes)
                    sw1.Start();


                var array_length = 262144; //0.25mb

                int matchedCheckSums = 0;
                int chunkRetries = 0;
                double bytesRead = 0;

                int abort = 0;

                using (FileStream fsread = new(file, FileMode.Open, FileAccess.Read, FileShare.None, array_length, FileOptions.Asynchronous))
                {
                    using (FileStream fswrite = new(tmpDestinationFile, FileMode.Create, FileAccess.Write, FileShare.Read, array_length, FileOptions.Asynchronous))
                    {
                        using (FileStream fsCheckRead = new(tmpDestinationFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, array_length, FileOptions.Asynchronous))
                        {

                            var dataArray = new byte[array_length];
                            var checkArray = new byte[array_length];

                            for (; ; )
                            {
                                var sourcePositon = fsread.Position;

                                int read = await fsread.ReadAsync(dataArray.AsMemory(0, array_length));
                                if (read == 0)
                                    break;

                                var sourceChecksum = is64bit ? xxHash64.Hash(dataArray) : xxHash32.Hash(dataArray);

                                bytesRead += read;

                                await fswrite.WriteAsync(dataArray.AsMemory(0, read));

                                //have to write to disk so that we can read and check the results
                                await fswrite.FlushAsync();

                                await fsCheckRead.ReadAsync(checkArray.AsMemory(0, read));

                                var destinationChecksum = is64bit ? xxHash64.Hash(checkArray) : xxHash32.Hash(checkArray);

                                ////code to test chunk retries
                                //var rnd = new Random();
                                //var p = rnd.Next(1, 10000);

                                //if (p < 1000)
                                //    destHash += 1;


                                if (sourceChecksum == destinationChecksum)
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


                if (File.Exists(tmpDestinationFile))
                    File.Move(tmpDestinationFile, destinationFile, true);

                File.SetCreationTime(destinationFile, File.GetCreationTime(file));
                File.SetLastWriteTime(destinationFile, File.GetLastWriteTime(file));
                File.SetLastAccessTime(destinationFile, File.GetLastAccessTime(file));


                if (flag2 == Operation.Move)
                    if (File.Exists(file))
                        File.Delete(file);


                if (flag4 == Logging.Yes)
                {
                    sw1.Stop();
                    Console.WriteLine($"Source:{file} Dest:{destinationFile} Size:{ByteSize.FromBytes(bytesRead):0.00} Matched Checksum:{matchedCheckSums} Chunk Retries:{chunkRetries} Time:{sw1.ElapsedMilliseconds}ms");
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
