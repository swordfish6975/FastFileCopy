using ByteSizeLib;
using System.Diagnostics;
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

        static async Task Main(string[] args)
        {

            var SourcePath = args[0];
            var DestinationPath = args[1];

            Operation flag2 = Operation.Copy;
            int flag3 = 10;
            Logging flag4 = Logging.Yes;
            int flag5 = 5;
            string? flag6 = null;
            bool flag7 = true;

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
            try { flag7 = bool.Parse(args[7]); } catch { }


            if (string.IsNullOrEmpty(SourcePath))
            {
                Console.WriteLine("ERROR SourcePath is null");
                Console.ReadLine();
                return;
            }

            if (string.IsNullOrEmpty(DestinationPath))
            {
                Console.WriteLine("ERROR DestinationPath is null");
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
                RecurseSubdirectories = flag7,
                IgnoreInaccessible = true,
                ReturnSpecialDirectories = false,
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
            (Source, t) =>
            {
                Execute(DestinationPath, SourcePath.Length, Source, flag2, flag4, flag5);
                return new ValueTask();
            });


            if (flag4 == Logging.Yes)
            {
                sw2.Stop();
                Console.WriteLine($"copy/move took {sw2.ElapsedMilliseconds}ms ");
            }


        }


        private static void Execute(string DestinationPath, int SourcePathLength, string Source, Operation flag2, Logging flag4, int flag5)
        {
            try
            {

                var dest = new List<string> { DestinationPath, Source.Remove(0, SourcePathLength) }.ForceCombine();

                string? path = Path.GetDirectoryName(dest);

                if (string.IsNullOrEmpty(path))
                    throw new Exception($"path == null");

                var tempDest = $"{new List<string> { path, Path.GetFileNameWithoutExtension(dest) }.ForceCombine()}.temp";

                string? targetFolder = Path.GetDirectoryName(tempDest);

                if (!Directory.Exists(targetFolder) && !string.IsNullOrEmpty(targetFolder))
                    Directory.CreateDirectory(targetFolder);

                Stopwatch sw1 = new();

                if (flag4 == Logging.Yes)
                    sw1.Start();


                var array_length = (int)Math.Pow(2, 19);  //0.5mb

                int matchedCheckSums = 0;
                int chunkRetries = 0;
                double bytesRead = 0;

                using (FileStream fsread = new(Source, FileMode.Open, FileAccess.Read, FileShare.None, array_length))
                {
                    using (BinaryReader bwread = new(fsread))
                    {
                        using (FileStream fswrite = new(tempDest, FileMode.Create, FileAccess.Write, FileShare.Read, array_length))
                        {
                            using (BinaryWriter bwwrite = new(fswrite))
                            {
                                using (FileStream fsCheckRead = new(tempDest, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, array_length))
                                {
                                    using (BinaryReader bwCheckread = new(fsCheckRead))
                                    {
                                        var dataArray = new byte[array_length];
                                        var checkArray = new byte[array_length];

                                        int abort = 0;

                                        for (; ; )
                                        {
                                            var sourcePositon = fsread.Position;

                                            int read = bwread.Read(dataArray, 0, array_length);
                                            if (read == 0)
                                                break;

                                            var sourceHash = xxHash64.Hash(dataArray);

                                            bytesRead += read;

                                            bwwrite.Write(dataArray, 0, read);

                                            //have to write to disk so that we can read and check the results
                                            bwwrite.Flush();

                                            bwCheckread.Read(checkArray, 0, read);
                                            var destHash = xxHash64.Hash(checkArray);

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
                                                    throw new Exception($"xxHash64 not matching after {flag5} attempts!");

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
                        };
                    };
                };


                if (File.Exists(tempDest))
                    File.Move(tempDest, dest, true);


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
    }

}
