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

        static void Main(string[] args)
        {

            var SourcePath = args[0];
            var DestinationPath = args[1];

            Operation flag2 = Operation.Copy;
            int flag3 = 10;
            Logging flag4 = Logging.Yes;
            int flag5 = 5;

            //Operation
            try { flag2 = (Operation)int.Parse(args[2]); } catch { }

            //MaxDegreeOfParallelism
            try { _ = int.TryParse(args[3], out flag3); } catch { }

            //Logging
            try { flag4 = (Logging)int.Parse(args[4]); } catch { }

            //abort
            try { _ = int.TryParse(args[5], out flag5); } catch { }

            if (SourcePath == null)
            {
                Console.WriteLine("ERROR SourcePath is null");
                Console.ReadLine();
                return;
            }

            if (DestinationPath == null)
            {
                Console.WriteLine("ERROR DestinationPath is null");
                Console.ReadLine();
                return;
            }


            Stopwatch sw1 = new();

            if (flag4 == Logging.Yes)
                sw1.Start();

            var files = Directory.GetFiles(SourcePath);

            if (flag4 == Logging.Yes)
            {
                sw1.Stop();
                Console.WriteLine($"files {files.Length} listing files took {sw1.ElapsedMilliseconds}ms ");

            }

            Stopwatch sw2 = new();


            if (flag4 == Logging.Yes)
                sw2.Start();

            if (flag3 == 1)
            {
                files.ToList().ForEach(source =>
                {
                    DoTaskWork(DestinationPath, source, flag2, flag4, flag5);
                });

            }
            else
            {

                Parallel.ForEach(files, new ParallelOptions()
                {
                    MaxDegreeOfParallelism = flag3
                },
                (source) =>
                {
                    DoTaskWork(DestinationPath, source, flag2, flag4, flag5);
                });

            }


            if (flag4 == Logging.Yes)
            {
                sw2.Stop();
                Console.WriteLine($"copy/move took {sw2.ElapsedMilliseconds}ms ");
            }



        }


        private static void DoTaskWork(string DestinationPath, string source, Operation flag2, Logging flag4, int flag5)
        {
            try
            {
                var dest = Path.Combine(DestinationPath, Path.GetFileName(source));

                Execute(source, dest, flag2, flag4, flag5);

            }
            catch (Exception ex)
            {

                if (flag4 == Logging.Yes)
                    Console.WriteLine($"error:  {ex.Message} | {ex.InnerException} | {ex.StackTrace}");
            }

        }


        private static void Execute(string source, string dest, Operation flag2, Logging flag4, int flag5)
        {
            try
            {
                Stopwatch sw1 = new();

                if (flag4 == Logging.Yes)
                    sw1.Start();


                var array_length = (int)Math.Pow(2, 19);  //0.5mb

                int matchedCheckSums = 0;
                int chunkRetries = 0;

                using (FileStream fsread = new(source, FileMode.Open, FileAccess.Read, FileShare.None, array_length))
                {
                    using (BinaryReader bwread = new(fsread))
                    {
                        using (FileStream fswrite = new(dest, FileMode.Create, FileAccess.Write, FileShare.Read, array_length))
                        {
                            using (BinaryWriter bwwrite = new(fswrite))
                            {
                                using (FileStream fsCheckRead = new(dest, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, array_length))
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
                                            var sourceHash = xxHash64.Hash(dataArray);
                                            if (read == 0)
                                                break;

                                            bwwrite.Write(dataArray, 0, read);

                                            //have to write to disk so that we can read and check the results
                                            bwwrite.Flush();

                                            bwCheckread.Read(checkArray, 0, read);
                                            var destHash = xxHash64.Hash(checkArray);

                                            if (sourceHash == destHash)
                                            {
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
                                            }
                                        }

                                    };
                                };

                            };
                        };
                    };
                };


                if (flag2 == Operation.Move)
                    File.Delete(source);

                if (flag4 == Logging.Yes)
                {
                    sw1.Stop();
                    Console.WriteLine($"source:{source} dest:{dest} matchedCheckSums:{matchedCheckSums} chunkRetries:{chunkRetries} in {sw1.ElapsedMilliseconds}ms");
                }


            }
            catch (Exception)
            {
                throw;
            }

        }
    }

}