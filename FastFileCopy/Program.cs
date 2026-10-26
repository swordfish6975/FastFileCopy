using ByteSizeLib;
using Microsoft.Win32.SafeHandles;
using System.Buffers;
using System.Collections.Concurrent;
using System.CommandLine;
using System.Diagnostics;
using System.IO.Hashing;

namespace FastFileCopy
{
    internal class Program
    {
        //dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true --self-contained true
        //dotnet publish -c Release -r win-x86 -p:PublishSingleFile=true --self-contained true

        static async Task<int> Main(string[] args)
        {
            //1.x took every setting as a positional number, point old scripts at the new switches
            if (args.Length > 2 && int.TryParse(args[2], out _))
            {
                Console.Error.WriteLine("ERROR positional settings from FastFileCopy 1.x are no longer supported, use named options instead");
                Console.Error.WriteLine(@"e.g. 1.x: ""C:\temp1"" ""C:\temp2"" 0 500 1 5 ""*.xml"" 1");
                Console.Error.WriteLine(@"     2.0: ""C:\temp1"" ""C:\temp2"" --move --threads 500 --retries 5 --pattern ""*.xml""");
                Console.Error.WriteLine("run FastFileCopy --help for all options");
                return 1;
            }

            var sourceArgument = new Argument<DirectoryInfo>("source") { Description = "Source directory (full path)" }.AcceptExistingOnly();
            var destinationArgument = new Argument<DirectoryInfo>("destination") { Description = "Destination directory (full path)" };

            var moveOption = new Option<bool>("--move", "-m") { Description = "Move files instead of copying them" };
            var threadsOption = new Option<int>("--threads", "-t") { Description = "Maximum number of files processed in parallel", DefaultValueFactory = _ => 10 };
            var patternOption = new Option<string>("--pattern", "-p") { Description = "Search pattern for files to include", DefaultValueFactory = _ => "*" };
            var noRecurseOption = new Option<bool>("--no-recurse") { Description = "Only process the top directory, skip subdirectories" };
            var maxFilesOption = new Option<int?>("--max-files", "-n") { Description = "Maximum number of files to process (default: all files)" };
            var retriesOption = new Option<int>("--retries", "-r") { Description = "Checksum mismatches allowed per chunk before giving up on a file (100ms delay between attempts)", DefaultValueFactory = _ => 5 };
            var noVerifyOption = new Option<bool>("--no-verify") { Description = "Skip checksum verification of written chunks (fastest)" };
            var noTimestampsOption = new Option<bool>("--no-timestamps") { Description = "Don't copy creation, last write and last access times to the destination" };
            var bufferOption = new Option<int>("--buffer", "-b") { Description = "Chunk size in KB used for reading, writing and verification", DefaultValueFactory = _ => 1024 };
            var quietOption = new Option<bool>("--quiet", "-q") { Description = "Don't log anything except the final summary" };

            threadsOption.Validators.Add(r => { if (r.GetValueOrDefault<int>() < 1) r.AddError("--threads must be at least 1"); });
            retriesOption.Validators.Add(r => { if (r.GetValueOrDefault<int>() < 0) r.AddError("--retries can't be negative"); });
            bufferOption.Validators.Add(r => { if (r.GetValueOrDefault<int>() is < 4 or > 1048576) r.AddError("--buffer must be between 4 and 1048576 KB"); });
            maxFilesOption.Validators.Add(r => { if (r.GetValueOrDefault<int?>() < 1) r.AddError("--max-files must be at least 1"); });

            var rootCommand = new RootCommand("FastFileCopy - fast, parallel, checksum verified file copy/move")
            {
                sourceArgument,
                destinationArgument,
                moveOption,
                threadsOption,
                patternOption,
                noRecurseOption,
                maxFilesOption,
                retriesOption,
                noVerifyOption,
                noTimestampsOption,
                bufferOption,
                quietOption
            };

            rootCommand.SetAction((parseResult, cancellationToken) =>
            {
                var options = new CopyOptions(
                    Source: Path.TrimEndingDirectorySeparator(parseResult.GetValue(sourceArgument)!.FullName),
                    Destination: Path.TrimEndingDirectorySeparator(parseResult.GetValue(destinationArgument)!.FullName),
                    Move: parseResult.GetValue(moveOption),
                    Threads: parseResult.GetValue(threadsOption),
                    Pattern: parseResult.GetValue(patternOption) ?? "*",
                    Recurse: !parseResult.GetValue(noRecurseOption),
                    MaxFiles: parseResult.GetValue(maxFilesOption),
                    Retries: parseResult.GetValue(retriesOption),
                    Verify: !parseResult.GetValue(noVerifyOption),
                    CopyTimestamps: !parseResult.GetValue(noTimestampsOption),
                    BufferSize: parseResult.GetValue(bufferOption) * 1024,
                    Logging: !parseResult.GetValue(quietOption));

                return Run(options, cancellationToken);
            });

            return await rootCommand.Parse(args).InvokeAsync();
        }

        record CopyOptions(string Source, string Destination, bool Move, int Threads, string Pattern, bool Recurse, int? MaxFiles,
            int Retries, bool Verify, bool CopyTimestamps, int BufferSize, bool Logging);

        record FileResult(long Bytes, int MatchedChecksums, int ChunkRetries, bool Renamed);

        private static async Task<int> Run(CopyOptions options, CancellationToken cancellationToken)
        {
            if (string.Equals(options.Source, options.Destination, StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine("ERROR source and destination are the same directory");
                return 1;
            }

            //buffered output, logging every file through an autoflushing console is a bottleneck when redirecting to a log
            var output = TextWriter.Synchronized(new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = false });
            Console.SetOut(output);

            try
            {
                var sw1 = Stopwatch.StartNew();

                var enumerationOptions = new EnumerationOptions()
                {
                    RecurseSubdirectories = options.Recurse,
                    IgnoreInaccessible = true,
                    ReturnSpecialDirectories = false,
                    AttributesToSkip = default,
                    MatchCasing = MatchCasing.CaseInsensitive
                };

                var files = Directory.EnumerateFiles(options.Source, options.Pattern, enumerationOptions);

                var lFiles = (options.MaxFiles is int maxFiles) ? files.Take(maxFiles).ToArray() : files.ToArray();

                //spread big and small files evenly across the threads
                Random.Shared.Shuffle(lFiles);

                sw1.Stop();

                if (options.Logging)
                    Console.WriteLine($"files {lFiles.Length} listing files took {sw1.ElapsedMilliseconds}ms ");

                //a move inside the same volume is just a rename, no need to touch the data
                var sameVolume = string.Equals(Path.GetPathRoot(options.Source), Path.GetPathRoot(options.Destination), StringComparison.OrdinalIgnoreCase);

                var createdFolders = new ConcurrentDictionary<string, Lazy<bool>>(StringComparer.OrdinalIgnoreCase);

                long totalBytes = 0;
                int succeeded = 0;
                int failed = 0;

                var sw2 = Stopwatch.StartNew();

                try
                {
                    await Parallel.ForEachAsync(lFiles, new ParallelOptions()
                    {
                        MaxDegreeOfParallelism = options.Threads,
                        CancellationToken = cancellationToken
                    },
                    async (file, t) =>
                    {
                        var result = await Execute(file, options, sameVolume, createdFolders, t);

                        if (result is null)
                        {
                            Interlocked.Increment(ref failed);
                        }
                        else
                        {
                            Interlocked.Increment(ref succeeded);
                            Interlocked.Add(ref totalBytes, result.Bytes);
                        }
                    });
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("cancelled");
                }

                sw2.Stop();

                var seconds = Math.Max(sw2.Elapsed.TotalSeconds, 0.001);

                Console.WriteLine($"{(options.Move ? "Move" : "Copy")} took {sw2.ElapsedMilliseconds}ms Files:{succeeded} Failed:{failed} Skipped:{lFiles.Length - succeeded - failed} " +
                    $"Size:{ByteSize.FromBytes(totalBytes):0.00} Speed:{ByteSize.FromBytes(totalBytes / seconds):0.00}/s");

                return (failed > 0 || cancellationToken.IsCancellationRequested) ? 1 : 0;
            }
            finally
            {
                output.Flush();
            }
        }


        private static async Task<FileResult?> Execute(string file, CopyOptions options, bool sameVolume, ConcurrentDictionary<string, Lazy<bool>> createdFolders, CancellationToken cancellationToken)
        {
            string? tmpDestinationFile = null;

            try
            {
                var relativePath = Path.GetRelativePath(options.Source, file);
                var destinationFile = Path.Combine(options.Destination, relativePath);

                string? destinationFolder = Path.GetDirectoryName(destinationFile);

                if (string.IsNullOrEmpty(destinationFolder))
                    throw new Exception($"destinationFolder IsNullOrEmpty");

                //every thread waits until the folder really exists, but it's only created once
                _ = createdFolders.GetOrAdd(destinationFolder, f => new Lazy<bool>(() => Directory.CreateDirectory(f).Exists)).Value;

                Stopwatch sw1 = Stopwatch.StartNew();

                FileResult result;

                if (options.Move && sameVolume)
                {
                    var length = new FileInfo(file).Length;
                    File.Move(file, destinationFile, true);
                    result = new FileResult(length, 0, 0, true);
                }
                else
                {
                    //read the times up front, reading the file would change the last access time
                    var creationTime = File.GetCreationTimeUtc(file);
                    var lastWriteTime = File.GetLastWriteTimeUtc(file);
                    var lastAccessTime = File.GetLastAccessTimeUtc(file);

                    tmpDestinationFile = $"{destinationFile}.tmp";

                    result = await CopyFile(file, tmpDestinationFile, options, cancellationToken);

                    File.Move(tmpDestinationFile, destinationFile, true);
                    tmpDestinationFile = null;

                    if (options.CopyTimestamps)
                    {
                        File.SetCreationTimeUtc(destinationFile, creationTime);
                        File.SetLastWriteTimeUtc(destinationFile, lastWriteTime);
                        File.SetLastAccessTimeUtc(destinationFile, lastAccessTime);
                    }

                    if (options.Move)
                        File.Delete(file);
                }

                if (options.Logging)
                {
                    sw1.Stop();
                    Console.WriteLine(result.Renamed
                        ? $"Source:{file} Dest:{destinationFile} Size:{ByteSize.FromBytes(result.Bytes):0.00} Renamed Time:{sw1.ElapsedMilliseconds}ms"
                        : $"Source:{file} Dest:{destinationFile} Size:{ByteSize.FromBytes(result.Bytes):0.00} Matched Checksum:{result.MatchedChecksums} Chunk Retries:{result.ChunkRetries} Time:{sw1.ElapsedMilliseconds}ms");
                }

                return result;
            }
            catch (Exception ex)
            {
                if (tmpDestinationFile != null)
                    try { File.Delete(tmpDestinationFile); } catch { }

                if (ex is OperationCanceledException)
                    throw;

                if (options.Logging)
                    Console.WriteLine($"error: Source:{file} {ex.Message} | {ex.InnerException} | {ex.StackTrace}");

                return null;
            }
        }


        private static async Task<FileResult> CopyFile(string file, string tmpDestinationFile, CopyOptions options, CancellationToken cancellationToken)
        {
            var bufferSize = options.BufferSize;

            int matchedCheckSums = 0;
            int chunkRetries = 0;
            int abort = 0;
            long offset = 0;

            //source stays locked for the whole copy so it can't change underneath us
            using SafeFileHandle fsread = File.OpenHandle(file, FileMode.Open, FileAccess.Read, FileShare.None, FileOptions.Asynchronous | FileOptions.SequentialScan);

            var length = RandomAccess.GetLength(fsread);

            //one handle for writing and reading back, preallocated to avoid fragmentation
            using SafeFileHandle fswrite = File.OpenHandle(tmpDestinationFile, FileMode.Create, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous, length);

            var dataArray = ArrayPool<byte>.Shared.Rent(bufferSize);
            var checkArray = options.Verify ? ArrayPool<byte>.Shared.Rent(bufferSize) : null;

            try
            {
                for (; ; )
                {
                    int read = await RandomAccess.ReadAsync(fsread, dataArray.AsMemory(0, bufferSize), offset, cancellationToken);
                    if (read == 0)
                        break;

                    await RandomAccess.WriteAsync(fswrite, dataArray.AsMemory(0, read), offset, cancellationToken);

                    if (checkArray == null)
                    {
                        offset += read;
                        continue;
                    }

                    var sourceChecksum = XxHash3.HashToUInt64(dataArray.AsSpan(0, read));

                    //written data goes straight to the OS, read it back through the same handle and compare
                    int checkRead = 0;
                    while (checkRead < read)
                    {
                        int r = await RandomAccess.ReadAsync(fswrite, checkArray.AsMemory(checkRead, read - checkRead), offset + checkRead, cancellationToken);
                        if (r == 0)
                            break;
                        checkRead += r;
                    }

                    ////code to test chunk retries
                    //if (Random.Shared.Next(1, 10000) < 1000)
                    //    checkRead = 0;

                    if (checkRead == read && sourceChecksum == XxHash3.HashToUInt64(checkArray.AsSpan(0, read)))
                    {
                        abort = 0;
                        matchedCheckSums++;
                        offset += read;
                    }
                    else
                    {
                        chunkRetries++;

                        abort++;

                        if (abort > options.Retries)
                            throw new Exception($"xxHash not matching after {options.Retries} attempts!");

                        //offset stays the same so the chunk is read and written again
                        await Task.Delay(100, cancellationToken);
                    }
                }

                //preallocation can leave the file longer than what was actually written
                if (offset != length)
                    RandomAccess.SetLength(fswrite, offset);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(dataArray);

                if (checkArray != null)
                    ArrayPool<byte>.Shared.Return(checkArray);
            }

            return new FileResult(offset, matchedCheckSums, chunkRetries, false);
        }
    }
}
