# FastFileCopy

A C# console application that copies or moves a directory tree as fast as possible. Files are processed in parallel and every chunk written is read back and checked with xxHash3, retrying a chunk if it doesn't match. Files are written to a `.tmp` file first and only renamed into place once complete.

## Usage

```
FastFileCopy <source> <destination> [options]

  -m, --move                   Move files instead of copying them
  -t, --threads <threads>      Maximum number of files processed in parallel [default: 10]
  -p, --pattern <pattern>      Search pattern for files to include [default: *]
  --no-recurse                 Only process the top directory, skip subdirectories
  -n, --max-files <max-files>  Maximum number of files to process (default: all files)
  -r, --retries <retries>      Checksum mismatches allowed per chunk before giving up on a file (100ms delay between attempts) [default: 5]
  --no-verify                  Skip checksum verification of written chunks (fastest)
  --no-timestamps              Don't copy creation, last write and last access times to the destination
  -b, --buffer <buffer>        Chunk size in KB used for reading, writing and verification [default: 1024]
  -q, --quiet                  Don't log anything except the final summary
  -?, -h, --help               Show help and usage information
  --version                    Show version information
```

Examples:

```
FastFileCopy "C:\temp1" "C:\temp2" > log.txt
FastFileCopy "C:\temp1" "C:\temp2" --move --threads 500 --pattern "*.xml" > log.txt
FastFileCopy "C:\temp2" "C:\temp1" --move -t 200 -p "*.xml" --no-recurse --quiet
FastFileCopy "C:\temp1" "D:\backup" --no-timestamps
```

The exit code is `0` when every file succeeded and `1` if any file failed, the arguments were invalid or the run was cancelled with Ctrl+C.

## Upgrading from 1.x

Settings are now named options instead of positional numbers:

| 1.x position | 2.0 option |
|---|---|
| 3 Move/Copy `0` | `--move` |
| 4 MaxDegreeOfParallelism | `--threads` |
| 5 Logging `0` | `--quiet` |
| 6 Abort | `--retries` |
| 7 SearchPattern | `--pattern` |
| 8 RecurseSubdirectories `0` | `--no-recurse` |
| 9 MaxGrab | `--max-files` |

`"C:\temp1" "C:\temp2" 0 500 1 5 "*.xml" 1` becomes `"C:\temp1" "C:\temp2" --move --threads 500 --retries 5 --pattern "*.xml"`.

Other changes:

- Targets .NET 10, uses the built in `System.IO.Hashing` xxHash3 and `Random.Shared.Shuffle`
- Moves within the same volume are a rename instead of a copy and delete
- 1MB chunks (configurable), pooled buffers, a single read/write handle for the destination with preallocation, no flush per chunk
- Only the bytes actually read are hashed (1.x hashed the whole buffer)
- Source timestamps are captured before reading, so the last access time is the real one
- Failed or cancelled copies no longer leave `.tmp` files behind
- Console output is buffered, which matters when logging thousands of files to a file
- No more "press any key" prompts, so it's safe to use in scripts
- A summary line with file count, failures, total size and throughput

## Build

```
dotnet publish -c Release -r win-x64 -p:PublishSingleFile=true --self-contained true
```
