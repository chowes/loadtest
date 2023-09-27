# loadtest
A parallel file system load test and batch file generator

# Usage

## Parameters

```
  -create-dirs
        each thread creates files in a separate directory
  -delete-files
        delete created files at the end of the test
  -file-size int
        size of data to write to file (default 1048576)
  -files-per-thread int
        number of files that should be written by each writer (default 1)
  -num-readers int
        number of readers
  -num-writers int
        number of writers
  -path string
        test directory into which files will be written
  -reads-per-thread int
        number of times each reader should recurslively list files (default 1)
```

## Example:

```
go run loadtest.go \
  --path test-dir \
  --num-writers 32 \
  --files-per-thread 32768 \
  --file-size 4096 \
  --num-readers 32 \
  --reads-per-thread 32768 \
  --create-dirs \
  --delete-files
```
