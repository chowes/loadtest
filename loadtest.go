package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	fileNameLen = 10
	bufferSize  = 4096
)

var (
	charSet = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randomString(length int) (string, error) {
	var sb strings.Builder
	charSetSize := big.NewInt(int64(len(charSet)))

	for i := 0; i < length; i++ {
		n, err := rand.Int(rand.Reader, charSetSize)
		if err != nil {
			return "", fmt.Errorf("failed to generate random character: %v", err)
		}
		sb.WriteRune(charSet[n.Int64()])
	}

	return sb.String(), nil
}

func deleteFiles(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read files in directory %q: %v", dir, err)
	}

	for _, f := range files {
		err := os.Remove(filepath.Join(dir, f.Name()))
		if err != nil {
			return fmt.Errorf("failed to remove file %q: %v", f.Name(), err)
		}
	}

	return nil
}

func writer(testDir string, fileSize int, numFiles int, createDir bool) error {
	dirPath := testDir
	if createDir {
		dirName, err := randomString(fileNameLen)
		if err != nil {
			return fmt.Errorf("failed to generate directory name: %v", err)
		}
		dirPath = filepath.Join(testDir, dirName)
	}

	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		return fmt.Errorf("failed to create directory %q: %v", dirPath, err)
	}

	for i := 0; i < numFiles; i++ {
		err := writeRandomFile(dirPath, fileSize)
		if err != nil {
			return err
		}
	}

	return nil
}

func recursiveListDir(dir string) error {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read files in directory %q", dir)
	}

	for _, f := range files {
		if f.IsDir() {
			err := recursiveListDir(filepath.Join(dir, f.Name()))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func reader(testDir string, readsPerThread int) error {
	for i := 0; i < readsPerThread; i++ {
		err := recursiveListDir(testDir)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeRandomFile(testDir string, fileSize int) error {
	fileName, err := randomString(fileNameLen)
	if err != nil {
		return fmt.Errorf("failed to generate file name: %v", err)
	}
	path := filepath.Join(testDir, fileName)

	f, err := os.Create(path)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("failed to create file %q: %v", fileName, err)
	}

	bytesRemaining := fileSize
	for bytesRemaining > 0 {
		buffer := make([]byte, min(bufferSize, bytesRemaining))
		_, err := rand.Read(buffer)
		if err != nil {
			return fmt.Errorf("failed to read random bytes: %v", err)
		}
		n, err := f.Write(buffer)
		if err != nil {
			return fmt.Errorf("failed to write bytes to file %q: %v", fileName, err)
		}
		bytesRemaining -= n
	}

	return nil
}

func startWriters(testPath string, fileSize, numWriters, numFiles int, createDirs bool, wg *sync.WaitGroup) error {
	for i := 0; i < numWriters; i++ {
		threadnum := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := writer(testPath, fileSize, numFiles, createDirs)
			if err != nil {
				fmt.Printf("writer thread %d failed: %v\n", threadnum, err)
			}
			fmt.Printf("writer thread %d finished\n", threadnum)
		}()
	}

	return nil
}

func startReaders(testPath string, numReaders, readsPerThread int, wg *sync.WaitGroup) error {
	for i := 0; i < numReaders; i++ {
		threadnum := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := reader(testPath, readsPerThread)
			if err != nil {
				fmt.Printf("reader thread %d failed: %v\n", threadnum, err)
			}
			fmt.Printf("reader thread %d finished\n", threadnum)
		}()
	}

	return nil
}

func cleanup(testPath string, wg *sync.WaitGroup) error {
	files, err := ioutil.ReadDir(testPath)
	if err != nil {
		return fmt.Errorf("failed to read files in directory %q", testPath)
	}

	for i, f := range files {
		filePath := filepath.Join(testPath, f.Name())
		if !f.IsDir() {
			err := os.Remove(filePath)
			if err != nil {
				fmt.Printf("failed to delete file %q", filePath, err)
			}
			continue
		}

		threadnum := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := os.RemoveAll(filePath)
			if err != nil {
				fmt.Printf("failed to recursively delete %q", filePath, err)
			}

			fmt.Printf("deleter thread %d finished\n", threadnum)
		}()
	}

	return nil
}

func main() {
	numWriters := flag.Int("num-writers", 0, "number of writers")
	numReaders := flag.Int("num-readers", 0, "number of readers")
	testPath := flag.String("path", "", "test directory into which files will be written")
	fileSize := flag.Int("file-size", 1048576, "size of data to write to file")
	filesPerThread := flag.Int("files-per-thread", 1, "number of files that should be written by each writer")
	readsPerThread := flag.Int("reads-per-thread", 1, "number of times each reader should recurslively list files")
	deleteFiles := flag.Bool("delete-files", false, "delete created files at the end of the test")
	createDirs := flag.Bool("create-dirs", false, "each thread creates files in a separate directory")

	flag.Parse()

	var wg sync.WaitGroup

	err := startWriters(*testPath, *fileSize, *numWriters, *filesPerThread, *createDirs, &wg)
	if err != nil {
		fmt.Printf("failed to start writers: %v", err)
	}
	err = startReaders(*testPath, *numReaders, *readsPerThread, &wg)
	if err != nil {
		fmt.Printf("failed to start readers: %v", err)
	}

	fmt.Println("Waiting for readers and writers to finish...")
	wg.Wait()

	if !*deleteFiles {
		return
	}

	err = cleanup(*testPath, &wg)
	if err != nil {
		fmt.Printf("failed to start deleters: %v\n", err)
	}

	fmt.Println("Waiting for deleters to finish...")
	wg.Wait()
}
