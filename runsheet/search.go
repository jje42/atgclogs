package runsheet

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"
)

type searchResult struct {
	runSheet RunSheet
	err      error
}

func Find(runSheetFolder string, excludeList []string) []string {
	fs, _ := ioutil.ReadDir(runSheetFolder)
	runSheetFiles := []string{}
	for _, f := range fs {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "NovaSeq") && strings.HasSuffix(f.Name(), ".xlsx") {
			if isNotExcluded(f.Name(), excludeList) {
				fn := filepath.Join(runSheetFolder, f.Name())
				runSheetFiles = append(runSheetFiles, fn)
			}
		}
	}
	return runSheetFiles
}

func readRunSheets(runSheetFolder string, excludeList []string) chan searchResult {
	fs, _ := ioutil.ReadDir(runSheetFolder)
	runSheetFiles := []string{}
	for _, f := range fs {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "NovaSeq") && strings.HasSuffix(f.Name(), ".xlsx") {
			if isNotExcluded(f.Name(), excludeList) {
				fn := filepath.Join(runSheetFolder, f.Name())
				runSheetFiles = append(runSheetFiles, fn)
			}
		}
	}

	c := make(chan searchResult)
	var wg sync.WaitGroup

	for _, fn := range runSheetFiles {
		wg.Add(1)
		go func(fn string, c chan searchResult, wg *sync.WaitGroup) {
			defer wg.Done()
			runsheet, err := New(fn)
			c <- searchResult{runsheet, err}
		}(fn, c, &wg)
	}
	go func() {
		wg.Wait()
		close(c)
	}()
	return c
}

func ParseRunsheets(runSheetFolder string, excludeList []string) ([]RunSheet, error) {
	xs := []RunSheet{}
	c := readRunSheets(runSheetFolder, excludeList)
	hasError := false
	for result := range c {
		if result.err == nil {
			xs = append(xs, result.runSheet)
		} else {
			log.Printf("Error reading %s: %v", result.runSheet.Filename, result.err)
			hasError = true
		}
	}
	if hasError {
		return []RunSheet{}, fmt.Errorf("failed to parse all runsheets")
	}
	return xs, nil
}

func isNotExcluded(fn string, excludeList []string) bool {
	ret := true
	for _, f := range excludeList {
		if f == fn {
			ret = false
			break
		}
	}
	return ret
}
