package runsheet

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
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

func isRunsheetFile(f os.FileInfo) bool {
	return !f.IsDir() && (strings.HasPrefix(f.Name(), "NovaSeq") || strings.HasPrefix(f.Name(), "NextSeq")) && strings.HasSuffix(f.Name(), ".xlsx")
}

func readRunSheets(runSheetFolder string, excludeList []string) chan searchResult {
	fs, _ := ioutil.ReadDir(runSheetFolder)
	runSheetFiles := []string{}
	for _, f := range fs {
		if isRunsheetFile(f) {
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

func FindByNumber(n int, runsheetdir string) (RunSheet, error) {
	num := fmt.Sprintf("%04d", n)
	sheet, err := findByLookup(num, runsheetdir)
	if err != nil {
		sheet, err = findByParse(num, runsheetdir)
		if err != nil {
			return RunSheet{}, fmt.Errorf("failed to find run sheet: %w", err)
		}
	}
	return sheet, nil
}

func findByParse(num, runsheetdir string) (RunSheet, error) {
	sheets, err := ParseRunsheets(runsheetdir, []string{})
	if err != nil {
		return RunSheet{}, err
	}
	if len(sheets) == 0 {
		return RunSheet{}, errors.New("no run sheets found in directory")
	}
	for _, sheet := range sheets {
		if sheet.Header.RunNumber == num {
			return sheet, nil
		}
	}
	return RunSheet{}, errors.New("failed to identify run sheet")
}

func findByLookup(num string, runsheetdir string) (RunSheet, error) {
	r, err := os.Open(filepath.Join(runsheetdir, "lookup.csv"))
	if err != nil {
		return RunSheet{}, err
	}
	defer r.Close()
	reader := csv.NewReader(r)
	fn := ""
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return RunSheet{}, err
		}
		if record[0] == num {
			fn = record[1]
		}
	}
	if fn == "" {
		return RunSheet{}, errors.New("failed to identify run sheet")
	}

	return New(fn)
}
