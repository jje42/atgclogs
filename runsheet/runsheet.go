package runsheet

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/xuri/excelize/v2"
)

const sheetName = "SampleRunSheet"

type RunSheet struct {
	Filename  string
	f         *excelize.File
	Header    Header
	headerMap map[string]int
	dataIdx   int
	sheetName string
	Samples   []Sample
}

type Header struct {
	SequencingStartDate string `csv:"sequencing_start_date"`
	InstrumentName      string `csv:"instrument_name"`
	RunNumber           string `csv:"run_number"`
	FlowCellPosition    string `csv:"flowcell_position"`
	FlowCellID          string `csv:"flowcell_id"`
	RunName             string `csv:"run_name"`
	FlowCellType        string `csv:"flowcell_type"`
	Version             string `csv:"version"`
	RunType             string `csv:"run_type"`
	Workflow            string `csv:"workflow"`
	Indexing            string `csv:"indexing"`
	Read1Cycles         int    `csv:"read1_cycles"`
	Read2Cycles         int    `csv:"read2_cycles"`
	I7IndexReadCycles   int    `csv:"i7_index_read_cycles"`
	I5IndexReadCycles   int    `csv:"i5_index_read_cycles"`
}

func New(fn string) (RunSheet, error) {
	f, err := excelize.OpenFile(fn)
	if err != nil {
		return RunSheet{Filename: fn}, err
	}
	cols, err := f.Cols(sheetName)
	if err != nil {
		return RunSheet{Filename: fn}, err
	}
	cols.Next()
	col, err := cols.Rows()
	if err != nil {
		return RunSheet{Filename: fn}, err
	}
	dataIdx := -1
	for i, rowCell := range col {
		if rowCell == "[Data]" {
			dataIdx = i
		}
	}
	headerRow := dataIdx + 3
	headerMap := make(map[string]int)
	header := Header{}
	rows, err := f.Rows(sheetName)
	if err != nil {
		return RunSheet{Filename: fn}, err
	}
	rowIdx := -1

	for i := 1; i < dataIdx; i++ {
		axis, err := excelize.CoordinatesToCellName(1, i)
		if err != nil {
			return RunSheet{Filename: fn}, err
		}
		key, err := f.GetCellValue(sheetName, axis)
		if err != nil {
			return RunSheet{Filename: fn}, err
		}
		axis, err = excelize.CoordinatesToCellName(2, i)
		if err != nil {
			return RunSheet{Filename: fn}, err
		}
		value, err := f.GetCellValue(sheetName, axis)
		if err != nil {
			return RunSheet{Filename: fn}, err
		}
		switch key {
		case "Sequencing Start Date":
			if value == "" {
				return RunSheet{Filename: fn}, fmt.Errorf("sequencing start date is empty: %s", filepath.Base(fn))
			}
			t, err := time.Parse("01-02-06", value)
			if err != nil {
				return RunSheet{Filename: fn}, fmt.Errorf("unable to parse sequencing start date: %w", err)
			}
			header.SequencingStartDate = t.Format("02/01/2006")
		case "Instrument Name":
			header.InstrumentName = value
		case "Run Number":
			header.RunNumber = value
		case "Flow Cell Position":
			header.FlowCellPosition = value
			if !(value == "A" || value == "B") {
				return RunSheet{Filename: fn}, fmt.Errorf("flow cell positions was %s. Expected A or B", value)
			}
		case "Flow Cell ID":
			header.FlowCellID = value
		case "Run Name":
			header.RunName = value
		case "Flow Cell Type":
			header.FlowCellType = value
		case "Version":
			header.Version = value
		case "Run Type":
			header.RunType = value
		case "Workflow":
			header.Workflow = value
		case "Indexing":
			header.Indexing = value
		case "Read 1 Cycles":
			n, err := strconv.Atoi(value)
			if err != nil {
				return RunSheet{Filename: fn}, err
			}
			header.Read1Cycles = n
		case "Read 2 Cycles":
			n, err := strconv.Atoi(value)
			if err != nil {
				return RunSheet{Filename: fn}, err
			}
			header.Read2Cycles = n
		case "i7 Index Read Cycles":
			n, err := strconv.Atoi(value)
			if err != nil {
				return RunSheet{Filename: fn}, err
			}
			header.I7IndexReadCycles = n
		case "i5 Index Read Cycles":
			n, err := strconv.Atoi(value)
			if err != nil {
				return RunSheet{Filename: fn}, err
			}
			header.I5IndexReadCycles = n
		}
	}

	for rows.Next() {
		rowIdx++
		row, err := rows.Columns()
		if err != nil {
			return RunSheet{Filename: fn}, err
		}
		if len(row) == 0 {
			return RunSheet{Filename: fn}, fmt.Errorf("found 0 length row: %d", rowIdx)
		}

		if rowIdx == headerRow {
			for i, col := range row {
				headerMap[col] = i
			}
		}
		if rowIdx > headerRow {
			break
		}
	}
	if err := rows.Error(); err != nil {
		return RunSheet{Filename: fn}, err
	}
	if header.FlowCellID == "0.0" || header.FlowCellID == "0" {
		return RunSheet{Filename: fn}, fmt.Errorf("missing flowcell ID")
	}
	r := RunSheet{
		Filename:  fn,
		f:         f,
		Header:    header,
		headerMap: headerMap,
		dataIdx:   dataIdx,
		sheetName: sheetName,
	}
	scanner := r.NewScanner()
	for scanner.Scan() {
		sample := scanner.Sample()
		if sample.UIN == "" {
			return RunSheet{Filename: fn}, fmt.Errorf("failed to scan runsheet: missing sample UIN")
		}
		r.Samples = append(r.Samples, sample)
	}
	if err := scanner.Error(); err != nil {
		return RunSheet{Filename: fn}, fmt.Errorf("failed to scan runsheet: %w", err)
	}
	return r, nil
}

func (r RunSheet) NewScanner() *Scanner {
	return &Scanner{
		r:      r,
		curRow: r.dataIdx + 4,
	}
}

type Sample struct {
	ID          string `csv:"id"`
	UIN         string `csv:"uin"`
	Lane        string `csv:"lane"`
	SubjectID   string `csv:"subject_id"`
	ProjectID   string `csv:"project_id"`
	Cohort      string `csv:"cohort"`
	LibraryType string `csv:"library_type"`
	CaptureType string `csv:"capture_type"`
	LibraryID   string `csv:"library_id"`
	CaptureID   string `csv:"capture_id"`
	Index       string `csv:"index"`
	Index2      string `csv:"index2"`
	I7IndexID   string `csv:"i7_index_id"`
	I5IndexID   string `csv:"i5_index_id"`
}

type Scanner struct {
	r          RunSheet
	err        error
	curRow     int
	nextSample Sample
}

func (s *Scanner) Scan() bool {
	id, err := getFormattedString(s.r, "SampleID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	if id == "" {
		s.err = nil
		return false
	}
	uin, err := getFormattedString(s.r, "SampleName", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	lane, err := getFormattedString(s.r, "LaneNumber", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	subjectID, err := getFormattedString(s.r, "SubjectID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	projectID, err := getFormattedString(s.r, "ProjectID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	cohort, err := getFormattedString(s.r, "Cohort", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	libraryType, err := getFormattedString(s.r, "LibraryType", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	captureType, err := getFormattedString(s.r, "CaptureType", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	libraryID, err := getFormattedString(s.r, "LibraryID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	captureID, err := getFormattedString(s.r, "CaptureID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	index, err := getFormattedString(s.r, "Index", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	index2, err := getFormattedString(s.r, "Index2", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	i7IndexID, err := getFormattedString(s.r, "I7indexiD", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	i5IndexID, err := getFormattedString(s.r, "I5indexiD", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	sample := Sample{
		ID:          id,
		UIN:         uin,
		Lane:        lane,
		SubjectID:   subjectID,
		ProjectID:   projectID,
		Cohort:      cohort,
		LibraryType: libraryType,
		CaptureType: captureType,
		LibraryID:   libraryID,
		CaptureID:   captureID,
		Index:       index,
		Index2:      index2,
		I7IndexID:   i7IndexID,
		I5IndexID:   i5IndexID,
	}
	s.nextSample = sample
	s.curRow++
	return true
}

func (s *Scanner) Sample() Sample {
	return s.nextSample
}

func (s *Scanner) Error() error {
	return s.err
}

// this is a repeat of the code in weslog.Scanner.getFormattedString. How can we
// reduce repetition?
func getFormattedString(r RunSheet, column string, rowIdx int) (string, error) {
	idx, ok := r.headerMap[column]
	if !ok {
		return "", fmt.Errorf("unable to find index for '%s' column", column)
	}
	cellName, err := excelize.CoordinatesToCellName(idx+1, rowIdx+1)
	if err != nil {
		return "", fmt.Errorf("failed to get coordinate: %w", err)
	}
	c, err := r.f.GetCellValue(sheetName, cellName)
	return c, err
}
