package runsheet

import (
	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
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
	SequencingStartDate string
	InstrumentName      string
	RunNumber           string
	FlowCellPosition    string
	FlowCellID          string
	RunName             string
	FlowCellType        string
	Version             string
	RunType             string
	Workflow            string
	Indexing            string
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
	for rows.Next() {
		rowIdx++
		row, err := rows.Columns()
		if err != nil {
			return RunSheet{Filename: fn}, err
		}
		switch row[0] {
		case "Sequencing Start Date":
			header.SequencingStartDate = row[1]
		case "Instrument Name":
			header.InstrumentName = row[1]
		case "Run Number":
			header.RunNumber = row[1]
		case "Flow Cell Position":
			header.FlowCellPosition = row[1]
		case "Flow Cell ID":
			header.FlowCellID = row[1]
		case "Run Name":
			header.RunName = row[1]
		case "Flow Cell Type":
			header.FlowCellType = row[1]
		case "Version":
			header.Version = row[1]
		case "Run Type":
			header.RunType = row[1]
		case "Workflow":
			header.Workflow = row[1]
		case "Indexing":
			header.Indexing = row[1]
		}
		if rowIdx == headerRow {
			for i, col := range row {
				headerMap[col] = i
			}
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
	ID          string
	UIN         string
	Lane        string
	SubjectID   string
	ProjectID   string
	Cohort      string
	LibraryType string
	CaptureType string
	LibraryID   string
	CaptureID   string
	Index       string
	Index2      string
	I7IndexID   string
	I5IndexID   string
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
