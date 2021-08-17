package arraylog

import (
	"fmt"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
)

type Sample struct {
	UIN             string
	SubjectID       string
	ProjectID       string
	InfiniumID      string
	BeadChipVersion string
	SentrixID       string
	SentrixPosition string
}

type Scanner struct {
	err        error
	f          *excelize.File
	headerMap  map[string]int
	sheetName  string
	curRow     int
	nextSample Sample
}

func NewScanner(fn string) (*Scanner, error) {
	f, err := excelize.OpenFile(fn)
	if err != nil {
		return &Scanner{}, err
	}
	sheet := "IFM Queue"
	headerMap, err := getHeaderMap(f, sheet)
	if err != nil {
		return &Scanner{}, err
	}
	return &Scanner{
		err:       nil,
		f:         f,
		curRow:    2,
		headerMap: headerMap,
		sheetName: sheet,
	}, nil
}

func (s *Scanner) Scan() bool {
	uin, err := s.getFormattedString("UIN", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	if uin == "" {
		s.err = nil
		return false
	}
	subjectID, err := s.getFormattedString("SubjectID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	projectID, err := s.getFormattedString("ProjectID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	beadChipVersion, err := s.getFormattedString("Beadchip version", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	sentrixID, err := s.getFormattedString("BeadChip Sentrix ID", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	sentrixPosition, err := s.getFormattedString("Beadchip Sentrix Position", s.curRow)
	if err != nil {
		s.err = err
		return false
	}
	sample := Sample{
		UIN:             uin,
		SubjectID:       subjectID,
		ProjectID:       projectID,
		BeadChipVersion: beadChipVersion,
		SentrixID:       sentrixID,
		SentrixPosition: sentrixPosition,
	}
	s.nextSample = sample
	s.err = nil
	s.curRow++
	return true
}

func (s *Scanner) Error() error {
	return s.err
}

func (s *Scanner) Sample() Sample {
	return s.nextSample
}

func (s *Scanner) getFormattedString(column string, rowIdx int) (string, error) {
	idx, ok := s.headerMap[column]
	if !ok {
		return "", fmt.Errorf("unable to find index for '%s' column", column)
	}
	cellName, err := excelize.CoordinatesToCellName(idx+1, rowIdx+1)
	if err != nil {
		return "", fmt.Errorf("failed to get coordinate: %w", err)
	}
	c, err := s.f.GetCellValue(s.sheetName, cellName)
	if c == "NA" {
		c = ""
	}
	return c, err
}

func getHeaderMap(f *excelize.File, sheet string) (map[string]int, error) {
	m := make(map[string]int)
	i := 0
	rows, err := f.Rows(sheet)
	if err != nil {
		return m, err
	}
	for rows.Next() {
		i++
		row, err := rows.Columns()
		if err != nil {
			return m, err
		}
		if i == 2 {
			for ci, col := range row {
				m[col] = ci
			}
			break
		}
	}
	return m, nil
}
