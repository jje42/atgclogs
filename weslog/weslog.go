package weslog

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

type Sample struct {
	Record               string
	UIN                  string `csv:"uin"`
	Status               string `csv:"status"`
	ReceiptDate          Date   `csv:"receipt_date"`
	SampleCollectionDate Date   `csv:"collection_date"`
	ConsentReceivedDate  Date   `csv:"consent_received_date"`
	URN                  string `csv:"urn"`
	FIN                  string `csv:"fin"`
	PatientName          string `csv:"patient_name"`
	DOB                  Date   `csv:"dob"`
	Gender               string `csv:"sex"`
	SubjectID            string `csv:"subject_id"`
	PreservationMethod   string `csv:"preservation_method"`
	SampleType           string `csv:"sample_type"`
	ReportType           string `csv:"report_type"`
	Disease              string `csv:"disease"`
	RequestingClinician  string `csv:"requesting_clinician"`
	Comments             string `csv:"comments"`
	ProjectID            string `csv:"project_id"`
	TissueSourceType     string `csv:"tissue_source_type"`
	NucleicType          string `csv:"nucleic_type"`
	PrimaryTumourSite    string `csv:"primary_tumour_site"`
	TumourContent        string `csv:"tumour_content"`
	RequestDate          string `csv:"request_date"`
	RequestingDoctor     string `csv:"requesting_doctor"`
	Consultant           string `csv:"consultant"`
	SourceLaboratory     string `csv:"source_laboratory"`
	Auslab               string `csv:"auslab"`
	AnatomicalPathology  string `csv:"anatomical_pathology"`
	EMR                  string `csv:"emr"`
}

type Scanner struct {
	err        error
	f          *excelize.File
	headerMap  map[string]int
	sheetName  string
	curRow     int
	nextSample Sample
}

func NewScanner(fn, password string) (*Scanner, error) {
	f, err := excelize.OpenFile(fn, excelize.Options{Password: password})
	if err != nil {
		return &Scanner{}, err
	}
	sheet := "ATG Sample Log"
	headerMap, err := getHeaderMap(f, sheet)
	if err != nil {
		return &Scanner{}, err
	}
	return &Scanner{
		err:       nil,
		curRow:    3,
		f:         f,
		sheetName: sheet,
		headerMap: headerMap,
	}, nil
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
		if i == 3 {
			for ci, col := range row {
				m[col] = ci
			}
			break
		}
	}
	return m, nil
}

func (s *Scanner) Scan() bool {
	sample, err := s.readSample()
	if err != nil {
		s.err = err
		return false
	}
	// Do
	for sample.Status == "NO TEST - Do No Process" || sample.Status == "Duplicate - Exclude" {
		s.curRow++
		sample, err = s.readSample()
		if err != nil {
			s.err = err
			return false
		}
	}
	if sample.UIN == "" && sample.SubjectID == "" {
		s.err = nil
		return false
	}
	// Validate sample.
	if sample.Gender != "MALE" && sample.Gender != "FEMALE" && sample.Gender != "" {
		s.err = fmt.Errorf("found a unknown gender: '%s'", sample.Gender)
		return false
	}
	//if !sample.ReceiptDate.IsZero() && !sample.SampleCollectionDate.IsZero() {
	//        if sample.ReceiptDate.Time.Before(sample.SampleCollectionDate.Time) {
	//                log.Printf("found a sample with a receipt date before collection date: %v\t%v (%s, row %d)",
	//                        sample.ReceiptDate, sample.SampleCollectionDate, sample.UIN, s.curRow+1)
	//        }
	//}
	//if !sample.SampleCollectionDate.IsZero() && !sample.DOB.IsZero() {
	//        if sample.SampleCollectionDate.Time.Before(sample.DOB.Time) {
	//                log.Printf("found sample collected before DOB: %s (row %d) %v %v", sample.UIN, s.curRow+1, sample.SampleCollectionDate, sample.DOB)
	//        }
	//}
	s.nextSample = sample
	s.err = nil
	s.curRow++
	return true
}

func (s *Scanner) readSample() (Sample, error) {
	uin, err := s.getFormattedString("SampleName", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	//if uin == "" {
	//s.err = nil
	//return false
	//}
	record, err := s.getFormattedString("Record", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	receiptDate, err := s.getTime("Receipt date", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	collectionDate, err := s.getTime("Sample Collection Date", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	consentDate, err := s.getTime("Consent Received date", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	urn, err := s.getFormattedString("UR", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	fin, err := s.getFormattedString("FIN", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	name, err := s.getFormattedString("PatientName", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	dob, err := s.getTime("DOB", s.curRow)
	if err != nil {
		// return Sample{}, fmt.Errorf("failed to get DOB: %w", err)
		log.Printf("failed to get DOB, using null value: %s", err)
		dob = time.Time{}

	}
	gender, err := s.getFormattedString("Gender", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	subjectID, err := s.getFormattedString("SubjectID", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	preservationMethod, err := s.getFormattedString("TissuePreservationType", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	sampleType, err := s.getFormattedString("SampleType", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	reportType, err := s.getFormattedString("ReportType", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	disease, err := s.getFormattedString("Disease", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	requestingClinician, err := s.getFormattedString("Requesting Clinician", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	comments, err := s.getFormattedString("Comments", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	status, err := s.getFormattedString("Status", s.curRow)
	if err != nil {
		return Sample{}, err
	}

	projectID, err := s.getFormattedString("ProjectID", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	tissueSourceType, err := s.getFormattedString("TissueSourceType", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	nucleicType, err := s.getFormattedString("NucleicType", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	primaryTumourSite, err := s.getFormattedString("Primary Tumour Site", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	tumourContent, err := s.getFormattedString("Tumour Content", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	requestDate, err := s.getFormattedString("Request Date", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	requestingDoctor, err := s.getFormattedString("Requesting Doctor", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	consultant, err := s.getFormattedString("Consultant", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	sourceLaboratory, err := s.getFormattedString("Source Laboratory", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	auslab, err := s.getFormattedString("Auslab", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	anatomicalPathology, err := s.getFormattedString("Anatomical Pathology", s.curRow)
	if err != nil {
		return Sample{}, err
	}
	emr, err := s.getFormattedString("EMR", s.curRow)
	if err != nil {
		return Sample{}, err
	}

	sample := Sample{
		Record:               record,
		UIN:                  uin,
		Status:               status,
		ReceiptDate:          Date{receiptDate},
		SampleCollectionDate: Date{collectionDate},
		ConsentReceivedDate:  Date{consentDate},
		URN:                  urn,
		FIN:                  fin,
		PatientName:          name,
		DOB:                  Date{dob},
		Gender:               strings.ToUpper(gender),
		SubjectID:            subjectID,
		PreservationMethod:   preservationMethod,
		SampleType:           sampleType,
		ReportType:           reportType,
		Disease:              disease,
		RequestingClinician:  requestingClinician,
		Comments:             comments,
		ProjectID:            projectID,
		TissueSourceType:     tissueSourceType,
		NucleicType:          nucleicType,
		PrimaryTumourSite:    primaryTumourSite,
		TumourContent:        tumourContent,
		RequestDate:          requestDate,
		RequestingDoctor:     requestingDoctor,
		Consultant:           consultant,
		SourceLaboratory:     sourceLaboratory,
		Auslab:               auslab,
		AnatomicalPathology:  anatomicalPathology,
		EMR:                  emr,
	}
	return sample, nil
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

func (s *Scanner) getTime(column string, rowIdx int) (time.Time, error) {
	idx, ok := s.headerMap[column]
	if !ok {
		return time.Time{}, fmt.Errorf("unable to find index for '%s' column", column)
	}
	cellName, err := excelize.CoordinatesToCellName(idx+1, rowIdx+1)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get coordinate: %w", err)
	}
	c, err := GetRawCellValue(s.f, s.sheetName, cellName)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get raw cell value: %w", err)
	}
	c = strings.TrimSpace(c)
	if c == "NA" || c == "" {
		return time.Time{}, nil
	}
	n, err := strconv.ParseFloat(c, 64)
	if err != nil {
		// If the cell's raw value can not be passed as a float, it is
		// not a true date. Assuming it is a date in string format, try
		// possible formats.
		formats := []string{
			"02-Jan-2006",
			"2-Jan-2006",
			"02/01/2006",
			"2/01/2006",
			"02-Jan-06",
			"2-Jan-06",
		}
		// s := strings.TrimSpace(c)
		for _, format := range formats {
			t, err := time.Parse(format, c)
			if err == nil {
				return t, nil
			}
		}
		return time.Time{}, err
	}
	t, err := excelize.ExcelDateToTime(n, false)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func GetRawCellValue(f *excelize.File, sheet, axis string) (string, error) {
	styleID, err := f.GetCellStyle(sheet, axis)
	if err != nil {
		return "", fmt.Errorf("unable to get cell style: %w", err)
	}
	defer func() {
		f.SetCellStyle(sheet, axis, axis, styleID)
	}()
	err = f.SetCellStyle(sheet, axis, axis, 0)
	if err != nil {
		return "", fmt.Errorf("unable to set cell style: %w", err)
	}
	value, err := f.GetCellValue(sheet, axis)
	if err != nil {
		return "", fmt.Errorf("failed to get cell value: %w", err)
	}
	return value, nil
}

func New(fn, password string) (map[string]Sample, error) {
	scanner, err := NewScanner(fn, password)
	if err != nil {
		return nil, fmt.Errorf("unable to create log scanner: %w", err)
	}
	sampleLog := make(map[string]Sample)
	for scanner.Scan() {
		sample := scanner.Sample()
		sampleLog[sample.UIN] = sample
	}
	if err := scanner.Error(); err != nil {
		return nil, fmt.Errorf("failed to scan log: %w", err)
	}
	return sampleLog, nil
}
