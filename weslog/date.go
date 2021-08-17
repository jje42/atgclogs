package weslog

import (
	"strconv"
	"strings"
	"time"
)

// Date ...
type Date struct {
	time.Time
}

const dateLayout = "2006-01-02"

// UnmarshalJSON ...
func (date *Date) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		date.Time = time.Time{}
		return
	}
	date.Time, err = time.Parse(dateLayout, s)
	return
}

// MarshalJSON ...
func (date Date) MarshalJSON() ([]byte, error) {
	if date.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(strconv.Quote(date.Time.Format(dateLayout))), nil
}

func (date Date) String() string {
	return date.Time.Format(dateLayout)
}

// IsUndefined ...
func (date Date) IsUndefined() bool {
	return date == Date{time.Time{}}
}

func (date *Date) UnmarshalCSV(csv string) (err error) {
	s := strings.Trim(csv, "\"")
	if s == "" {
		date.Time = time.Time{}
		return
	}
	date.Time, err = time.Parse(dateLayout, s)
	return
}

// MarshalJSON ...
func (date Date) MarshalCSV() (string, error) {
	if date.Time.IsZero() {
		// return "null", nil
		return "", nil
	}
	return date.Time.Format(dateLayout), nil
}
