package mysql

import (
	"bytes"
	"database/sql"
	"testing"
)

func TestCompressedRawBytes(t *testing.T) {
	runTests(t, dsn+"&compress=true", func(dbt *DBTest) {
		v1 := []byte("aaa")
		v2 := []byte("bbb")
		rows := dbt.mustQuery("SELECT ?, ?", v1, v2)
		defer rows.Close()
		if rows.Next() {
			var o1, o2 sql.RawBytes
			if err := rows.Scan(&o1, &o2); err != nil {
				dbt.Errorf("Got error: %v", err)
			}
			if !bytes.Equal(v1, o1) {
				dbt.Errorf("expected %v, got %v", v1, o1)
			}
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
			o1 = append(o1, "xyzzy"...)
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
		} else {
			dbt.Errorf("no data")
		}
	})
}
