package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func newTestServer(t *testing.T) (*server, *httptest.Server) {
	t.Helper()
	dir, err := os.MkdirTemp("", "chdbsrv-")
	if err != nil {
		t.Fatal(err)
	}
	s := newServer(dir)
	t.Cleanup(func() {
		s.db.Close()
		os.RemoveAll(dir)
	})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handleRootPost(w, r)
	}))
	t.Cleanup(ts.Close)
	return s, ts
}

func post(t *testing.T, ts *httptest.Server, query string, body string) string {
	t.Helper()
	resp, err := http.Post(ts.URL+"/?query="+url.QueryEscape(query), "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return string(b)
}

func postDB(t *testing.T, ts *httptest.Server, database, query, body string) string {
	t.Helper()
	resp, err := http.Post(ts.URL+"/?query="+url.QueryEscape(query)+"&database="+url.QueryEscape(database),
		"text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return string(b)
}

func TestQueryBasic(t *testing.T) {
	_, ts := newTestServer(t)
	if got := post(t, ts, "SELECT 1", ""); strings.TrimSpace(got) != "1" {
		t.Fatalf("got %q", got)
	}
}

func TestQueryFormatNative(t *testing.T) {
	_, ts := newTestServer(t)
	got := post(t, ts, "SELECT 1 FORMAT Native", "")
	if got == "1\n" || len(got) == 0 {
		t.Fatalf("expected raw Native bytes, got %q", got)
	}
}

func TestExtractFormat(t *testing.T) {
	cases := []struct{ in, sql, format, data string }{
		{"SELECT 1 FORMAT Native", "SELECT 1", "Native", ""},
		{"SELECT 1 format native", "SELECT 1", "native", ""},
		{"SELECT 1\n FORMAT Native", "SELECT 1", "Native", ""},
		{"INSERT INTO t FORMAT Native\n\x01\x02", "INSERT INTO t", "Native", "\x01\x02"},
		{"SELECT 1", "SELECT 1", "", ""},
	}
	for _, c := range cases {
		s, f, d := extractFormat(c.in)
		if s != c.sql || f != c.format || d != c.data {
			t.Errorf("extractFormat(%q) = (%q,%q,%q), want (%q,%q,%q)", c.in, s, f, d, c.sql, c.format, c.data)
		}
	}
}

// TestInsertNativeEndToEnd is the regression test for the silent-failure bug:
// a Native INSERT used to return success while storing nothing.
func TestInsertNativeEndToEnd(t *testing.T) {
	_, ts := newTestServer(t)
	post(t, ts, "CREATE TABLE t (id UInt8, s String) ENGINE=Memory", "")

	// Native block exactly as chDB emits it for (1,'a'),(2,'b').
	native := "\x02\x01\x02id\x05UInt8\x01\x01s\x06String\x01a" +
		"\x02\x01\x02id\x05UInt8\x02\x01s\x06String\x01b"

	post(t, ts, "INSERT INTO t (id, s) FORMAT Native", native)

	got := post(t, ts, "SELECT id, s FROM t ORDER BY id", "")
	if strings.TrimSpace(got) != "1\ta\n2\tb" {
		t.Fatalf("rows = %q", got)
	}
}

// TestInsertNativeWithDatabase covers clients that send "?database=..." and
// then a Native INSERT: the stream API rejects multi-statement SQL, so the
// database must be selected separately.
func TestInsertNativeWithDatabase(t *testing.T) {
	_, ts := newTestServer(t)
	post(t, ts, "CREATE DATABASE db1", "")
	postDB(t, ts, "db1", "CREATE TABLE t (id UInt8, s String) ENGINE=Memory", "")

	native := "\x02\x01\x02id\x05UInt8\x01\x01s\x06String\x01a" +
		"\x02\x01\x02id\x05UInt8\x02\x01s\x06String\x01b"
	postDB(t, ts, "db1", "INSERT INTO t (id, s) FORMAT Native", native)

	if got := postDB(t, ts, "db1", "SELECT id, s FROM t ORDER BY id", ""); strings.TrimSpace(got) != "1\ta\n2\tb" {
		t.Fatalf("rows = %q", got)
	}
}

// TestClickHouseConnectNativeInsert drives a real clickhouse_connect client,
// which sends INSERT bodies as "INSERT ... FORMAT Native\n<binary>".
func TestClickHouseConnectNativeInsert(t *testing.T) {
	py := "/tmp/ccvenv/bin/python"
	if _, err := os.Stat(py); err != nil {
		t.Skip("test venv not available")
	}
	_, ts := newTestServer(t)
	host := strings.Split(strings.Split(ts.URL, "//")[1], ":")[0]
	port := strings.Split(ts.URL, ":")[2]

	script := `
import sys, clickhouse_connect
c = clickhouse_connect.get_client(host=sys.argv[1], port=int(sys.argv[2]))
c.command('CREATE TABLE t2 (id Int32, s String) ENGINE=Memory')
c.insert('t2', [[1, 'a'], [2, 'b']], column_names=['id', 's'])
rows = c.query('SELECT id, s FROM t2 ORDER BY id').result_rows
assert rows == [(1, 'a'), (2, 'b')], rows
print('ONECLIENT_OK', rows)
`
	out, err := exec.Command(py, "-c", script, host, port).CombinedOutput()
	if err != nil {
		t.Fatalf("client failed: %v\n%s", err, out)
	}
	t.Logf("%s", out)
}
