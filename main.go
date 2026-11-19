package main

import (
	"bytes"
	"embed"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/pierrec/lz4/v4"
)

// Embedding the HTML file
//
//go:embed play.html
var content embed.FS

var (
	path string
)

func init() {
	path = os.Getenv("DATA_PATH")
	if path == "" {
		path = ".chdb_data"
	}
}

// formatClauseRe matches a "FORMAT <name>" clause (case-insensitive).
var formatClauseRe = regexp.MustCompile(`(?is)\s+FORMAT\s+([A-Za-z][A-Za-z0-9_]*)`)

// extractFormat splits a "FORMAT xxx" clause off the query.
//
// Everything after the format name is returned as data: clients such as
// clickhouse_connect send an INSERT as
//
//	INSERT INTO t (...) FORMAT Native\n<raw bytes>
//
// so that tail is row data and must be preserved byte for byte. data is empty
// when the clause merely ends the query ("SELECT 1 FORMAT Native").
func extractFormat(query string) (sql, format, data string) {
	loc := formatClauseRe.FindStringSubmatchIndex(query)
	if loc == nil {
		return query, "", ""
	}
	format = query[loc[2]:loc[3]]
	sql = strings.TrimSpace(query[:loc[0]])
	// A single trailing newline is just separator noise, not row data.
	data = strings.TrimLeft(query[loc[1]:], "\r\n")
	return sql, format, data
}

// queryWithParams runs query, binding "param_<name>" HTTP parameters as chDB
// {name:Type} placeholders via --param_<name> arguments.
func (s *server) queryWithParams(query, format string, params map[string]string) ([]byte, error) {
	var b strings.Builder
	for k, v := range params {
		fmt.Fprintf(&b, "SET param_%s = '%s';\n", strings.TrimPrefix(k, "param_"), strings.ReplaceAll(v, "'", "\\'"))
	}
	b.WriteString(query)
	return s.db.Query(b.String(), format)
}

// insertWithFormatRe reports whether a query is an INSERT that already carries
// its own FORMAT clause, meaning the request body is row data, not more SQL.
var insertWithFormatRe = regexp.MustCompile(`(?is)^\s*(USE\s+\S+\s*;\s*)?INSERT\b[\s\S]*\bFORMAT\s+[A-Za-z]`)

func isInsertWithFormat(query string) bool {
	return insertWithFormatRe.MatchString(query)
}

// insertDataPrefix matches the SQL head of a request body that carries inline
// row data: the statement up to and including its FORMAT clause, followed by a
// newline separator that ends the SQL text. Insert drivers send exactly this
// shape, e.g. "INSERT INTO t (a, b)\n FORMAT Native\n<binary>".
var insertDataPrefix = regexp.MustCompile(`(?is)^([\s\S]*?\bFORMAT\s+[A-Za-z][A-Za-z0-9_]*)\s*\r?\n`)

// splitInsertData reports whether the body is an INSERT statement followed by
// raw row data, returning the SQL head and the data bytes untouched.
func splitInsertData(body []byte) (head string, data []byte, ok bool) {
	if !isInsertWithFormat(string(body)) {
		return "", nil, false
	}
	loc := insertDataPrefix.FindSubmatchIndex(body)
	if loc == nil {
		return "", nil, false
	}
	// The trailing newline is part of the separator, not the data.
	return string(body[loc[2]:loc[3]]), body[loc[1]:], true
}

// insertRows writes raw format-encoded row data through chDB's streaming insert
// API, which is binary safe. The command-line path cannot be used here: argv is
// NUL-terminated, so Native blocks would be truncated at the first 0x00 byte,
// silently inserting nothing while still reporting success.
func (s *server) insertRows(database, query, format string, data []byte) error {
	return s.db.Insert(database, query, format, data)
}

type server struct {
	db *client
}

func newServer(path string) *server {
	db, err := newClient(path)
	if err != nil {
		log.Fatalf("Error creating session: %v", err)
	}
	return &server{db: db}
}

func (s *server) handleRootPost(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("query")
	format := r.URL.Query().Get("default_format")
	if format == "" {
		format = "TSV"
	}
	database := r.URL.Query().Get("database")
	// Server-side query parameters, sent by clickhouse_connect for {name:Type} placeholders.
	params := make(map[string]string)
	for k, vs := range r.URL.Query() {
		if strings.HasPrefix(k, "param_") && len(vs) > 0 {
			params[k] = vs[0]
		}
	}
	var body string
	var bodyBytes []byte
	if r.Body != nil {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		bodyBytes = decompressBody(r.Header.Get("Content-Encoding"), b)
		body = string(bodyBytes)
	}
	// The body either carries SQL or, for inserts, the raw row data. Row bytes
	// must never be whitespace-collapsed, so detect that shape first and only
	// fold the body when it is plain SQL.
	var rowData []byte
	if len(bodyBytes) > 0 {
		if head, data, ok := splitInsertData(bodyBytes); ok {
			// Body starts with its own "INSERT ... FORMAT X" statement.
			if query != "" {
				query += " "
			}
			query += head
			rowData = data
		} else if isInsertWithFormat(query) {
			// Statement came from the URL; the whole body is row data.
			rowData = bodyBytes
		} else {
			body = strings.Join(strings.Fields(body), " ")
			if query != "" {
				query += " "
			}
			query += body
		}
	}
	// Honor a "FORMAT xxx" clause, as sent by clients like clickhouse_connect.
	if q, f, d := extractFormat(query); f != "" {
		query, format = q, f
		if d != "" && len(rowData) == 0 {
			rowData = []byte(d)
		}
	}
	// connQuery carries the statement for the connection-based paths: they take
	// a single statement, so the database is selected separately.
	connQuery := query
	if database != "" {
		query = "USE " + database + "; " + query
	}
	log.Printf("Query: %s [format=%s] [data=%d bytes]\n", query, format, len(rowData))

	if len(rowData) > 0 {
		if err := s.insertRows(database, connQuery, format, rowData); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	output, err := s.queryWithParams(query, format, params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Native (binary) formats must be written verbatim.
	if _, err := w.Write(output); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func (s *server) handlePing(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Ok")
}

func (s *server) handlePlay(w http.ResponseWriter, r *http.Request) {
	data, err := content.ReadFile("play.html")
	if err != nil {
		http.Error(w, "Unable to open play.html", http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, "play.html", time.Now(), bytes.NewReader(data))
}

func (s *server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	data, err := content.ReadFile("play.html")
	if err != nil {
		http.Error(w, "Unable to open play.html", http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, "play.html", time.Now(), bytes.NewReader(data))
}

func main() {
	s := newServer(path)
	mux := http.NewServeMux()

	// Register specific handlers
	mux.HandleFunc("/play", s.handlePlay)
	mux.HandleFunc("/ping", s.handlePing)

	// Catch-all handler: place this last
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			if r.Method == http.MethodPost {
				s.handleRootPost(w, r)
				return
			}
			http.Redirect(w, r, "/play", http.StatusFound)
		default:
			s.handleNotFound(w, r) // Use this for 404 responses
		}
	})

	log.Println("Listening on :8123")
	log.Fatal(http.ListenAndServe(":8123", mux))
}

// lz4FrameMagic is the LZ4 frame header (0x184D2204 little endian), which
// clickhouse_connect prepends to compressed HTTP bodies.
var lz4FrameMagic = []byte{0x04, 0x22, 0x4d, 0x18}

// decompressBody undoes an HTTP content encoding, or an LZ4 frame that a driver
// applied to the body without declaring it in Content-Encoding.
func decompressBody(encoding string, body []byte) []byte {
	if len(body) == 0 {
		return body
	}
	enc := strings.ToLower(strings.TrimSpace(encoding))
	switch enc {
	case "lz4":
	case "":
		if !bytes.HasPrefix(body, lz4FrameMagic) {
			return body
		}
	default:
		// gzip/zstd/brotli are not expected from the drivers we serve; leave as is.
		return body
	}
	r := lz4.NewReader(bytes.NewReader(body))
	out, err := io.ReadAll(r)
	if err != nil {
		log.Printf("Failed to decompress request body: %v", err)
		return body
	}
	return out
}
