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

	"github.com/chdb-io/chdb-go/chdb"
	"github.com/chdb-io/chdb-go/chdbstable"
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

// trailingFormatRe matches a trailing "FORMAT <name>" clause (case-insensitive).
var trailingFormatRe = regexp.MustCompile(`(?is)\s+FORMAT\s+([A-Za-z0-9_]+)\s*$`)

// extractFormat splits a trailing "FORMAT xxx" clause off the query.
// It returns the remaining query and the format name (empty if none found).
func extractFormat(query string) (string, string) {
	m := trailingFormatRe.FindStringSubmatchIndex(query)
	if m == nil {
		return query, ""
	}
	format := query[m[2]:m[3]]
	return strings.TrimSpace(query[:m[0]]), format
}

// queryWithParams runs query in the session, forwarding "param_<name>" HTTP
// parameters as chDB CLI --param_<name> arguments for {name:Type} bindings.
func (s *server) queryWithParams(query, format string, params map[string]string) (*chdbstable.LocalResult, error) {
	argv := []string{"clickhouse", "--multiquery", "--output-format=" + format}
	if p := s.sess.Path(); p != "" {
		argv = append(argv, "--path="+p)
	}
	for k, v := range params {
		argv = append(argv, "--"+k+"="+v)
	}
	argv = append(argv, "--query="+query)
	return chdbstable.QueryStable(len(argv), argv)
}

type server struct {
	sess *chdb.Session
}

func newServer(path string) *server {
	s := &server{}
	sess, err := chdb.NewSession(path)
	if err != nil {
		log.Fatalf("Error creating session: %v", err)
	}
	s.sess = sess
	return s
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
	if r.Body != nil {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		body = string(bodyBytes)
	}
	if body != "" {
		body = strings.Join(strings.Fields(body), " ")
		if query != "" {
			query += " "
		}
		query += body
	}
	if database != "" {
		query = "USE " + database + "; " + query
	}
	// Honor a trailing "FORMAT xxx" clause, as sent by clients like clickhouse_connect.
	if q, f := extractFormat(query); f != "" {
		query = q
		format = f
	}
	log.Printf("Query: %s [format=%s]\n", query, format)
	output, err := s.queryWithParams(query, format, params)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Native (binary) formats must be written verbatim.
	if _, err := w.Write(output.Buf()); err != nil {
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
