package main

/*
#cgo LDFLAGS: -L/usr/local/lib -lchdb
#include <stdlib.h>
#include <string.h>
#include "chdb.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

// client drives a single chDB engine instance through the connection API.
//
// chDB embeds a Poco application singleton, so a process may only host one
// engine. Both queries and streaming inserts therefore go through the same
// chdb_connection; mixing the connection API with the argv-based
// query_stable_v2 path trips an assertion in Poco::Application.
type client struct {
	mu       sync.Mutex // chDB connections are not safe for concurrent statements
	conn     *C.chdb_connection
	path     string
	database string // database currently selected on the connection
}

func newClient(dbPath string) (*client, error) {
	argv := []string{"clickhouse", "--multiquery"}
	if dbPath != "" {
		argv = append(argv, "--path="+dbPath)
	}
	cArgv := make([]*C.char, len(argv))
	for i, s := range argv {
		cArgv[i] = C.CString(s)
		defer C.free(unsafe.Pointer(cArgv[i]))
	}
	conn := C.chdb_connect(C.int(len(argv)), &cArgv[0])
	if conn == nil {
		return nil, errors.New("chdb: failed to open connection")
	}
	return &client{conn: conn, path: dbPath}, nil
}

// Close shuts the engine down. Call once at process exit.
func (c *client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		C.chdb_close_conn(c.conn)
		c.conn = nil
	}
}

// Query runs a statement and returns the formatted result bytes.
func (c *client) Query(query, format string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queryLocked(query, format)
}

// queryLocked is Query without locking; callers must hold c.mu.
func (c *client) queryLocked(query, format string) ([]byte, error) {
	if c.conn == nil {
		return nil, errors.New("chdb: connection closed")
	}

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	res := C.chdb_query(*c.conn, cQuery, cFormat)
	if res == nil {
		return nil, errors.New("chdb: query returned no result")
	}
	defer C.chdb_destroy_query_result(res)

	if msg := C.chdb_result_error(res); msg != nil {
		return nil, errors.New("chdb: " + C.GoString(msg))
	}
	n := C.chdb_result_length(res)
	if n == 0 {
		return nil, nil
	}
	return C.GoBytes(unsafe.Pointer(C.chdb_result_buffer(res)), C.int(n)), nil
}

// Insert streams raw format-encoded rows.
//
// The command-line path cannot carry this data: argv entries are NUL-terminated
// C strings, so a Native block would be truncated at its first 0x00 byte and the
// insert would silently write nothing while still reporting success.
func (c *client) Insert(database, query, format string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return errors.New("chdb: connection closed")
	}

	// The stream API takes a single statement, so select the database with a
	// separate query instead of prefixing "USE db;".
	if database != "" && database != c.database {
		if err := c.exec("USE " + quoteIdent(database)); err != nil {
			return err
		}
		c.database = database
	}

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))
	cFormat := C.CString(format)
	defer C.free(unsafe.Pointer(cFormat))

	stream := C.chdb_stream_insert(*c.conn, cQuery, cFormat)
	if stream == nil {
		return errors.New("chdb: failed to start insert stream")
	}
	defer C.chdb_destroy_insert_stream(stream)

	if msg := C.chdb_stream_insert_error(stream); msg != nil {
		C.chdb_stream_cancel_insert(stream)
		return errors.New("chdb: " + C.GoString(msg))
	}

	if len(data) > 0 {
		if C.chdb_stream_append(stream, unsafe.Pointer(&data[0]), C.size_t(len(data))) != C.CHDBSuccess {
			err := streamErr(stream)
			C.chdb_stream_cancel_insert(stream)
			return err
		}
	}

	res := C.chdb_stream_done(stream)
	if res == nil {
		return errors.New("chdb: insert stream returned no result")
	}
	defer C.chdb_destroy_query_result(res)
	if msg := C.chdb_result_error(res); msg != nil {
		return errors.New("chdb: " + C.GoString(msg))
	}
	return nil
}

// exec runs a statement and discards its output. Callers hold c.mu.
func (c *client) exec(query string) error {
	_, err := c.queryLocked(query, "")
	return err
}

// quoteIdent back-quotes an identifier for safe interpolation.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func streamErr(stream C.chdb_insert_stream) error {
	if msg := C.chdb_stream_insert_error(stream); msg != nil {
		return errors.New("chdb: " + C.GoString(msg))
	}
	return fmt.Errorf("chdb: insert stream append failed")
}
