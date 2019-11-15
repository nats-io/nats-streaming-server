package pqdeadlines

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/lib/pq"
)

func init() {
	sql.Register("pq-deadlines", deadlineDriver{})
}

type deadlineDriver struct{}

type deadlineDialer struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
}

type deadlineConn struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c *deadlineConn) Read(b []byte) (n int, err error) {
	if c.readTimeout != 0 {
		c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	n, err = c.Conn.Read(b)
	if c.readTimeout != 0 {
		c.Conn.SetReadDeadline(time.Time{})
	}
	return n, err
}

func (c *deadlineConn) Write(b []byte) (n int, err error) {
	if c.writeTimeout != 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	n, err = c.Conn.Write(b)
	if c.writeTimeout != 0 {
		c.Conn.SetWriteDeadline(time.Time{})
	}
	return n, err
}

func (d deadlineDialer) Dial(network string, address string) (net.Conn, error) {
	return d.DialTimeout(network, address, 0)
}

func (d deadlineDialer) DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error) {
	c, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return c, err
	}
	if d.readTimeout == 0 && d.writeTimeout == 0 {
		return c, nil
	}
	return &deadlineConn{Conn: c, readTimeout: d.readTimeout, writeTimeout: d.writeTimeout}, nil
}

func (t deadlineDriver) Open(connection string) (driver.Conn, error) {
	var (
		connKeyVals  []string
		readTimeout  time.Duration
		writeTimeout time.Duration
		err          error
	)

	if strings.HasPrefix(connection, "postgres://") || strings.HasPrefix(connection, "postgresql://") {
		connection, err = pq.ParseURL(connection)
		if err != nil {
			return nil, err
		}
	}

	for _, keyVal := range strings.Fields(connection) {
		s := strings.Split(keyVal, "=")
		key := strings.ToLower(s[0])
		switch key {
		case "readtimeout":
			readTimeout, err = time.ParseDuration(s[1])
			if err != nil {
				return nil, fmt.Errorf("unable to parse readTimeout: %v", err)
			}
		case "writetimeout":
			writeTimeout, err = time.ParseDuration(s[1])
			if err != nil {
				return nil, fmt.Errorf("unable to parse writeTimeout: %v", err)
			}
		default:
			connKeyVals = append(connKeyVals, keyVal)
		}
	}

	connStr := strings.Join(connKeyVals, " ")
	td := deadlineDialer{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
	return pq.DialOpen(td, connStr)
}
