// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"compress/zlib"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"net"
	"os"
)

// Wraps a net.Conn and allows transparent activation of the mysql compression protocol.
type compressableReaderConn struct {
	net.Conn
	compression bool
	seq         uint8
	buff        buffer
	// TODO replace by buffer somehow?
	remainder io.ReadCloser
}

type compressableWriterConn struct {
	net.Conn
	compression bool
	seq         uint8
	header      []byte
	compressor  bytes.Buffer
}

// sets or resets compression
func (c *compressableReaderConn) activateCompression() (err error) {
	c.remainder = nil
	c.seq = math.MaxInt8
	c.compression = true
	c.buff.nc = c.Conn
	return err
}

func (c *compressableWriterConn) activateCompression() (err error) {
	c.compression = true
	c.seq = 0
	if c.header == nil {
		c.header = make([]byte, 7)
	}
	return err
}

func (c *compressableReaderConn) Read(b []byte) (n int, err error) {
	if !c.compression {
		return c.Conn.Read(b)
	}

	if c.remainder != nil {
		n, err = c.remainder.Read(b)
		if err == io.EOF {
			c.remainder = nil
			err = nil
		}
		return n, err
	}

	// if nothing is left from former packet read the next compressed packet
	payloadLen, seq, err := c.readCompressedHeader()
	if err != nil {
		return 0, err
	}

	if seq != c.seq+1 {
		return 0, fmt.Errorf("wrong seq number [%d != %d] on compressed connection", seq, c.seq+1)
	}
	c.seq++

	compressed, err := c.buff.readNext(payloadLen)
	if err != nil {
		return 0, err
	}
	c.remainder, err = zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return 0, err
	}

	// and finally read the data
	return c.Read(b)
}

func (c *compressableReaderConn) readCompressedHeader() (int, uint8, error) {
	data, err := c.buff.readNext(7)
	if err != nil {
		return 0, 0, err
	}

	// length of compressed payload [24 bit]
	return int(uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16),
		// compressed sequence id [8 bit]
		data[3],
		nil
}

func (c *compressableWriterConn) Write(b []byte) (n int, err error) {
	defer func() {
		if err != nil {
			fmt.Println(err)
		}
	}()
	fmt.Println("Write", c.compression, len(b))
	stdoutDumper := hex.Dumper(os.Stdout)
	defer stdoutDumper.Close()
	stdoutDumper.Write(b)

	if !c.compression {
		return c.Conn.Write(b)
	}

	// only compress data if we have more than 50 byte.
	// TODO somehow batch writes. via timeout or flush

	if len(b) <= 50 {
		err = c.writeHeader(len(b), 0)
		if err != nil {
			return 0, err
		}
		return c.Write(b)
	}

	c.compressor.Reset()
	zw, err := zlib.NewWriterLevel(&c.compressor, zlib.BestSpeed)
	if err != nil {
		return 0, err
	}
	_, err = zw.Write(b)
	if err != nil {
		return 0, err
	}
	err = zw.Close()
	if err != nil {
		return 0, err
	}
	res := c.compressor.Bytes()
	err = c.writeHeader(len(res), len(b))

	if err != nil {
		return 0, err
	}
	return c.Write(res)
}

func (c *compressableWriterConn) writeHeader(compressed, uncompressed int) (err error) {
	// length of compressed payload [24 bit]
	c.header[0] = byte(compressed)
	c.header[1] = byte(compressed >> 8)
	c.header[2] = byte(compressed >> 16)
	// compressed sequence id [8 bit]
	c.header[3] = byte(c.seq)
	// length of payload before compression [24 bit]
	c.header[4] = byte(compressed)
	c.header[5] = byte(compressed >> 8)
	c.header[6] = byte(compressed >> 16)

	c.seq++
	fmt.Println("header: ", c.header)
	_, err = c.Conn.Write(c.header)
	return err
}
