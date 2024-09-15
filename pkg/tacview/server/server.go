package server

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc64"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/dharmab/acmi-exporter/pkg/streamer"
	"github.com/dharmab/acmi-exporter/pkg/tacview/objects"
	"github.com/dharmab/acmi-exporter/pkg/tacview/properties"
)

type Server struct {
	listener     net.Listener
	hostname     string
	passwordHash uint64
	streamer     *streamer.Streamer
	lock         sync.Mutex
	serial       int
	updateMux    map[int]chan *objects.Object
	removalMux   map[int]chan uint32
}

// NewServer creates a new server
func NewServer(listener net.Listener, hostname, password string, streamer *streamer.Streamer) *Server {
	return &Server{
		listener:     listener,
		hostname:     hostname,
		passwordHash: crc64.Checksum([]byte(password), crc64.MakeTable(crc64.ECMA)),
		streamer:     streamer,
		updateMux:    make(map[int]chan *objects.Object),
		removalMux:   make(map[int]chan uint32),
	}
}

func (s *Server) Serve(ctx context.Context, updates <-chan *objects.Object, removals <-chan uint32) error {

	for {
		select {
		case <-ctx.Done():
			return nil
		case update := <-updates:
			for _, mux := range s.updateMux {
				mux <- update
			}
		case removal := <-removals:
			for _, mux := range s.removalMux {
				mux <- removal
			}
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				return err
			}

			go s.handleConnection(ctx, conn)
		}
	}
}

const lowLevelProtocolHeader = "XtraLib.Stream.0"
const highLevelProtocolHeader = "Tacview.RealTimeTelemetry.0"
const fileTypeHeader = "FileType=text/acmi/tacview"
const fileVersionHeader = "FileVersion=2.2"

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	writer.WriteString(fmt.Sprintf("%s\n%s\nHost %s\n\n", lowLevelProtocolHeader, highLevelProtocolHeader, s.hostname))
	writer.Flush()

	line, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	if line != lowLevelProtocolHeader {
		return
	}

	line, err = reader.ReadString('\n')
	if err != nil {
		return
	}
	if line != highLevelProtocolHeader {
		return
	}

	line, err = reader.ReadString('\n')
	if err != nil {
		return
	}
	if !strings.HasPrefix(line, "Client ") {
		return
	}

	line, err = reader.ReadString('\n')
	if err != nil {
		return
	}
	hash, err := strconv.Atoi(line)
	if err != nil {
		return
	}
	if uint64(hash) != s.passwordHash {
		return
	}

	_, err = writer.WriteString(fmt.Sprintf("%s\n%s\n", fileTypeHeader, fileVersionHeader))
	if err != nil {
		return
	}
	writer.Flush()

	updates := make(chan *objects.Object)
	removals := make(chan uint32)
	registration := s.registerHandler(updates, removals)
	defer s.unregisterHandler(registration)

	for _, property := range []string{properties.DataSource, properties.DataRecorder, properties.Title, properties.ReferenceTime} {
		value, ok := s.streamer.GetGlobalProperty(property)
		value = strings.ReplaceAll(value, ",", "\\,")
		if ok {
			writer.WriteString(fmt.Sprintf("%s=%s\n", property, value))
		} else {
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case update := <-updates:
			writer.WriteString(fmt.Sprintf("#%.3f\n", update.Time.Seconds()))
			line = string(update.ID)
			for k, v := range update.Properties() {
				line = strings.ReplaceAll(v, ",", "\\,")
				line += fmt.Sprintf(",%s=%s", k, v)
			}
			writer.WriteString(fmt.Sprint(line, "\n"))
		case removal := <-removals:
			writer.WriteString("-" + string(removal) + "\n")
		}
	}
}

func (s *Server) registerHandler(update chan *objects.Object, removal chan uint32) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.serial++
	s.updateMux[s.serial] = update
	s.removalMux[s.serial] = removal
	return s.serial
}

func (s *Server) unregisterHandler(serial int) {
	s.lock.Lock()
	defer s.lock.Unlock()

	delete(s.updateMux, serial)
	delete(s.removalMux, serial)
}
