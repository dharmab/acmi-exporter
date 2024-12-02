package publishers

import (
	"bufio"
	"context"
	"hash/crc64"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dharmab/skyeye/pkg/telemetry"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Server listens for real-time telemetry client connections and publishes ACMI messages over TCP.
type Server struct {
	// Address to listen on.
	Address string
	// Password required for clients to connect.
	Password string
}

var _ Publisher = &Server{}

// Publish implements [Publisher.Publish] by listening for client connections on the server's address, negotiating a handshake, and writing ACMI data over TCP.
func (s *Server) Publish(ctx context.Context, initials InitialsProvider, messages <-chan string) error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	defer listener.Close()

	handlers := make(map[string]*handler)
	handlersLock := sync.RWMutex{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message, ok := <-messages:
				if !ok {
					return
				}
				func() {
					handlersLock.Lock()
					defer handlersLock.Unlock()
					for _, h := range handlers {
						h.receiver <- message
					}
				}()
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				return err
			}
			h := &handler{
				receiver: make(chan string, 0x10000),
				password: s.Password,
			}
			remoteAddr := conn.RemoteAddr().String()
			func() {
				handlersLock.Lock()
				defer handlersLock.Unlock()
				handlers[remoteAddr] = h
			}()
			go func() {
				defer func() {
					handlersLock.Lock()
					defer handlersLock.Unlock()
					close(h.receiver)
					conn.Close()
					delete(handlers, remoteAddr)
				}()
				h.handle(conn, initials)
			}()
		}
	}
}

type handler struct {
	receiver chan string
	password string
}

func (h *handler) handle(conn net.Conn, initials InitialsProvider) {
	logger := log.With().Str("remote", conn.RemoteAddr().String()).Logger()
	timeout := time.After(30 * time.Second)
	initialLines := make([]string, 0)

	for len(initialLines) == 0 {
		select {
		case <-timeout:
			log.Error().Msg("client handshake timed out")
			return
		default:
			var err error
			initialLines, err = initials.Get()
			if err != nil {
				log.Error().Err(err).Msg("failed to get initial lines")
			}
		}
	}

	if err := conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		logger.Error().Err(err).Msg("failed to set deadline")
		return
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	logger.Info().Msg("negotiating handshake")
	serverHandshake := strings.Join([]string{
		"XtraLib.Stream.0",
		"Tacview.RealTimeTelemetry.0",
		"Host acmi-exporter",
	}, "\n") + string(rune(0))

	if _, err := rw.WriteString(serverHandshake); err != nil {
		logger.Error().Err(err).Msg("failed to write host handshake")
		return
	}
	if err := rw.Flush(); err != nil {
		logger.Error().Err(err).Msg("failed to flush writer during handshake")
		return
	}

	packet, err := rw.ReadString(0)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to read client handshake")
		return
	}
	if !h.authorize(packet, &logger) {
		logger.Warn().Msg("client handshake failed authorization")
		return
	}

	logger.Info().Msg("publishing telemetry")

	for _, line := range initialLines {
		if _, err := rw.WriteString(line + "\n"); err != nil {
			logger.Error().Err(err).Msg("failed to write initial line")
			return
		}
		if err = rw.Flush(); err != nil {
			logger.Error().Err(err).Msg("failed to flush writer")
			return
		}
	}

	for message, ok := <-h.receiver; ok; {
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			logger.Error().Err(err).Msg("failed to set write deadline")
			return
		}
		if _, err := rw.WriteString(message + "\n"); err != nil {
			logger.Error().Err(err).Msg("failed to write message")
			return
		}
		if err := rw.Flush(); err != nil {
			logger.Error().Err(err).Msg("failed to flush writer")
			return
		}
	}
}

func (h *handler) authorize(packet string, logger *zerolog.Logger) bool {
	handshake, err := telemetry.DecodeClientHandshake(packet)

	if err != nil {
		logger.Warn().Err(err).Msg("failed to decode client handshake")
		return false
	}
	ok := handshake.PasswordHash == h.hash()
	return ok
}

func (h *handler) hash() string {
	if h.password == "" {
		return "0"
	}
	table := crc64.MakeTable(crc64.ECMA)
	data := []byte(h.password)
	hash := crc64.Checksum(data, table)
	return strconv.FormatUint(hash, 10)
}
