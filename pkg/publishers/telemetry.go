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
	"github.com/rs/zerolog/log"
)

type Server struct {
	Address  string
	Password string
}

var _ Publisher = &Server{}

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
		log.Error().Err(err).Msg("failed to set deadline")
		return
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	serverHandshake := strings.Join([]string{
		"XtraLib.Stream.0",
		"Tacview.RealTimeTelemetry.0",
		"Host acmi-exporter",
	}, "\n") + string(rune(0))

	if _, err := rw.WriteString(serverHandshake); err != nil {
		log.Error().Err(err).Msg("failed to write host handshake")
		return
	}
	if err := rw.Flush(); err != nil {
		log.Error().Err(err).Msg("failed to flush writer during handshake")
		return
	}

	packet, err := rw.ReadString(0)
	if err != nil {
		log.Warn().Err(err).Msg("failed to read client handshake")
		return
	}
	if !h.authorize(packet) {
		log.Warn().Msg("client handshake failed authorization")
		return
	}

	log.Info().Strs("lines", initialLines).Msg("writing initial lines")
	for _, line := range initialLines {
		log.Info().Str("line", line).Msg("writing initial line")
		if _, err := rw.WriteString(line + "\n"); err != nil {
			log.Error().Err(err).Msg("failed to write initial line")
			return
		}
		if err = rw.Flush(); err != nil {
			log.Error().Err(err).Msg("failed to flush writer")
			return
		}
	}
	log.Info().Msg("initial lines written")

	for message, ok := <-h.receiver; ok; {
		log.Info().Str("message", message).Msg("writing message")
		if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			log.Error().Err(err).Msg("failed to set write deadline")
			return
		}
		if _, err := rw.WriteString(message + "\n"); err != nil {
			log.Error().Err(err).Msg("failed to write message")
			return
		}
	}
}

func (h *handler) authorize(packet string) bool {
	handshake, err := telemetry.DecodeClientHandshake(packet)

	if err != nil {
		log.Warn().Err(err).Msg("failed to decode client handshake")
		return false
	}
	ok := handshake.PasswordHash == h.hash()
	log.Info().Str("hostname", handshake.Hostname).Bool("authorized", ok).Msg("received client handshake")
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
