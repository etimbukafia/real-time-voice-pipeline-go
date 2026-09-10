// audio/websocket_source.go

package audio

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketSource turns a websocket carrying binary PCM frames into an AudioSource.
type WebSocketSource struct {
	conn *websocket.Conn
}

// NewWebSocketSource wraps an existing websocket connection as an audio source.
func NewWebSocketSource(conn *websocket.Conn) *WebSocketSource {
	return &WebSocketSource{conn: conn}
}

// DialWebSocketSource opens a websocket connection and exposes it as an audio source.
func DialWebSocketSource(ctx context.Context, url string, header http.Header) (*WebSocketSource, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, header)
	if err != nil {
		return nil, err
	}
	return NewWebSocketSource(conn), nil
}

// Stream reads binary websocket messages and turns them into timestamped audio frames.
func (w *WebSocketSource) Stream(ctx context.Context) (<-chan *AudioFrame, error) {
	out := make(chan *AudioFrame, 10)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return

			default:
				// ReadMessage blocks until the peer sends something.
				//
				// We set a short deadline on every loop so that an idle connection
				// wakes up regularly and notices ctx cancellation. Without a
				// deadline, this goroutine can get stuck forever inside ReadMessage.
				if err := w.conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond)); err != nil {
					log.Printf("audio: failed to set websocket read deadline: %v", err)
					return
				}

				messageType, data, err := w.conn.ReadMessage()
				if err != nil {
					if ctx.Err() != nil {
						return
					}

					if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
						return
					}

					var netErr net.Error
					if errors.As(err, &netErr) && netErr.Timeout() {
						// Timeout is expected here. It is just our chance to re-check ctx.
						continue
					}

					log.Printf("audio: failed to read from websocket: %v", err)
					return
				}

				// Only binary websocket messages should carry PCM audio frames.
				if messageType != websocket.BinaryMessage {
					log.Printf("audio: received non-binary message type %d", messageType)
					continue
				}

				frame := &AudioFrame{
					Data:      data,
					Timestamp: time.Now(),
				}

				select {
				case <-ctx.Done():
					return
				case out <- frame:
				default:
					// This pipeline prefers freshness over backlog replay.
					// If the consumer falls behind, we drop the oldest queued frame
					// and keep the most recent one.
					log.Printf("audio: websocket frame buffer full, dropping oldest frame")

					select {
					case <-out:
					default:
					}

					select {
					case <-ctx.Done():
						return
					case out <- frame:
					}
				}
			}
		}
	}()

	return out, nil
}
