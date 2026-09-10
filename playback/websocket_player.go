package playback

import (
	"context"
	"fmt"
	"github.com/etimbukafia/real-time-voice-pipeline-go/audio"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketPlayer turns a websocket connection into a playback sink for assistant audio.
type WebSocketPlayer struct {
	conn *websocket.Conn
}

// NewWebSocketPlayer wraps an existing websocket connection as a playback sink.
func NewWebSocketPlayer(conn *websocket.Conn) *WebSocketPlayer {
	return &WebSocketPlayer{conn: conn}
}

// DialWebSocketPlayer opens a websocket playback connection and returns it as a Player.
func DialWebSocketPlayer(ctx context.Context, url string, header http.Header) (*WebSocketPlayer, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, header)
	if err != nil {
		return nil, fmt.Errorf("playback: connect websocket player: %w", err)
	}
	return NewWebSocketPlayer(conn), nil
}

// Play forwards assistant PCM frames to the websocket peer as binary messages.
func (p *WebSocketPlayer) Play(ctx context.Context, frames <-chan audio.AudioFrame) error {
	for {
		select {
		case <-ctx.Done():
			_ = p.conn.Close()
			return ctx.Err()
		case frame, ok := <-frames:
			if !ok {
				return nil
			}

			if err := p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			if err := p.conn.WriteMessage(websocket.BinaryMessage, frame.Data); err != nil {
				return err
			}
		}
	}
}
