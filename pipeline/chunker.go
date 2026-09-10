package pipeline

import "strings"

// PhraseChunker groups streamed LLM deltas into TTS-friendly phrases.
//
// Token-level TTS sounds bad and creates too many tiny synthesis requests.
// Waiting for the full response sounds better, but wastes latency. The chunker
// is the compromise: release a phrase as soon as it is likely to have usable
// prosody, usually at punctuation or once the buffered text grows large enough.
type PhraseChunker struct {
	minChars int
	buffer   strings.Builder
}

// NewPhraseChunker creates a phrase chunker with a minimum size floor for usable prosody.
func NewPhraseChunker(minChars int) *PhraseChunker {
	if minChars < 8 {
		minChars = 8
	}
	return &PhraseChunker{minChars: minChars}
}

// Push appends a new text delta and returns any phrase that is now ready for TTS.
func (c *PhraseChunker) Push(delta string) []string {
	if delta == "" {
		return nil
	}

	c.buffer.WriteString(delta)
	text := c.buffer.String()

	if len(text) < c.minChars {
		return nil
	}

	if idx := lastPhraseBoundary(text); idx != -1 {
		phrase := strings.TrimSpace(text[:idx+1])
		rest := strings.TrimSpace(text[idx+1:])
		c.buffer.Reset()
		c.buffer.WriteString(rest)
		if phrase != "" {
			return []string{phrase}
		}
		return nil
	}

	// Long buffers without punctuation still need to move forward. In that case
	// we flush at the last whitespace boundary so TTS can start rather than
	// waiting forever for a perfect sentence-ending token.
	if len(text) >= c.minChars*2 {
		if idx := strings.LastIndex(text, " "); idx != -1 {
			phrase := strings.TrimSpace(text[:idx])
			rest := strings.TrimSpace(text[idx+1:])
			c.buffer.Reset()
			c.buffer.WriteString(rest)
			if phrase != "" {
				return []string{phrase}
			}
		}
	}

	return nil
}

// Flush returns the remaining buffered text when the LLM stream ends.
func (c *PhraseChunker) Flush() string {
	text := strings.TrimSpace(c.buffer.String())
	c.buffer.Reset()
	return text
}

// lastPhraseBoundary finds the last punctuation mark that is good enough to split on.
func lastPhraseBoundary(text string) int {
	return strings.LastIndexAny(text, ".?!;:\n")
}
