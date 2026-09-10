package main

import (
	"context"
	"fmt"
	"log"
	"os"
)

// main builds the live pipeline, runs one session, and prints a compact latency summary.
func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	cfg := loadAppConfig()
	p, err := buildPipeline(context.Background(), cfg, logger)
	if err != nil {
		logger.Fatalf("failed to build pipeline: %v", err)
	}

	summary, err := p.Run(context.Background())
	if err != nil {
		logger.Fatalf("pipeline failed: %v", err)
	}

	fmt.Println()
	fmt.Println("Summary")
	for _, turn := range summary.Turns {
		fmt.Printf(
			"- %s interrupted=%t user=%q assistant=%q stt=%s first_token=%s first_audio=%s playback=%s\n",
			turn.ID,
			turn.Interrupted,
			turn.UserText,
			turn.AssistantText,
			turn.EndOfSpeechToSTTFinal(),
			turn.EndOfSpeechToFirstToken(),
			turn.EndOfSpeechToFirstAudio(),
			turn.EndOfSpeechToPlayback(),
		)
	}
}
