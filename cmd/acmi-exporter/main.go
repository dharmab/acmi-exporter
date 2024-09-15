package main

import (
	"context"
	"time"

	"github.com/dharmab/acmi-exporter/pkg/streamer"
	"github.com/dharmab/acmi-exporter/pkg/tacview/objects"

	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
)

var grpcAddress string

var exporterCmd = &cobra.Command{
	Use:   "acmi-exporter",
	Short: "ACMI exporter",
	Long:  `ACMI exporter`,
	RunE:  RunApp,
}

func init() {
	exporterCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", "localhost:50051", "Address of the DCS-gRPC server")
}

func main() {
	if err := exporterCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute command")
	}
}

func RunApp(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	streamer, err := streamer.NewStreamer(grpcAddress, 2*time.Second)
	if err != nil {
		return err
	}
	updates := make(chan *objects.Object)
	removals := make(chan uint32)
	streamer.Stream(ctx, updates, removals)

	return nil
}
