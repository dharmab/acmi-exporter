package main

import (
	"context"
	"net"
	"time"

	"github.com/dharmab/acmi-exporter/pkg/acmi/objects"
	acmi "github.com/dharmab/acmi-exporter/pkg/acmi/server"
	"github.com/dharmab/acmi-exporter/pkg/streamer"

	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
)

var (
	grpcAddress      string
	telemetryAddress string
	hostname         string
	password         string
)

var exporterCmd = &cobra.Command{
	Use:   "acmi-exporter",
	Short: "ACMI exporter",
	Long:  `ACMI exporter`,
	RunE:  RunApp,
}

func init() {
	exporterCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", "localhost:50051", "Address of the DCS-gRPC server")
	exporterCmd.PersistentFlags().StringVar(&telemetryAddress, "telemetry-address", "localhost:42675", "Address to serve telemetry on")
	exporterCmd.PersistentFlags().StringVar(&hostname, "hostname", "acmi-exporter", "ACMI protocol hostname")
	exporterCmd.PersistentFlags().StringVar(&password, "password", "", "ACMI protocol password")
	exporterCmd.MarkPersistentFlagRequired("password")
}

func main() {
	if err := exporterCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute command")
	}
}

func RunApp(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	updates := make(chan *objects.Object)
	removals := make(chan uint32)

	streamer, err := streamer.NewStreamer(grpcAddress, 2*time.Second)
	if err != nil {
		return err
	}
	streamer.Stream(ctx, updates, removals)

	listener, err := net.Listen("tcp", telemetryAddress)
	if err != nil {
		return err
	}
	server := acmi.NewServer(listener, "acmi-exporter", "password", streamer)
	server.Serve(ctx, updates, removals)

	return nil
}
