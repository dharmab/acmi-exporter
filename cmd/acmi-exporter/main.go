package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DCS-gRPC/go-bindings/dcs/v0/coalition"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/mission"
	"github.com/dharmab/acmi-exporter/pkg/streamer"
	"github.com/dharmab/goacmi/objects"
	"github.com/dharmab/goacmi/properties"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
)

var (
	grpcAddress               string
	telemetryAddress          string
	hostname                  string
	password                  string
	airUnitUpdateInterval     time.Duration
	surfaceUnitUpdateInterval time.Duration
	weaponUpdateInterval      time.Duration
)

var exporterCmd = &cobra.Command{
	Use:   "acmi-exporter",
	Short: "ACMI exporter",
	Long:  `ACMI exporter`,
	RunE:  Run,
}

func init() {
	exporterCmd.PersistentFlags().StringVar(&grpcAddress, "grpc-address", "localhost:50051", "Address of the DCS-gRPC server")
	exporterCmd.PersistentFlags().StringVar(&telemetryAddress, "telemetry-address", "localhost:42675", "Address to serve telemetry on")
	exporterCmd.PersistentFlags().StringVar(&hostname, "hostname", "acmi-exporter", "ACMI protocol hostname")
	exporterCmd.PersistentFlags().StringVar(&password, "password", "", "ACMI protocol password")
	exporterCmd.PersistentFlags().DurationVar(&airUnitUpdateInterval, "air-unit-update-interval", time.Second/10, "How often to publish frames for air units")
	exporterCmd.PersistentFlags().DurationVar(&surfaceUnitUpdateInterval, "surface-unit-update-interval", time.Second, "How often to publish frames for surface units")
	exporterCmd.PersistentFlags().DurationVar(&weaponUpdateInterval, "weapon-update-interval", time.Second/8, "How often to publish frames for weapons")
	exporterCmd.MarkPersistentFlagRequired("password")
}

func main() {
	if err := exporterCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute command")
	}
}

func Run(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	wg := &sync.WaitGroup{}

	grpcClient, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to create gRPC client: %w", err)
	}
	missionServiceClient := mission.NewMissionServiceClient(grpcClient)
	coalitionServiceClient := coalition.NewCoalitionServiceClient(grpcClient)

	dataStreamer := streamer.New(missionServiceClient, coalitionServiceClient)

	updates := make(chan streamer.Payload)
	messages := make(chan string)

	wg.Add(1)
	go func() {
		defer wg.Done()
		globalObject, err := dataStreamer.GetGlobalObject(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get global object")
			return
		}
		bullseyes, err := dataStreamer.GetBullseyes(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get bullseyes")
			return
		}
		if err := Process(ctx, globalObject, bullseyes, updates, messages); err != nil {
			log.Error().Err(err).Msg("Failed to process updates")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-messages:
				fmt.Println(message)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		dataStreamer.Stream(ctx, updates, airUnitUpdateInterval, surfaceUnitUpdateInterval, weaponUpdateInterval)
	}()

	<-ctx.Done()
	wg.Wait()
	return nil
}

func Process(ctx context.Context, global *objects.Object, initialObjects []*objects.Object, updates <-chan streamer.Payload, messages chan<- string) error {
	frameTime := time.Duration(0)
	if err := publishGlobals(global, messages); err != nil {
		return fmt.Errorf("failed to publish globals: %w", err)
	}
	for _, obj := range initialObjects {
		update := &objects.Update{
			ID:         obj.ID,
			IsRemoval:  false,
			Properties: obj.Properties,
		}
		messages <- update.String()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case update := <-updates:
			if update.MissionTime > frameTime {
				frameTime = update.MissionTime
				messages <- fmt.Sprintf("#%.2f", frameTime.Seconds())
			}
			messages <- update.Update.String()
		}
	}
}

func publishGlobals(global *objects.Object, messages chan<- string) error {
	for _, propName := range []string{
		properties.ReferenceTime,
		properties.RecordingTime,
		properties.Title,
		properties.DataRecorder,
		properties.DataSource,
		properties.Author,
		properties.Comments,
		properties.ReferenceLongitude,
		properties.ReferenceLatitude,
	} {
		value, ok := global.GetProperty(propName)
		if !ok {
			return fmt.Errorf("missing global property %q", propName)
		}
		update := objects.Update{
			ID:         global.ID,
			IsRemoval:  false,
			Properties: map[string]string{propName: value},
		}
		messages <- update.String()
	}
	return nil
}
