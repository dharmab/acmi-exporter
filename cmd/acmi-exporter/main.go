package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/DCS-gRPC/go-bindings/dcs/v0/coalition"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/hook"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/mission"
	"github.com/dharmab/acmi-exporter/pkg/publishers"
	"github.com/dharmab/acmi-exporter/pkg/streamer"
	"github.com/dharmab/goacmi/objects"
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
	publishStdout             bool
	publishToFolder           string
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
	exporterCmd.PersistentFlags().DurationVar(&airUnitUpdateInterval, "air-unit-update-interval", time.Second, "How often to publish frames for air units")
	exporterCmd.PersistentFlags().DurationVar(&surfaceUnitUpdateInterval, "surface-unit-update-interval", time.Second, "How often to publish frames for surface units")
	exporterCmd.PersistentFlags().DurationVar(&weaponUpdateInterval, "weapon-update-interval", time.Second, "How often to publish frames for weapons")
	exporterCmd.PersistentFlags().BoolVar(&publishStdout, "publish-stdout", false, "Publish updates to stdout (useful for debugging)")
	exporterCmd.PersistentFlags().StringVar(&publishToFolder, "publish-to-folder", "", "Publish updates as a new file in the given folder")
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
	hookServiceClient := hook.NewHookServiceClient(grpcClient)

	dataStreamer := streamer.New(missionServiceClient, coalitionServiceClient, hookServiceClient)

	updates := make(chan streamer.Payload)
	messages := make(chan string)
	consumers := []chan string{}
	consumersLock := sync.RWMutex{}

	globalObject, err := dataStreamer.GetGlobalObject(ctx)
	if err != nil {
		return fmt.Errorf("failed to get global object: %w", err)
	}
	bullseyes, err := dataStreamer.GetBullseyes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bullseyes: %w", err)
	}
	initials := &publishers.Initials{
		Global:    globalObject,
		Bullseyes: bullseyes,
	}

	if publishStdout {
		ch := make(chan string)
		func() {
			consumersLock.Lock()
			defer consumersLock.Unlock()
			consumers = append(consumers, ch)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			publisher := publishers.StdoutPublisher{}
			if err := publisher.Publish(ctx, initials, ch); err != nil {
				log.Error().Err(err).Msg("Failed to publish to stdout")
			}
		}()
	}

	if publishToFolder != "" {
		folder, err := filepath.Abs(publishToFolder)
		if err != nil {
			return fmt.Errorf("failed to get absolute path to folder: %w", err)
		}
		if _, err := os.Stat(folder); os.IsNotExist(err) {
			if err := os.MkdirAll(folder, 0755); err != nil {
				return fmt.Errorf("failed to create folder: %w", err)
			}
		}

		title, err := dataStreamer.GetMissionName(ctx)
		if err != nil {
			return fmt.Errorf("failed to get mission name: %w", err)
		}

		publisher := publishers.FilePublisher{
			Folder: folder,
			Title:  title,
		}

		ch := make(chan string)
		func() {
			consumersLock.Lock()
			defer consumersLock.Unlock()
			consumers = append(consumers, ch)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := publisher.Publish(ctx, initials, ch); err != nil {
				log.Error().Err(err).Msg("Failed to publish to folder")
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		publisher := publishers.Server{
			Address:  telemetryAddress,
			Password: password,
		}
		ch := make(chan string)
		func() {
			consumersLock.Lock()
			defer consumersLock.Unlock()
			consumers = append(consumers, ch)
		}()
		if err := publisher.Publish(ctx, initials, ch); err != nil {
			log.Error().Err(err).Msg("Failed to publish to server")
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
				func() {
					consumersLock.RLock()
					defer consumersLock.RUnlock()
					log.Debug().Str("message", message).Msg("Muxing message")
					for _, ch := range consumers {
						ch <- message
					}
				}()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		dataStreamer.Stream(ctx, updates, airUnitUpdateInterval, surfaceUnitUpdateInterval, weaponUpdateInterval)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := Process(ctx, globalObject, bullseyes, updates, messages); err != nil {
			log.Error().Err(err).Msg("Failed to process updates")
		}
	}()

	<-ctx.Done()
	wg.Wait()
	return nil
}

func Process(ctx context.Context, global *objects.Object, initialObjects []*objects.Object, updates <-chan streamer.Payload, messages chan<- string) error {
	frameTime := time.Duration(0)
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
			log.Debug().Str("update", update.Update.String()).Msg("Processed update")
		}
	}
}
