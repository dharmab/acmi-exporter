package streamer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/DCS-gRPC/go-bindings/dcs/v0/coalition"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/common"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/mission"
	"github.com/DCS-gRPC/go-bindings/dcs/v0/unit"
	"github.com/dharmab/acmi-exporter/pkg/tacview/objects"
	"github.com/dharmab/acmi-exporter/pkg/tacview/properties"
	"github.com/dharmab/acmi-exporter/pkg/tacview/tags"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

type Streamer struct {
	missionService   mission.MissionServiceClient
	coalitionService coalition.CoalitionServiceClient
	unitService      unit.UnitServiceClient
	interval         time.Duration
	globalObject     *objects.Object
	objects          map[uint32]*objects.Object
	objectsLock      sync.RWMutex
}

func NewStreamer(address string, interval time.Duration) (*Streamer, error) {
	grpcClient, err := grpc.NewClient(address)
	if err != nil {
		return nil, err
	}
	globalObject := objects.NewObject(0)
	globalObject.SetProperty(properties.DataSource, "DCS World")
	globalObject.SetProperty(properties.DataRecorder, "acmi-exporter")
	globalObject.SetProperty(properties.Title, "test mission please ignore")

	return &Streamer{
		missionService:   mission.NewMissionServiceClient(grpcClient),
		coalitionService: coalition.NewCoalitionServiceClient(grpcClient),
		unitService:      unit.NewUnitServiceClient(grpcClient),
		interval:         interval,
		globalObject:     objects.NewObject(0),
		objects:          make(map[uint32]*objects.Object),
	}, nil
}

func (s *Streamer) Stream(ctx context.Context, updates chan<- *objects.Object, removals chan<- uint32) {
	var wg sync.WaitGroup
	streamCtx, cancel := context.WithCancel(ctx)

	wg.Add(2)
	go func() {
		defer wg.Done()
		defer cancel()
		s.streamUnits(streamCtx, removals)
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				cancel()
				wg.Wait()
				return
			default:
				time.Sleep(s.interval)
				updates <- s.globalObject
			}
		}
	}()
	wg.Wait()
}

func (s *Streamer) streamUnits(ctx context.Context, removals chan<- uint32) {
	rate := uint32(s.interval.Seconds())
	request := &mission.StreamUnitsRequest{
		PollRate:   &rate,
		MaxBackoff: &rate,
		Category:   common.GroupCategory_GROUP_CATEGORY_UNSPECIFIED,
	}
	stream, err := s.missionService.StreamUnits(ctx, request)
	if err != nil {
		log.Error().Err(err).Msg("failed to stream units")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			response, err := stream.Recv()
			if err != nil {
				log.Error().Err(err).Msg("received error from units stream")
				return
			}
			gone := response.GetGone()
			if gone != nil {
				log.Info().
					Uint32("unitID", gone.GetId()).
					Str("name", gone.GetName()).
					Msg("removing unit")
				s.removeObject(gone.GetId(), removals)
			} else {
				_unit := response.GetUnit()
				logger := log.With().Uint32("unitID", _unit.GetId()).Str("name", _unit.GetName()).Logger()
				obj := objects.NewObject(_unit.GetId())
				obj.Time = time.Duration(response.GetTime()) * time.Second
				obj.SetProperty(properties.Name, _unit.GetName())
				obj.SetProperty(properties.Pilot, _unit.GetPlayerName())
				obj.SetProperty(properties.Type, _unit.GetType())
				obj.SetProperty(properties.CallSign, _unit.GetCallsign())
				if _unit.GetCoalition() == common.Coalition_COALITION_RED {
					obj.SetProperty(properties.Coalition, "Enemies")
				} else {
					obj.SetProperty(properties.Coalition, "Allies")
				}

				group := _unit.GetGroup()
				if group != nil {
					switch group.GetCategory() {
					case common.GroupCategory_GROUP_CATEGORY_AIRPLANE:
						obj.SetProperty(properties.Category, fmt.Sprintf("%s+%s", tags.Air, tags.FixedWing))
					case common.GroupCategory_GROUP_CATEGORY_HELICOPTER:
						obj.SetProperty(properties.Category, fmt.Sprintf("%s+%s", tags.Air, tags.Rotorcraft))
					case common.GroupCategory_GROUP_CATEGORY_TRAIN:
						obj.SetProperty(properties.Category, tags.Ground)
					case common.GroupCategory_GROUP_CATEGORY_GROUND:
						obj.SetProperty(properties.Category, tags.Ground)
					case common.GroupCategory_GROUP_CATEGORY_SHIP:
						obj.SetProperty(properties.Category, tags.Sea)
					}
				}

				position := _unit.GetPosition()
				if position == nil {
					logger.Warn().Msg("unit has no position")
				}
				orientation := _unit.GetOrientation()
				if orientation == nil {
					logger.Warn().Msg("unit has no orientation")
				}
				obj.SetProperty(properties.Transform, transform(position, orientation))

				s.updateObject(obj)
			}
		}
	}
}

func transform(position *common.Position, orientation *common.Orientation) string {
	var longitude, latitude, altitude, roll, pitch, yaw, u, v, heading string
	if position != nil {
		longitude = fmt.Sprintf("%.6f", position.GetLon())
		latitude = fmt.Sprintf("%.6f", position.GetLat())
		altitude = fmt.Sprintf("%.1f", position.GetAlt())
		u = fmt.Sprintf("%.6f", position.GetU())
		v = fmt.Sprintf("%.6f", position.GetV())
	}
	if orientation != nil {
		roll = fmt.Sprintf("%.2f", orientation.GetRoll())
		pitch = fmt.Sprintf("%.2f", orientation.GetPitch())
		yaw = fmt.Sprintf("%.2f", orientation.GetYaw())
	}

	coordinates := make([]string, 0)
	if orientation != nil {
		coordinates = append(coordinates, longitude, latitude, altitude, roll, pitch, yaw, u, v, heading)
	} else {
		coordinates = append(coordinates, longitude, latitude, altitude, u, v)
	}
	return strings.Join(coordinates, "|")
}

func (s *Streamer) updateObject(obj *objects.Object) {
	s.objectsLock.Lock()
	defer s.objectsLock.Unlock()
	s.objects[obj.ID] = obj
}

func (s *Streamer) removeObject(id uint32, removals chan<- uint32) {
	s.objectsLock.Lock()
	defer s.objectsLock.Unlock()
	delete(s.objects, id)
	removals <- id
}

func (s *Streamer) GetGlobalProperty(property string) (string, bool) {
	return s.globalObject.GetProperty(property)
}
