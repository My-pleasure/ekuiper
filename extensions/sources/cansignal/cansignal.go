package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type canSignal struct {
	Name        string `json:"name"`
	DataType    string `json:"dataType"`
	MinValuePhy string `json:"minValuePhy"`
	MaxValuePhy string `json:"maxValuePhy"`
	CyclicTime  int32  `json:"cyclicTime"`
	ChangeTime  int32  `json:"changeTime"`
}

type canSignalSourceConfig struct {
	Signal []canSignal `json:"signal"`
}

// Emit data randomly with only a string field
type canSignalSource struct {
	conf *canSignalSourceConfig
	list [][]byte
}

func (s *canSignalSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &canSignalSourceConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	for _, v := range cfg.Signal {
		if v.CyclicTime <= 0 {
			return fmt.Errorf("source `can_signal` %s property `cyclicTime` must be a positive integer but got %d", v.Name, v.CyclicTime)
		}
		if v.ChangeTime < 0 {
			return fmt.Errorf("source `can_signal` %s property `changeTime` must be a positive integer or zero but got %d", v.Name, v.ChangeTime)
		}
		if v.MaxValuePhy < v.MinValuePhy {
			return fmt.Errorf("source `can_signal` %s property `maxValuePhy` must be greater than `minValuePhy`", v.Name)
		}
	}
	s.conf = cfg
	return nil
}

func (s *canSignalSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	s.list = make([][]byte, 100)
	s.list[0] = []byte{}
	for k, v := range s.conf.Signal {
		logger.Debugf("can signal %s ready to send data, DataType: %s, MinValuePhy: %s, MaxValuePhy: %s, CyclicTime: %d, ChangeTime: %d", v.Name, v.DataType, v.MinValuePhy, v.MaxValuePhy, v.CyclicTime, v.ChangeTime)
		// 初始化信号值
		next, err := randomize(v.Name, v.DataType, v.MinValuePhy, v.MaxValuePhy)
		if err != nil {
			errCh <- err
			return
		}
		ns, err := json.Marshal(next)
		if err != nil {
			logger.Warnf("invalid input data %v", next)
			return
		}
		s.list[k] = ns

		// 如果变化时间不为 0
		if v.ChangeTime != 0 {
			go func() {
				changeT := time.NewTicker(time.Duration(v.ChangeTime) * time.Millisecond)
				defer changeT.Stop()

				for {
					select {
					case <-changeT.C:
						next, err := randomize(v.Name, v.DataType, v.MinValuePhy, v.MaxValuePhy)
						if err != nil {
							errCh <- err
							return
						}
						ns, err := json.Marshal(next)
						if err != nil {
							logger.Warnf("invalid input data %v", next)
							return
						}
						s.list[k] = ns
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		// 循环发送信号值
		go func() {
			cyclicT := time.NewTicker(time.Duration(v.CyclicTime) * time.Millisecond)
			defer cyclicT.Stop()

			for {
				select {
				case <-cyclicT.C:
					next := make(map[string]interface{})
					err := json.Unmarshal(s.list[k], &next)
					if err != nil {
						logger.Warnf("unmarshal input data failed %v", s.list[k])
					}
					logger.Debugf("Send out data %v", next)
					consumer <- api.NewDefaultSourceTuple(next, nil)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	<-ctx.Done()
}

func randomize(name, dataType, minValuePhy, maxValuePhy string) (map[string]interface{}, error) {
	r := make(map[string]interface{})
	switch dataType {
	case "int32", "sint32":
		minValue, err := strconv.ParseInt(minValuePhy, 10, 32)
		if err != nil {
			return nil, err
		}
		maxValue, err := strconv.ParseInt(maxValuePhy, 10, 32)
		if err != nil {
			return nil, err
		}
		vi := rand.Int31n(int32(maxValue)-int32(minValue)) + int32(minValue)
		r[name] = vi
	case "int64":
		minValue, err := strconv.ParseInt(minValuePhy, 10, 64)
		if err != nil {
			return nil, err
		}
		maxValue, err := strconv.ParseInt(maxValuePhy, 10, 64)
		if err != nil {
			return nil, err
		}
		vi := rand.Int63n(maxValue-minValue) + minValue
		r[name] = vi
	case "uint32":
		minValue, err := strconv.ParseUint(minValuePhy, 10, 32)
		if err != nil {
			return nil, err
		}
		maxValue, err := strconv.ParseUint(maxValuePhy, 10, 32)
		if err != nil {
			return nil, err
		}
		vi := rand.Uint32()%uint32(maxValue-minValue) + uint32(minValue)
		r[name] = vi
	case "uint64":
		minValue, err := strconv.ParseUint(minValuePhy, 10, 64)
		if err != nil {
			return nil, err
		}
		maxValue, err := strconv.ParseUint(maxValuePhy, 10, 64)
		if err != nil {
			return nil, err
		}
		vi := rand.Uint64()%(maxValue-minValue) + minValue
		r[name] = vi
	case "float", "float32":
		minValue, err := strconv.ParseFloat(minValuePhy, 32)
		if err != nil {
			return nil, err
		}
		maxValue, err := strconv.ParseFloat(maxValuePhy, 32)
		if err != nil {
			return nil, err
		}
		vi := rand.Float32()*float32(maxValue-minValue) + float32(minValue)
		r[name] = vi
	case "double", "float64":
		minValue, err := strconv.ParseFloat(minValuePhy, 64)
		if err != nil {
			return nil, err
		}
		maxValue, err := strconv.ParseFloat(maxValuePhy, 64)
		if err != nil {
			return nil, err
		}
		vi := rand.Float64()*(maxValue-minValue) + minValue
		r[name] = vi
	default:
		return nil, fmt.Errorf("unkonw data type: %s", dataType)
	}
	return r, nil
}

func (s *canSignalSource) Close(_ api.StreamContext) error {
	return nil
}

func CanSignal() api.Source {
	return &canSignalSource{}
}
