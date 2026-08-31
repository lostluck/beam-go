// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beam

import (
	"slices"

	"lostluck.dev/beam-go/coders"
	"lostluck.dev/beam-go/internal/harness"
	"lostluck.dev/beam-go/window"
)

// This file contains the data source and datasink Transforms
// and edges. These are added in by runners for execution on
// the SDK, and never added in manually by users.

// edgeDataSource represents a data connection from the runner.
type edgeDataSource[E Element] struct {
	index     edgeIndex
	transform string

	port        harness.Port
	makeCoder   func() coders.Coder[E]
	windowCoder windowCoder

	output nodeIndex

	timerExecutors map[string]timerExecutor
}

func (e *edgeDataSource[E]) protoID() string {
	return e.transform
}

func (e *edgeDataSource[E]) edgeID() edgeIndex {
	return e.index
}

// inputs for datasink, in practice there should only be one
// but if all else fails, we can insert a flatten.
func (e *edgeDataSource[E]) inputs() map[string]nodeIndex {
	return nil
}

// outputs for DataSink is nil, since it sends back to the runner.
func (e *edgeDataSource[E]) outputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.output}
}

func (e *edgeDataSource[E]) setTimerExecutors(execs map[string]timerExecutor) {
	e.timerExecutors = execs
}

func (e *edgeDataSource[E]) source(dc harness.DataContext, mets *metricsStore) (processor, processor) {
	// This is what the Datasource emits to.
	toConsumer := &DFC[E]{id: e.output}
	toConsumer.metrics = mets

	wCoder := e.windowCoder
	if wCoder == nil {
		wCoder = globalWindowCoderWrapper{}
	}

	var expectedTimerTransforms []string
	for tid := range e.timerExecutors {
		expectedTimerTransforms = append(expectedTimerTransforms, tid)
	}
	slices.Sort(expectedTimerTransforms)

	// Just kick it off with an impulse.
	root := &DFC[[]byte]{
		id:         e.output,
		downstream: []processor{toConsumer},
		transform:  e.transform,
		metrics:    mets,
		dofn: &datasource[E]{
			DC:                      dc,
			SID:                     harness.StreamID{PtransformID: e.transform, Port: e.port},
			Output:                  PCol[E]{valid: true, globalIndex: e.output, localDownstreamIndex: 0},
			Coder:                   e.makeCoder(),
			WindowCoder:             wCoder,
			expectedTimerTransforms: expectedTimerTransforms,
			timerExecutors:          e.timerExecutors,
			dc: &dataChannelIndex{
				transform: e.transform,
				index:     0,
				split:     (1<<63 - 1),
			},
		},
	}
	return root, toConsumer
}

var _ sourcer = (*edgeDataSource[int])(nil)

type sourcer interface {
	multiEdge
	source(dc harness.DataContext, mets *metricsStore) (processor, processor)
	setTimerExecutors(execs map[string]timerExecutor)
}

// datasource reads from GRPC and emits of the specified type.
//
// Unlike most generic Transforms, the generic isn't on the
// input type (which is alwas []byte), but the output type.
type datasource[E Element] struct {
	DC  harness.DataContext
	SID harness.StreamID

	// Window Coder to produce windows
	Coder       coders.Coder[E]
	WindowCoder windowCoder

	Output PCol[E]

	dc *dataChannelIndex

	expectedTimerTransforms []string
	timerExecutors          map[string]timerExecutor
}

func (fn *datasource[E]) ProcessBundle(dfc *DFC[[]byte]) error {
	// Connect to Data service
	elmsChan, err := fn.DC.Data.OpenElementChan(dfc.ctx, fn.SID, fn.expectedTimerTransforms)
	if err != nil {
		return err
	}

	// Track the data channel index for progress and split handling.
	if fn.dc == nil {
		fn.dc = &dataChannelIndex{
			transform: fn.SID.PtransformID,
			index:     0,
			split:     (1<<63 - 1),
		}
	} else {
		fn.dc.mu.Lock()
		fn.dc.index = 0
		fn.dc.split = (1<<63 - 1)
		fn.dc.mu.Unlock()
	}

	wCoder := fn.WindowCoder
	if wCoder == nil {
		wCoder = globalWindowCoderWrapper{}
	}

	return dfc.Process(func(ec ElmC, _ []byte) error {
	dataChan:
		for dataElm := range elmsChan {
			if len(dataElm.Timers) > 0 {
				if te, ok := fn.timerExecutors[dataElm.PtransformID]; ok {
					if err := te.executeTimer(dataElm.TimerFamilyID, dataElm.Timers); err != nil {
						return err
					}
				}
			}
			if len(dataElm.Data) > 0 {
				// Start reading byte blobs.
				dec := coders.NewDecoder(dataElm.Data)
				for !dec.Empty() {
					et := dec.Timestamp()
					numWindows := dec.Uint32()
					ws := make([]window.BoundedWindow, numWindows)
					for i := range ws {
						ws[i] = wCoder.Decode(dec)
					}
					pn := dec.Pane()
					elm := fn.Coder.Decode(dec)
					fn.Output.Emit(ElmC{
						eventTime:    et,
						windows:      ws,
						pane:         pn,
						pcollections: ec.pcollections,
					}, elm)
					if fn.dc.IncrementAndCheckSplit(dfc) {
						break dataChan
					}
				}
			}
		}
		return nil
	})
}

var _ sourceSplitter = &datasource[int]{}

func (fn *datasource[E]) splitSource(helper func(index, split int64) int64) {
	if fn.dc == nil {
		return
	}
	// We lock here to avoid moving past the new split.
	fn.dc.mu.Lock()
	defer fn.dc.mu.Unlock()
	fn.dc.split = helper(fn.dc.index, fn.dc.split)
}

// edgeDataSink represents a data connection back to the runner.
type edgeDataSink[E Element] struct {
	index     edgeIndex
	transform string

	port        harness.Port
	makeCoder   func() coders.Coder[E]
	windowCoder windowCoder

	input nodeIndex
}

func (e *edgeDataSink[E]) protoID() string {
	return e.transform
}

func (e *edgeDataSink[E]) edgeID() edgeIndex {
	return e.index
}

// inputs for datasink, in practice there should only be one
// but if all else fails, we can insert a flatten.
func (e *edgeDataSink[E]) inputs() map[string]nodeIndex {
	return map[string]nodeIndex{"o0": e.input}
}

// outputs for DataSink is nil, since it sends back to the runner.
func (e *edgeDataSink[E]) outputs() map[string]nodeIndex {
	return nil
}

var _ sinker = (*edgeDataSink[int])(nil)

type sinker interface {
	multiEdge
	sinkDoFn(dc harness.DataContext) any
}

func (e *edgeDataSink[E]) sinkDoFn(dc harness.DataContext) any {
	wCoder := e.windowCoder
	if wCoder == nil {
		wCoder = globalWindowCoderWrapper{}
	}
	return &datasink[E]{
		DC:          dc,
		SID:         harness.StreamID{PtransformID: e.transform, Port: e.port},
		Coder:       e.makeCoder(),
		WindowCoder: wCoder,
	}
}

// datasink writes window value encoded elements to the runner over the configured data channel.
type datasink[E Element] struct {
	DC  harness.DataContext
	SID harness.StreamID

	// Window Coder to produce windows
	Coder       coders.Coder[E]
	WindowCoder windowCoder

	OnBundleFinish
}

func (fn *datasink[E]) ProcessBundle(dfc *DFC[E]) error {
	wc, err := fn.DC.Data.OpenWrite(dfc.ctx, fn.SID)
	if err != nil {
		return err
	}

	wCoder := fn.WindowCoder
	if wCoder == nil {
		wCoder = globalWindowCoderWrapper{}
	}

	enc := coders.NewEncoder()
	// TODO outputing to timers callbacks
	if err := dfc.Process(func(ec ElmC, elm E) error {
		enc.Reset(100)
		enc.Timestamp(ec.EventTime())
		ws := ec.windows
		if len(ws) == 0 {
			if ec.window != nil {
				ws = []window.BoundedWindow{ec.window}
			} else {
				ws = []window.BoundedWindow{window.GlobalWindow{}}
			}
		}
		enc.Uint32(uint32(len(ws)))
		for _, w := range ws {
			wCoder.Encode(enc, w)
		}
		enc.Pane(ec.pane)

		fn.Coder.Encode(enc, elm)
		if _, err := wc.Write(enc.Data()); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	fn.Do(dfc, func() error {
		return wc.Close()
	})
	return nil
}
