// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package exporter contains an OpenCensus service exporter.
package exporter

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/census-instrumentation/opencensus-proto/gen-go/exporterproto"
	"github.com/census-instrumentation/opencensus-proto/gen-go/traceproto"
	"github.com/census-instrumentation/opencensus-service/internal"
)

var debug bool

func init() {
	_, debug = os.LookupEnv("OPENCENSUS_DEBUG")
}

type Exporter struct {
	OnError func(err error)

	initOnce sync.Once

	clientMu       sync.Mutex
	clientEndpoint string
	client         exporterproto.ExportClient
}

func (e *Exporter) init() {
	go func() {
		for {
			e.lookup()
		}
	}()
}

func (e *Exporter) lookup() {
	defer func() {
		debugPrintf("Sleeping for a minute...")
		time.Sleep(60 * time.Second)
	}()

	debugPrintf("Looking for the endpoint file")
	file := internal.DefaultEndpointFile()
	ep, err := ioutil.ReadFile(file)
	if os.IsNotExist(err) {
		e.deleteClient()
		debugPrintf("Endpoint file doesn't exist; disabling exporting")
		return
	}
	if err != nil {
		e.onError(err)
		return
	}

	e.clientMu.Lock()
	oldendpoint := e.clientEndpoint
	e.clientMu.Unlock()

	endpoint := string(ep)
	if oldendpoint == endpoint {
		debugPrintf("Endpoint is the same, doing nothing")
		return
	}

	debugPrintf("Dialing %v...", endpoint)
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		e.onError(err)
		return
	}

	e.clientMu.Lock()
	e.client = exporterproto.NewExportClient(conn)
	e.clientMu.Unlock()
}

func (e *Exporter) onError(err error) {
	if e.OnError != nil {
		e.OnError(err)
		return
	}
	log.Printf("Exporter fail: %v", err)
}

func (e *Exporter) ExportSpan(sd *trace.SpanData) {
	e.initOnce.Do(e.init)

	e.clientMu.Lock()
	client := e.client
	e.clientMu.Unlock()

	if client == nil {
		return
	}

	debugPrintf("Exporting span [%v]", sd.SpanContext)
	s := &traceproto.Span{
		TraceId:      sd.SpanContext.TraceID[:],
		SpanId:       sd.SpanContext.SpanID[:],
		ParentSpanId: sd.ParentSpanID[:],
		Name: &traceproto.TruncatableString{
			Value: sd.Name,
		},
		StartTime: nil, // TODO
		EndTime:   nil, // TODO
	}

	if _, err := client.Export(context.Background(), &exporterproto.ExportRequest{
		Spans: []*traceproto.Span{s},
	}); err != nil {
		s, ok := status.FromError(err)
		if ok && s.Code() == codes.Unavailable {
			debugPrintf("Connection became unavailable; will try to reconnect in a minute")
			e.deleteClient()
		} else {
			e.onError(err)
		}
	}
}

func (e *Exporter) deleteClient() {
	e.clientMu.Lock()
	e.client = nil
	e.clientEndpoint = ""
	e.clientMu.Unlock()
}

func debugPrintf(msg string, arg ...interface{}) {
	if debug {
		if len(arg) > 0 {
			fmt.Printf(msg, arg)
		} else {
			fmt.Printf(msg)
		}

		fmt.Println()
	}
}
