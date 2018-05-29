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
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/census-instrumentation/opencensus-proto/gen-go/exportproto"
	"github.com/census-instrumentation/opencensus-proto/gen-go/traceproto"
	"github.com/census-instrumentation/opencensus-service/internal"
)

type Exporter struct {
	OnError func(err error)

	initOnce sync.Once

	clientMu       sync.Mutex
	clientEndpoint string
	client         exportproto.ExportClient
}

func (e *Exporter) init() {
	go func() {
		for {
			// TODO(jbd): Disable lookups if connection is available.
			e.lookup()
		}
	}()
}

func (e *Exporter) lookup() {
	defer time.Sleep(60 * time.Second)

	file := internal.DefaultEndpointFile()
	ep, err := ioutil.ReadFile(file)
	if os.IsNotExist(err) {
		e.deleteClient()
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
		return
	}

	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		e.onError(err)
		return
	}

	e.clientMu.Lock()
	e.client = exportproto.NewExportClient(conn)
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

	if _, err := client.Export(context.Background(), &exportproto.ExportRequest{
		Spans: []*traceproto.Span{s},
	}); err != nil {
		s, ok := status.FromError(err)
		if ok && s.Code() == codes.Unavailable {
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
