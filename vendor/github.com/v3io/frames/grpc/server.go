/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package grpc

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/v3io/frames"
	"github.com/v3io/frames/api"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/v3io/frames/pb"
)

const (
	grpcMsgSize = 128 * (1 << 20) // 128MB
)

// Server is a frames gRPC server
type Server struct {
	frames.ServerBase

	address string
	api     *api.API
	config  *frames.Config
	logger  logger.Logger
	server  *grpc.Server
}

// NewServer returns a new gRPC server
func NewServer(config *frames.Config, addr string, logger logger.Logger) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "bad configuration")
	}

	if err := config.InitDefaults(); err != nil {
		return nil, errors.Wrap(err, "failed to init defaults")
	}

	if logger == nil {
		var err error
		logger, err = frames.NewLogger(config.Log.Level)
		if err != nil {
			return nil, errors.Wrap(err, "can't create logger")
		}
	}

	api, err := api.New(logger, config)
	if err != nil {
		return nil, errors.Wrap(err, "can't create API")
	}

	server := &Server{
		ServerBase: *frames.NewServerBase(),

		address: addr,
		api:     api,
		config:  config,
		logger:  logger,
		server: grpc.NewServer(
			grpc.MaxRecvMsgSize(grpcMsgSize),
			grpc.MaxSendMsgSize(grpcMsgSize),
		),
	}

	pb.RegisterFramesServer(server.server, server)
	reflection.Register(server.server)
	return server, nil
}

// Start starts the server
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		s.SetError(err)
		return err
	}

	s.SetState(frames.RunningState)
	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.ErrorWith("can't serve", "error", err)
			s.SetError(err)
		}
	}()

	return nil
}

func (s *Server) Read(request *pb.ReadRequest, stream pb.Frames_ReadServer) error {
	ch := make(chan frames.Frame)

	var apiError error
	go func() {
		defer close(ch)
		apiError = s.api.Read(request, ch)
		if apiError != nil {
			s.logger.ErrorWith("API error reading", "error", apiError)
		}
	}()

	for frame := range ch {
		fpb, ok := frame.(pb.Framed)
		if !ok {
			s.logger.Error("unknown frame type")
			return errors.New("unknown frame type")
		}

		if err := stream.Send(fpb.Proto()); err != nil {
			return err
		}
	}

	return apiError
}

// Write write data to table
func (s *Server) Write(stream pb.Frames_WriteServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	pbReq := msg.GetRequest()
	if pbReq == nil {
		return fmt.Errorf("stream didn't start with write request")
	}

	var frame frames.Frame
	if pbReq.InitialData != nil {
		frame = frames.NewFrameFromProto(pbReq.InitialData)
	}

	req := &frames.WriteRequest{
		Session:       pbReq.Session,
		Backend:       pbReq.Backend,
		Expression:    pbReq.Expression,
		HaveMore:      pbReq.More,
		ImmidiateData: frame,
		Table:         pbReq.Table,
	}

	// TODO: Unite with the code in HTTP server
	var (
		writeError     error
		nFrames, nRows int
		ch             = make(chan frames.Frame, 1)
		done           = make(chan bool)
	)

	go func() {
		defer close(done)
		nFrames, nRows, writeError = s.api.Write(req, ch)
	}()

	for writeError == nil {
		msg, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				s.logger.ErrorWith("stream error", "error", err)
				return err
			}
			break
		}

		frameMessage := msg.GetFrame()
		if frameMessage == nil {
			s.logger.ErrorWith("nil frame", "message", msg)
			return fmt.Errorf("nil frame")
		}

		frame := frames.NewFrameFromProto(frameMessage)
		ch <- frame
	}

	close(ch)
	<-done

	// We can't handle writeError right after .Write since it's done in a goroutine
	if writeError != nil {
		s.logger.ErrorWith("write error", "error", writeError)
		return writeError
	}

	resp := &pb.WriteRespose{
		Frames: int64(nFrames),
		Rows:   int64(nRows),
	}

	return stream.SendAndClose(resp)
}

// Create creates a table
func (s *Server) Create(ctx context.Context, req *pb.CreateRequest) (*pb.CreateResponse, error) {
	// TODO: Use ctx for timeout
	if err := s.api.Create(req); err != nil {
		return nil, err
	}

	return &pb.CreateResponse{}, nil
}

// Delete deletes a table
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.api.Delete(req); err != nil {
		return nil, err
	}

	return &pb.DeleteResponse{}, nil
}

// Exec executes a command
func (s *Server) Exec(ctx context.Context, req *pb.ExecRequest) (*pb.ExecResponse, error) {
	frame, err := s.api.Exec(req)
	if err != nil {
		return nil, err
	}

	resp := &pb.ExecResponse{}

	if frame != nil {
		fpb, ok := frame.(pb.Framed)
		if !ok {
			s.logger.Error("unknown frame type")
			return nil, errors.New("unknown frame type")
		}
		resp.Frame = fpb.Proto()
	}

	return resp, nil
}
