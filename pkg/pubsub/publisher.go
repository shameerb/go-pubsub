package pubsub

import (
	"context"

	pb "github.com/shameer/go-pubsub/pkg/grpcapi"
	"google.golang.org/grpc"
)

// you need a grpc connection to broker
type Publisher struct {
	conn   *grpc.ClientConn
	client pb.Pub
	ctx    context.Context
	cancel context.CancelFunc
}

func NewPublisher() (*Publisher, error) {

}

func (p *Publisher) Publish(topic string, mesage []byte) error {

}
