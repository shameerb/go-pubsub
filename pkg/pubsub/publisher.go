package pubsub

import (
	"context"
	"fmt"

	pb "github.com/shameerb/go-pubsub/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// responsible for sending messages for a topic to a broker service
type Publisher struct {
	brokerAddress string
	conn          *grpc.ClientConn
	client        pb.PubSubServiceClient // client to connect to the broker service
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewPublisher(brokerAddress string) (*Publisher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	publisher := &Publisher{
		brokerAddress: brokerAddress,
		ctx:           ctx,
		cancel:        cancel,
	}

	var err error
	publisher.conn, err = grpc.Dial(
		publisher.brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, fmt.Errorf("gRPC connection to broker failed %v", err)
	}

	publisher.client = pb.NewPubSubServiceClient(publisher.conn)

	return publisher, nil
}

func (p *Publisher) Publish(topic string, message []byte) error {
	_, err := p.client.Publish(p.ctx, &pb.PublishRequest{Topic: topic, Message: message})
	return err
}

func (p *Publisher) Close() error {
	p.cancel()
	return p.conn.Close()
}
