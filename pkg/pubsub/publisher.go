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
	brokerAddr   string
	brokerConn   *grpc.ClientConn
	brokerClient pb.BrokerServiceClient
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewPublisher(brokerAddrr string) (*Publisher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{
		brokerAddr: brokerAddrr,
		ctx:        ctx,
		cancel:     cancel,
	}

	var err error
	p.brokerConn, err = grpc.Dial(
		p.brokerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, fmt.Errorf("grpc connection to broker failed: %v", err)
	}
	p.brokerClient = pb.NewBrokerServiceClient(p.brokerConn)
	return p, nil
}

func (p *Publisher) Publish(topic string, data []byte) error {
	_, err := p.brokerClient.Publish(p.ctx, &pb.PublishRequest{Topic: topic, Data: data})
	return err
}

func (p *Publisher) Close() error {
	p.cancel()
	return p.brokerConn.Close()
}
