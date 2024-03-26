package pubsub

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	pb "github.com/shameerb/go-pubsub/pkg/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	ID            uint32
	brokerAddr    string
	brokerConn    *grpc.ClientConn
	brokerClient  pb.BrokerServiceClient
	Message       chan *pb.Data
	subscriptions sync.Map // each topic this consumer is subscribed to against the context.cancel function required to cancel the stream
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewConsumer(brokerAddr string) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Consumer{
		ID:            uuid.New().ID(),
		brokerAddr:    brokerAddr,
		ctx:           ctx,
		cancel:        cancel,
		Message:       make(chan *pb.Data, 500),
		subscriptions: sync.Map{},
	}
	var err error
	c.brokerConn, err = grpc.Dial(
		c.brokerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	c.brokerClient = pb.NewBrokerServiceClient(c.brokerConn)
	return c, nil
}

func (c *Consumer) Subscribe(topic string) error {
	if _, ok := c.subscriptions.Load(topic); ok {
		// already subscribed to the topic.
		return nil
	}
	// create new children context for each of the subscription, parent being the main context.
	// keep the cancel func stored in case you want to cancel the stream from the consumer side.
	streamCtx, streamCancel := context.WithCancel(c.ctx)
	stream, err := c.brokerClient.Subscribe(streamCtx, &pb.SubscribeRequest{
		Topic: topic,
		Id:    c.ID,
	})
	// any issue with the subscription, send a cancel context
	if err != nil {
		streamCancel()
		return err
	}
	c.subscriptions.Store(topic, streamCancel)
	go c.listen(stream, streamCtx)
	return nil
}

func (c *Consumer) Unsubscribe(topic string) error {
	cancel, ok := c.subscriptions.Load(topic)
	if !ok {
		return fmt.Errorf("not subscribed to topic: %s", topic)
	}
	// cancel the context, which in turn will send a nil from the broker.
	// this will also exit all the goroutine listen corresponding to each of the subcription topic
	cancel.(context.CancelFunc)()
	c.subscriptions.Delete(topic)

	// unsubscribe from the broker.
	_, err := c.brokerClient.Unsubscribe(c.ctx, &pb.UnsubscribeRequest{
		Topic: topic,
		Id:    c.ID,
	})
	return err
}

func (c *Consumer) listen(stream pb.BrokerService_SubscribeClient, streamCtx context.Context) {
	for {
		select {
		case <-streamCtx.Done():
			stream.CloseSend()

		default:
			// blocks on Recv until you get a message.
			// hence the streamCtx.Done() might get missed in this case.
			// else use a timedout version of Recv.
			data, err := stream.Recv()
			if err != nil {
				return
			}
			c.Message <- data
		}
	}
}

func (c *Consumer) Close() {
	c.cancel()
	// close all the streams
	c.subscriptions.Range(func(key, value interface{}) bool {
		c.Unsubscribe(key.(string))
		return true
	})

}
