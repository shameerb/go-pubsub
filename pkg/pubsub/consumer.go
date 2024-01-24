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

type stream pb.Message

// consumer will connect to the broker. It will also have a list of all the streams open that this consumer is connected to the broker.
type Consumer struct {
	ID            uint32
	brokerAddress string
	conn          *grpc.ClientConn
	client        pb.PubSubServiceClient
	Messages      chan *pb.Message
	subscriptions sync.Map // map of topic to corresponding streams cancel function to close the connection.
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewConsumer(brokerAddress string) (*Consumer, error) {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		ID:            uuid.New().ID(),
		brokerAddress: brokerAddress,
		Messages:      make(chan *pb.Message, 500),
		subscriptions: sync.Map{},
		ctx:           ctx,
		cancel:        cancel,
	}
	var err error
	consumer.conn, err = grpc.Dial(
		brokerAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	consumer.client = pb.NewPubSubServiceClient(consumer.conn)
	return consumer, nil
}

// will start a goroutine for that topic to wait for a cancel from client side or the broker
func (c *Consumer) Subscribe(topic string) error {
	_, ok := c.subscriptions.Load(topic)
	if ok {
		// already subscribed. No need to create a new one
		return nil
	}

	streamCtx, streamCancel := context.WithCancel(c.ctx)
	stream, err := c.client.Subscribe(streamCtx, &pb.SubscriberRequest{
		Topic:        topic,
		SubscriberId: c.ID,
	})

	if err != nil {
		streamCancel()
		return err
	}

	c.subscriptions.Store(topic, streamCtx)

	go c.receive(stream, streamCtx)
	return nil
}

func (c *Consumer) Unsubscribe(topic string) error {
	cancel, ok := c.subscriptions.Load(topic)
	if !ok {
		return fmt.Errorf("not subscribed to the topic %s", topic)
	}
	cancel.(context.CancelFunc)()
	c.subscriptions.Delete(topic)
	// send an unsubscribe request to the broker to remove the stream from its memory as well.
	_, err := c.client.Unsubscribe(c.ctx, &pb.UnsubscribeRequest{
		SubscriberId: c.ID,
		Topic:        topic,
	})
	return err
}

func (c *Consumer) receive(stream pb.PubSubService_SubscribeClient, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			stream.CloseSend()
			return
		default:
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			c.Messages <- msg
		}
	}
}

func (c *Consumer) Close() error {
	c.cancel()
	// close all streams that this subscriber is holding up with the broker.
	c.subscriptions.Range(func(key, value interface{}) bool {
		c.Unsubscribe(key.(string))
		return true
	})
	return c.conn.Close()
}
