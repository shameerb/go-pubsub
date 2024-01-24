package pubsub

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	pb "github.com/shameerb/go-pubsub/pkg/grpcapi"
	"google.golang.org/grpc"
)

type subscriberStream pb.PubSubService_SubscribeServer

type streamKey struct {
	topic        string
	subscriberID uint32
}

type Broker struct {
	pb.UnimplementedPubSubServiceServer
	port                  string
	listener              net.Listener
	grpcServer            *grpc.Server
	subscribers           map[string]map[uint32]subscriberStream
	topicSubscribersMutex map[streamKey]*sync.Mutex // avoiding multiple producers to write on the same stream and cause a lock. Streams do not allow that.
	mu                    sync.RWMutex              // mutex to write to this map. RW because writes are sparse. Reads occur on each publish. Writes occur on subscription and unsubscription
	ctx                   context.Context
	cancel                context.CancelFunc
	// (optional) Use a zap logger to avoid serialization latency in reflection serialization while logging.
}

func NewBroker(port string) *Broker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Broker{
		port:                  port,
		subscribers:           make(map[string]map[uint32]subscriberStream),
		topicSubscribersMutex: make(map[streamKey]*sync.Mutex),
		mu:                    sync.RWMutex{},
		ctx:                   ctx,
		cancel:                cancel,
	}

}

func (b *Broker) Start() error {
	// start a grpc server
	var err error
	b.listener, err = net.Listen("tcp", b.port)
	if err != nil {
		log.Fatalf("Failed to start grpc server")
	}
	b.grpcServer = grpc.NewServer()
	pb.RegisterPubSubServiceServer(b.grpcServer, b)
	// start the server
	go func() {
		if err := b.grpcServer.Serve(b.listener); err != nil {
			log.Fatalf("Failed to start the grpc server: %v", err)
		}
	}()
	return b.awaitShutdown()
}

func (b *Broker) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return b.Stop()
}

func (b *Broker) Stop() error {
	b.grpcServer.GracefulStop()
	// close all the streams.
	return b.listener.Close()
}

func (b *Broker) Subscribe(in *pb.SubscriberRequest, stream pb.PubSubService_SubscribeServer) error {
	b.mu.Lock()

	key := streamKey{topic: in.Topic, subscriberID: in.SubscriberId}
	if _, ok := b.subscribers[key.topic]; !ok {
		b.subscribers[key.topic] = make(map[uint32]subscriberStream)
	}
	b.subscribers[key.topic][key.subscriberID] = stream
	b.topicSubscribersMutex[key] = &sync.Mutex{}
	b.mu.Unlock()
	// this ensures that the stream is not closed.
	for {
		select {
		// Wait for the client to close the stream
		case <-stream.Context().Done():
			return nil
			// Wait for the broker to shutdown
		case <-b.ctx.Done():
			return nil
		}
	}
}

func (b *Broker) Unsubscribe(ctx context.Context, in *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := streamKey{topic: in.Topic, subscriberID: in.SubscriberId}
	if _, ok := b.subscribers[key.topic]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	if _, ok := b.subscribers[key.topic][key.subscriberID]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}

	delete(b.subscribers[key.topic], key.subscriberID)
	delete(b.topicSubscribersMutex, key)
	return &pb.UnsubscribeResponse{Success: true}, nil
}

func (b *Broker) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	b.mu.RLock()
	brokerSubscribers := make([]streamKey, 0)
	for subscriberId, stream := range b.subscribers[in.Topic] {
		key := streamKey{topic: in.Topic, subscriberID: subscriberId}
		b.topicSubscribersMutex[key].Lock()
		err := stream.Send(&pb.Message{Topic: in.Topic, Message: in.Message})
		b.topicSubscribersMutex[key].Unlock()
		if err != nil {
			brokerSubscribers = append(brokerSubscribers, key)
		}
	}
	b.mu.RUnlock()
	b.removeBrokenSubscribers(brokerSubscribers)
	if len(brokerSubscribers) > 0 {
		return &pb.PublishResponse{Success: false}, fmt.Errorf("failed to send to some subscribers")
	}
	return &pb.PublishResponse{Success: true}, nil
}

func (b *Broker) removeBrokenSubscribers(keys []streamKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// do I need to close the stream somehow? send a ctx done signal to the respective streams that would be open.
	for _, key := range keys {
		delete(b.subscribers[key.topic], key.subscriberID)
		delete(b.topicSubscribersMutex, key)
	}
}
