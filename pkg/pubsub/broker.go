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

/*
broker accepts publish request, keeps a grpc server open for publisher and consumers to connect to
implements the grpc service interface
publish, subscribe, unsubscribe
holds the mapping of topic to their corresponding list of subsribers
*/
type streamKey struct {
	topic        string
	subscriberId uint32
}

type subscriberStream pb.BrokerService_SubscribeServer

type Broker struct {
	pb.UnimplementedBrokerServiceServer
	port                 string
	listener             net.Listener
	grpcServer           *grpc.Server
	subscribers          map[string]map[uint32]subscriberStream
	mu                   *sync.RWMutex             // mutex to write to the subscriber map without inconsistency from concurrent subscribers. Concurrent reads are fine.
	topicSubscriberLocks map[streamKey]*sync.Mutex // avoiding multiple producers to write on the same stream and cause a lock. Streams do not allow that.
	ctx                  context.Context
	cancel               context.CancelFunc
}

func NewBroker(port string) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		port:                 port,
		ctx:                  ctx,
		cancel:               cancel,
		subscribers:          make(map[string]map[uint32]subscriberStream),
		mu:                   &sync.RWMutex{},
		topicSubscriberLocks: make(map[streamKey]*sync.Mutex),
	}
	return b
}

func (b *Broker) Start() error {
	var err error
	b.listener, err = net.Listen("tcp", b.port)
	if err != nil {
		log.Fatalf("Unable to create listener for grpc service : %v", err)
	}

	b.grpcServer = grpc.NewServer()
	go func() {
		if err := b.grpcServer.Serve(b.listener); err != nil {
			log.Fatalf("Failed to start grpc server: %v", err)
		}
	}()
	return b.awaitShutdown()
}

func (b *Broker) awaitShutdown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	return b.stop()
}

func (b *Broker) stop() error {
	b.grpcServer.GracefulStop()
	return b.listener.Close()
}

func (b *Broker) Subscribe(req *pb.SubscribeRequest, stream pb.BrokerService_SubscribeServer) error {
	b.mu.Lock()
	key := streamKey{topic: req.Topic, subscriberId: req.Id}
	if _, ok := b.subscribers[key.topic]; !ok {
		b.subscribers[key.topic] = make(map[uint32]subscriberStream)
	}
	b.subscribers[key.topic][key.subscriberId] = stream
	b.topicSubscriberLocks[key] = &sync.Mutex{}
	b.mu.Unlock()
	// Keep the stream open until the context is closed, during which it sends a nil on the stream to indicate closed.
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-b.ctx.Done():
			return nil
		}
	}
}

func (b *Broker) Unsubscribe(ctx context.Context, req *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := streamKey{topic: req.Topic, subscriberId: req.Id}
	if _, ok := b.subscribers[key.topic]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}
	if _, ok := b.subscribers[key.topic][key.subscriberId]; !ok {
		return &pb.UnsubscribeResponse{Success: false}, nil
	}
	delete(b.subscribers[key.topic], key.subscriberId)
	delete(b.topicSubscriberLocks, key)
	return &pb.UnsubscribeResponse{Success: true}, nil
}

func (b *Broker) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	b.mu.RLock()
	erroredSubscribers := make([]streamKey, 0)
	for id, stream := range b.subscribers[req.Topic] {
		key := streamKey{topic: req.Topic, subscriberId: id}
		b.topicSubscriberLocks[key].Lock()
		err := stream.Send(&pb.Data{Topic: req.Topic, Data: req.Data})
		b.topicSubscriberLocks[key].Unlock()
		if err != nil {
			erroredSubscribers = append(erroredSubscribers, key)
		}
	}
	b.mu.RUnlock()
	b.removeErroredSubscribers(erroredSubscribers)
	if len(erroredSubscribers) > 0 {
		return &pb.PublishResponse{Success: false}, fmt.Errorf("failed to send to a few subscribers")
	}
	return &pb.PublishResponse{Success: true}, nil
}

func (b *Broker) removeErroredSubscribers(erroredSubscribers []streamKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, key := range erroredSubscribers {
		delete(b.subscribers[key.topic], key.subscriberId)
		delete(b.topicSubscriberLocks, key)
	}
}
