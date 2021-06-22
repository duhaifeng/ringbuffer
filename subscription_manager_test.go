package ringbuffer

import (
	"fmt"
	"testing"
	"time"
)

type TestSubscriber struct {
}

func (this *TestSubscriber) Subscribe(msg *MessageEntity) {
	fmt.Println("recv: ", msg.Content)
	time.Sleep(time.Second)
}

func TestSubscriptionManager_PutMessage(t *testing.T) {
	sm := new(SubscriptionManager)
	sm.Init(1000)
	sm.SubscribeFromMessageId(0, "sub1", new(TestSubscriber))
	for i := 0; i < 20; i++ {
		sm.PublishMessage(i)
		time.Sleep(time.Millisecond * 500)
	}
	time.Sleep(time.Second * 10)
}
