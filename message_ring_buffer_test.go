package ringbuffer

import (
	"fmt"
	"sync"
	"testing"
)

func TestMessageRingBuffer_PutMessage(t *testing.T) {
	mb := new(MessageRingBuffer)
	mb.Init(15)
	msgEntity := mb.PublishMessage("a")
	fmt.Println("msg id:", msgEntity, mb.GetMessage(msgEntity.MessageId))
	fmt.Println("id list:", mb.GetMessageAfterMsgId(0))
	fmt.Println(mb.ToString())
	msgEntity = mb.PublishMessage("b")
	fmt.Println("msg id:", msgEntity, mb.GetMessage(msgEntity.MessageId))
	fmt.Println(mb.ToString())
	for i := 0; i < 20; i++ {
		msgEntity := mb.PublishMessage(fmt.Sprintf("%d", i))
		fmt.Println("msg id:", msgEntity.MessageId, msgEntity.Content)
		fmt.Println(mb.ToString())
	}
	fmt.Println("id list:", mb.GetMessageAfterMsgId(0))
}

func printMsgIdList(msgIdList []*MessageEntity) {
	fmt.Print("id list: [")
	for _, msg := range msgIdList {
		fmt.Print(" ", msg.MessageId)
	}
	fmt.Println(" ]")
}

func TestMessageRingBuffer_GetIdListAfter(t *testing.T) {
	mb := new(MessageRingBuffer)
	mb.Init(15)
	mb.PublishMessage("a")
	printMsgIdList(mb.GetMessageAfterMsgId(0))
	mb.PublishMessage("b")
	for i := 0; i < 20; i++ {
		mb.PublishMessage(fmt.Sprintf("%d", i))
		printMsgIdList(mb.GetMessageAfterMsgId(0))
	}
	printMsgIdList(mb.GetMessageAfterMsgId(0))
}

func TestMessageRingBuffer_PutMessageConcurrent(t *testing.T) {
	mb := new(MessageRingBuffer)
	mb.Init(1500)
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				msgEntity := mb.PublishMessage(fmt.Sprintf("%d-%d", w, i))
				fmt.Println("msg id:", msgEntity.MessageId, msgEntity.Content)
				fmt.Println(mb.ToString())
			}
		}(w)
	}
	wg.Wait()
}

func TestMessageRingBuffer_PutMessagePressure(t *testing.T) {
	//性能可以压到60万IOPS
	mb := new(MessageRingBuffer)
	mb.Init(0)
	var wg sync.WaitGroup
	for w := 0; w < 100; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 10000000; i++ {
				msg := mb.PublishMessage(fmt.Sprintf("%d-%d", w, i))
				if false {
					if i%19 == 0 {
						fmt.Printf("worker-%d write at %d \tmsgId=%d\n", w, i, msg.MessageId)
					}
				}
			}
			fmt.Println(mb.ToString())
		}(w)
	}
	wg.Wait()
}
