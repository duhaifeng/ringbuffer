package ringbuffer

import (
	"aio-post-app/common"
	"fmt"
)

/**
 * 消息订阅者接口定义
 */
type MessageSubscriber interface {
	Subscribe(*MessageEntity)
}

/**
 * 消息订阅者的包装器，将一个订阅者和一个缓存Channel关联起来，这样来了消息后先进行缓存，再异步推送到订阅者处理，避免阻塞整个消息总线
 */
type MessageSubscriberWrapper struct {
	msgChannelCap  int
	msgChannel     chan *MessageEntity
	subscriberName string
	subscriber     MessageSubscriber
}

/**
 * 初始化函数，只能调一次；TODO: 是否有其它机制保障只调一次？
 */
func (this *MessageSubscriberWrapper) Init(name string, subscriber MessageSubscriber) {
	this.msgChannelCap = 10000
	this.msgChannel = make(chan *MessageEntity, this.msgChannelCap)
	this.subscriberName = name
	this.subscriber = subscriber
	go func() {
		for {
			this.sendMessageToSubscriber()
		}
	}()
}

/**
 * 接收并缓存一条推送过来的消息，如果缓存管道已满，则直接丢弃消息
 */
func (this *MessageSubscriberWrapper) NotifyMessage(msg *MessageEntity) {
	if len(this.msgChannel) >= this.msgChannelCap {
		fmt.Println("throw msg: ", this.subscriberName, msg.Content)
		return
	}
	this.msgChannel <- msg
}

/**
 * 从消息缓存管道中消费消息，并交由订阅者处理，由于订阅者内部可能未有效处理异常，因此需要recover()，代价是需要在Init()中通过循环调用本方法
 */
func (this *MessageSubscriberWrapper) sendMessageToSubscriber() {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Printf("message subscriber<%s> process panic: %v\n", this.subscriberName, err)
		}
	}()
	msg, ok := <-this.msgChannel
	if ok && this.subscriber != nil {
		this.subscriber.Subscribe(msg)
	}
}
