package ringbuffer

import (
	"time"
)

/**
 * 基于环形队列实现的消息订阅管理器
 */
type SubscriptionManager struct {
	msgRingBuffer *MessageRingBuffer
	subscribers   []*MessageSubscriberWrapper
}

func (this *SubscriptionManager) Init(bufferSize int) {
	this.msgRingBuffer = new(MessageRingBuffer)
	this.msgRingBuffer.Init(bufferSize)
}

func (this *SubscriptionManager) GetMessageStatus() map[string]interface{} {
	status := make(map[string]interface{})
	status["minMsgId"] = this.msgRingBuffer.minMsgId
	status["maxMsgId"] = this.msgRingBuffer.maxMsgId
	status["msgCount"] = this.msgRingBuffer.msgCount
	status["msgIdIndex"] = this.msgRingBuffer.msgIdToIndexMap
	return status
}

/**
 * 获取某个时间段内的消息
 */
func (this *SubscriptionManager) GetMessageBetweenTimestamp(beginTime, endTime time.Time) []*MessageEntity {
	return this.msgRingBuffer.GetMessageBetweenTime(beginTime, endTime)
}

/**
 * 基于过滤器，获取total条消息
 */
func (this *SubscriptionManager) GetFilteredMessage(filter MessageFilter, total int) []*MessageEntity {
	return this.msgRingBuffer.GetFilteredMessage(filter, total)
}

/**
 * 从某个时间点后开始订阅消息
 */
func (this *SubscriptionManager) SubscribeFromTimestamp(beginTime time.Time, subscriberName string, subscriber MessageSubscriber) {
	this.SubscribeFromMessageId(beginTime.UnixNano(), subscriberName, subscriber)
}

/**
 * 从某个消息ID开始订阅消息
 */
func (this *SubscriptionManager) SubscribeFromMessageId(msgBeginId int64, subscriberName string, subscriber MessageSubscriber) {
	subscriberWrapper := new(MessageSubscriberWrapper)
	subscriberWrapper.Init(subscriberName, subscriber)
	this.subscribers = append(this.subscribers, subscriberWrapper)
	toBePushMsgList := this.msgRingBuffer.GetMessageAfterMsgId(msgBeginId)
	go func() {
		//为订阅者追齐存量数据
		for _, msg := range toBePushMsgList {
			subscriber.Subscribe(msg)
		}
	}()
}

/**
 * 发布一条消息，发布的消息会通知给所有订阅者
 */
func (this *SubscriptionManager) PublishMessage(msgContent interface{}) *MessageEntity {
	newMsgEntity := this.msgRingBuffer.PublishMessage(msgContent)
	for _, subscriber := range this.subscribers {
		subscriber.NotifyMessage(newMsgEntity) // TODO: 是否改为 Topic，增加过滤功能
	}
	return newMsgEntity
}
