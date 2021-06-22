package ringbuffer

var msgSubscriptionManager *SubscriptionManager

func init() {
	//初始化原始消息订阅管理器
	msgSubscriptionManager = new(SubscriptionManager)
	msgSubscriptionManager.Init(100000)
}

func MessageBus() *SubscriptionManager {
	return msgSubscriptionManager
}
