package ringbuffer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"sync"
	"time"
)

const MSG_RING_MIN_SIZE = 10
const MSG_RING_DEFAULT_SIZE = 1e5
const MSG_RING_MAX_SIZE = 1e7

/**
 * 保存消息内容的封装实体，一方面可以确保msg ID同步保存，另一方面可以确保即便传入空消息也会对应一个非空消息实体存在，避免nil对处理产生影响
 */
type MessageEntity struct {
	MessageId   int64  //每个插入保存的消息分配一个唯一ID，为了便于顺序消费，ID将通过时间戳生成
	MessageType string // TODO: 是否改为 Topic，增加过滤功能
	Content     interface{}
	ReceiveTime time.Time
}

/**
 * 消息内容过滤接口
 */
type MessageFilter interface {
	FilterMessage(*MessageEntity) bool
}

/**
 * 由于消息持久化时，会变成无类型的JSON格式，因此通过一个converter在恢复消息时进行转换
 */
type MessageRestoreConverter interface {
	ConvertMessage(*MessageEntity) *MessageEntity
}

/**
 * 用来缓存消息的Buffer，为了避免内存无限占用，采用环形机制进行存储。container/ring貌似也可以实现类似功能，但索引访问支持不好
 */
type MessageRingBuffer struct {
	bufferSize       int              //定义环形队列内部存储空间大小
	writeCursor      int              //记录当前RingBuffer写入的位置，为数组的索引
	msgBuffer        []*MessageEntity //用于实际存储消息的数组，通过维护存储游标实现环形存储
	msgCount         int              //当前存入环形队列实际消息的数量
	msgIdSeqInSecond int64            //在一个时间戳内的ID序列，解决1个时间戳内存入多个消息的ID重叠性
	minMsgId         int64            //存入环形队列消息的最小ID
	maxMsgId         int64            //存入环形队列消息的最大ID
	msgIdToIndexMap  map[int64]int    //用于将msgID与其落位索引进行倒排映射，便于通过msgID快速获取消息内容
	lock             sync.RWMutex     //在并发读取场景下控制访问安全的读写锁
}

func (this *MessageRingBuffer) Init(bufferSize int) {
	if bufferSize <= 0 {
		this.bufferSize = MSG_RING_DEFAULT_SIZE
	} else if bufferSize <= MSG_RING_MIN_SIZE {
		this.bufferSize = MSG_RING_MIN_SIZE
	} else if bufferSize > MSG_RING_MAX_SIZE {
		this.bufferSize = MSG_RING_MAX_SIZE
	} else {
		this.bufferSize = bufferSize
	}
	this.msgBuffer = make([]*MessageEntity, this.bufferSize)
	this.msgCount = 0
	this.msgIdToIndexMap = make(map[int64]int)
	this.writeCursor = -1 //写入游标初始化为-1，方便第一个消息直接的落位索引直接计算为0
}

/**
 * 以指定秒数周期性地，将环形队列数据持久化到指定路径的文件中
 */
func (this *MessageRingBuffer) PersistBufferToFilePeriodically(persistPath string, period int) {
	go func() {
		for {
			time.Sleep(time.Second * time.Duration(period))
			this.PersistBufferToFile(persistPath)
		}
	}()
}

/**
 * 以json格式将环形队列数据持久化到指定路径的文件中
 */
func (this *MessageRingBuffer) PersistBufferToFile(persistPath string) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	if len(this.msgBuffer) == 0 {
		fmt.Println("message ring buffer is empty")
		return nil
	}
	//持久化存储时需要对消息按照ID从小到大排序，为了避免对既有内存产生影响，需要单独copy一份数据出来
	var ringBufferCopy RingMessageArray
	for _, msg := range this.msgBuffer {
		if msg == nil {
			break
		}
		ringBufferCopy = append(ringBufferCopy, msg)
	}
	//由于环形队列内部可能已经发生覆盖性写入，这种场景会导致重新载入消息时新旧消息位置颠倒，因此在持久化的消息按照消息ID进行排序，方便进程重启后的载入
	sort.Sort(ringBufferCopy)
	ringBufferJsonBytes, err := json.Marshal(ringBufferCopy)
	if err != nil {
		fmt.Println("marshal message ring buffer err: %s", err.Error())
		return err
	}
	err = ioutil.WriteFile(persistPath, ringBufferJsonBytes, 0777)
	if err != nil {
		fmt.Println("persist message ring buffer err: %s", err.Error())
		return err
	}
	return nil
}

/**
 * 从消息持久化文件中重新载入消息，避免进程重启后丢失数据
 */
func (this *MessageRingBuffer) RestoreBufferFromFile(persistPath string, msgConverter MessageRestoreConverter) error {
	ringBufferJsonBytes, err := ioutil.ReadFile(persistPath)
	if err != nil {
		fmt.Println("read message ring buffer file err: %s", err.Error())
		return err
	}
	var restoredRingBuffer []*MessageEntity
	//注意：从文件重新加载后，所有的数据类型都会丢失，msg.Content会变成map格式
	err = json.Unmarshal(ringBufferJsonBytes, &restoredRingBuffer)
	if err != nil {
		fmt.Println("unmarshal message ring buffer file err: %s", err.Error())
		return err
	}
	for _, msg := range restoredRingBuffer {
		if msgConverter != nil {
			msg = msgConverter.ConvertMessage(msg)
		}
		this.PublishMessage(msg)
	}
	return nil
}

/**
 * 获取到真实的纳秒，已验证无碰撞，ref: https://github.com/golang/go/issues/27301
 */
func (this *MessageRingBuffer) NowInNanosecond() int64 {
	a := time.Now()
	b := time.Now()
	nano := b.Sub(a).Nanoseconds()
	return a.UnixNano() + nano
}

/**
 * 向环形队列内存入消息
 */
func (this *MessageRingBuffer) PublishMessage(message interface{}) *MessageEntity {
	this.lock.Lock()
	defer this.lock.Unlock()
	var msgId int64
	//如果传入的消息格式为MessageEntity，则说明是从持久化文件中恢复数据，此场景复用旧的ID、时间戳信息
	messageRingItem, ok := message.(*MessageEntity)
	if ok {
		//fmt.Println("ring buffer restore: %v", message)
		msgId = messageRingItem.MessageId
	} else {
		fmt.Println("ring buffer receive: ", message)
		msgId = this.NowInNanosecond()
		messageRingItem = &MessageEntity{MessageId: msgId, Content: message, ReceiveTime: time.Now()}
	}
	//计算新消息写入落位
	nextCursorPosition := this.writeCursor + 1
	if nextCursorPosition >= this.bufferSize {
		nextCursorPosition = 0
	}
	//通过写入位置是否已经有了消息，来确定是否属于覆盖写入
	oldMsg := this.msgBuffer[nextCursorPosition]
	if oldMsg != nil {
		//消息数量已经多到产生了循环覆盖插入，则更新min msg ID，并从msgID与落位索引的map中清除被覆盖的msgID
		delete(this.msgIdToIndexMap, oldMsg.MessageId)
		afterNextCursorPosition := nextCursorPosition + 1
		if afterNextCursorPosition >= this.bufferSize {
			afterNextCursorPosition = 0
		}
		//如果是产生了覆盖存储，earliestMsg是不可能为nil
		earliestMsg := this.msgBuffer[afterNextCursorPosition]
		this.minMsgId = earliestMsg.MessageId
	} else {
		//如果还没有产生循环覆盖，则保持使用第1个消息的ID作为min msg ID。如果是刚开始放入第一个消息，则对min msg ID进行初始化
		if nextCursorPosition == 0 {
			this.minMsgId = msgId
		}
		this.msgCount += 1
	}
	// TODO: 这里的 msgBuffer 一直在增加，什么时候减少
	this.msgBuffer[nextCursorPosition] = messageRingItem
	this.msgIdToIndexMap[msgId] = nextCursorPosition
	this.maxMsgId = msgId
	this.writeCursor = nextCursorPosition
	return messageRingItem
}

/**
 * 从环形队列中获取指定ID的消息
 */
func (this *MessageRingBuffer) GetMessage(messageId int64) *MessageEntity {
	this.lock.RLock()
	defer this.lock.RUnlock()
	msgIndex, ok := this.msgIdToIndexMap[messageId]
	if !ok {
		return nil
	}
	return this.msgBuffer[msgIndex]
}

/**
 * 获取环形队列内部Buffer大小，由于是初始化就固定的值，因此不用加锁访问
 */
func (this *MessageRingBuffer) GetMessageBufferSize() int {
	return this.bufferSize
}

/**
 * 获取环形队列内实际保存的消息数量
 */
func (this *MessageRingBuffer) GetMessageCount() int {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.msgCount
}

func (this *MessageRingBuffer) GetMinMessageId() int64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.minMsgId
}

func (this *MessageRingBuffer) GetMaxMessageId() int64 {
	this.lock.RLock()
	defer this.lock.RUnlock()
	return this.maxMsgId
}

//声明一个可排序数组，用于返回排序后的msg数组，以避免推送的消息乱序
type RingMessageArray []*MessageEntity

func (arr RingMessageArray) Len() int {
	return len(arr)
}
func (arr RingMessageArray) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}
func (arr RingMessageArray) Less(i, j int) bool {
	return arr[j].MessageId > arr[i].MessageId
}

/**
 * 获取某个时间点后所有的消息
 */
func (this *MessageRingBuffer) GetMessageAfterTime(beginTime time.Time) []*MessageEntity {
	return this.GetMessageAfterMsgId(beginTime.UnixNano())
}

/**
 * 获取某个ID后所有的消息
 */
func (this *MessageRingBuffer) GetMessageAfterMsgId(msgBeginId int64) []*MessageEntity {
	return this.GetMessageBetweenId(msgBeginId, time.Now().Add(time.Hour*24).UnixNano())
}

/**
 * 获取某个时间段内的消息
 */
func (this *MessageRingBuffer) GetMessageBetweenTime(beginTime, endTime time.Time) []*MessageEntity {
	return this.GetMessageBetweenId(beginTime.UnixNano(), endTime.UnixNano())
}

/**
 * 获取某两个ID间的消息
 */
func (this *MessageRingBuffer) GetMessageBetweenId(msgBeginId, msgEndId int64) []*MessageEntity {
	this.lock.RLock()
	defer this.lock.RUnlock()
	var msgList RingMessageArray
	if msgBeginId > this.maxMsgId {
		return msgList
	}
	if msgBeginId < this.minMsgId {
		msgBeginId = this.minMsgId
	}
	startIndex := 0
	msgIndex, ok := this.msgIdToIndexMap[msgBeginId]
	if ok {
		startIndex = msgIndex
	}
	//从指定的游标位置开始查找，以提升效率，如果没有找到对应游标位置则默认从第一个元素开始查找
	for i := startIndex; i < this.bufferSize; i++ {
		msg := this.msgBuffer[i]
		if msg == nil {
			//如果找到了空元素，则说明还没有产生覆盖写入，既有的消息顺序可以得到保证，不用排序直接返回即可
			return msgList
		}
		if msg.MessageId > msgEndId {
			return msgList
		}
		if msg.MessageId >= msgBeginId && msg.MessageId <= msgEndId {
			msgList = append(msgList, msg)
		}
	}
	//对于已经覆盖写入的情况，从头进行前半截查找，补上错过的
	//TODO：这里情况比较绕，需要再Check一下
	for i := 0; i < startIndex; i++ {
		msg := this.msgBuffer[i]
		if msg.MessageId >= msgBeginId && msg.MessageId <= msgEndId {
			msgList = append(msgList, msg)
		}
	}
	sort.Sort(msgList)
	return msgList
}

/**
 * 基于提供的过滤器，获取最新的total条消息
 */
func (this *MessageRingBuffer) GetFilteredMessage(filter MessageFilter, total int) []*MessageEntity {
	this.lock.RLock()
	defer this.lock.RUnlock()
	var msgList RingMessageArray
	maxMsgIndex, ok := this.msgIdToIndexMap[this.maxMsgId]
	if !ok {
		return nil
	}
	for i := maxMsgIndex; i >= 0; i-- {
		msg := this.msgBuffer[i]
		if filter.FilterMessage(msg) {
			msgList = append(msgList, msg)
		}
		if len(msgList) >= total {
			break
		}
	}
	sort.Sort(msgList)
	return msgList
}

func (this *MessageRingBuffer) ToString() string {
	return fmt.Sprintf("msg count:%d\nmin msg id:%d \nmax msg id:%d\n", this.msgCount, this.minMsgId, this.maxMsgId)
}
