package main

import (
	"golang.org/x/sys/unix"
	"log"
	"time"
)

// AE Loop
// FileEvent
// Type Readable / Writeable

// TimeEvent
// Type Normal / Once

type FeType int
type TeType int

// FileEvent事件类型与Epoll监听类型映射
var FeToEpoll [3]uint32 = [3]uint32{0, unix.EPOLLIN, unix.EPOLLOUT}

const (
	FeReadable  FeType = 0x01
	FeWriteable FeType = 0x02
	TeNormal    TeType = 0x01
	TeOnce      TeType = 0x02
)

// AeFileProc File Event回调函数
// AeTimeProc Time Event回调函数

type AeFileProc func(loop *AeLoop, fd int, extra interface{})
type AeTimeProc func(loop *AeLoop, id int, extra interface{})

type AeFileEvent struct {
	// 文件描述符
	fd   int
	mask FeType
	// 回调函数
	proc  AeFileProc
	extra interface{}
}

type AeTimeEvent struct {
	id   int
	mask TeType
	// 下一次执行时间
	nextExecTime int64
	// 执行间隔（仅NORMAL事件）
	interval int64
	proc     AeTimeProc
	// 链表方式存储
	next  *AeTimeEvent
	extra interface{}
}

type AeLoop struct {
	AeFileEvents map[int]*AeFileEvent
	// AeTimeEvent链表头节点
	AeTimeEvents *AeTimeEvent
	// fileEvent文件描述符
	fileEventFd     int
	timeEventNextId int
	stop            bool
}

func getAeFileEventKey(fd int, mask FeType) int {
	if mask == FeReadable {
		return fd
	} else {
		return fd * -1
	}
}

// getEpollMask 判断文件描述符fd是否注册过EPOLL读写事件
func (loop *AeLoop) getEpollMask(fd int) uint32 {
	// 注册过读事件
	var mask uint32 = 0
	if _, ext := loop.AeFileEvents[getAeFileEventKey(fd, FeReadable)]; ext {
		mask |= FeToEpoll[FeReadable]
	}
	// 注册过写事件
	if _, ext := loop.AeFileEvents[getAeFileEventKey(fd, FeWriteable)]; ext {
		mask |= FeToEpoll[FeWriteable]
	}
	return mask
}

// AddFileEvent 添加FileEvent(添加EPOLL监听)
func (loop *AeLoop) AddFileEvent(fd int, mask FeType, proc AeFileProc, extra interface{}) {
	var op int
	epollEvent := loop.getEpollMask(fd)
	if epollEvent == 0 {
		op = unix.EPOLL_CTL_ADD
	} else {
		op = unix.EPOLL_CTL_MOD
	}
	epollEvent |= FeToEpoll[mask]
	// EpollCtl 系统调用
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{
		Fd: int32(fd),
		// EPOLL 事件类型
		Events: epollEvent,
	})
	if err != nil {
		log.Printf("[AE LOOP ERROR] Ae loop epollCtl error, err: %s", err)
		return
	}
	// add to loop
	fileEvent := &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}
	loop.AeFileEvents[getAeFileEventKey(fd, mask)] = fileEvent
}

// RemoveFileEvent 移除FileEvent事件
func (loop *AeLoop) RemoveFileEvent(fd int, mask FeType) {
	var op int
	epollEvent := loop.getEpollMask(fd)
	epollEvent ^= FeToEpoll[mask]
	if epollEvent == 0 {
		op = unix.EPOLL_CTL_DEL
	} else {
		op = unix.EPOLL_CTL_MOD
	}
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{
		Fd:     int32(fd),
		Events: epollEvent,
	})
	if err != nil {
		log.Printf("[AE LOOP EPOLL_CTL ERROR] Ae loop epollCtl error, err: %s", err)
		return
	}
	// remove from loop
	delete(loop.AeFileEvents, getAeFileEventKey(fd, mask))
}

func getTime() int64 {
	// ms
	return time.Now().UnixNano()
}

// getNextExecTime 根据TimeEvent确定EPOLL_WAIT等待时间
func (loop *AeLoop) getNextExecTime() int64 {
	maxWait := getTime() + 1000
	timeEvent := loop.AeTimeEvents
	for timeEvent != nil {
		if timeEvent.nextExecTime < maxWait {
			maxWait = timeEvent.nextExecTime
		}
		timeEvent = timeEvent.next
	}
	return maxWait
}

// AddTimeEvent 添加Time Event O1 链表头插法
// interval == 0 if TeType == ONCE
func (loop *AeLoop) AddTimeEvent(mask TeType, interval int64, proc AeTimeProc, extra interface{}) int {
	nextId := loop.timeEventNextId
	loop.timeEventNextId += 1
	timeEvent := &AeTimeEvent{
		id:           nextId,
		mask:         mask,
		nextExecTime: getTime() + interval,
		interval:     interval,
		proc:         proc,
		next:         loop.AeTimeEvents,
		extra:        extra,
	}
	loop.AeTimeEvents = timeEvent
	return timeEvent.id
}

// RemoveTimeEvent 删除Time Event ON
// 若id不存在，则不做任何操作
func (loop *AeLoop) RemoveTimeEvent(delId int) {
	current := loop.AeTimeEvents
	var last *AeTimeEvent = nil
	for current != nil {
		if current.id == delId {
			if last == nil {
				loop.AeTimeEvents = current.next
			} else {
				last.next = current.next
			}
			return
		}
		last = current
		current = current.next
	}
}

// AeWait 底层EPOLL_WAIT（等待时间有限，为了及时响应TimeEvent）
func (loop *AeLoop) AeWait() ([]*AeFileEvent, []*AeTimeEvent, error) {
	now := getTime()
	waitTime := loop.getNextExecTime() - now
	// 至少等待10ms
	if waitTime < 10 {
		waitTime = 10
	}
	// 系统调用 EPOLL_WAIT返回的可以进行操作的FileEvent
	var events [128]unix.EpollEvent
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(waitTime))
	if err != nil {
		return nil, nil, err
	}
	// collect file events
	fileEvents := make([]*AeFileEvent, 0)
	for i := 0; i < n; i += 1 {
		// EPOLLIN -> FeReadable
		if events[i].Events&unix.EPOLLIN != 0 {
			if _, ext := loop.AeFileEvents[getAeFileEventKey(int(events[i].Fd), FeReadable)]; ext {
				fe := loop.AeFileEvents[getAeFileEventKey(int(events[i].Fd), FeReadable)]
				fileEvents = append(fileEvents, fe)
			}
		}
		// EPOLLOUT -> FeWriteable
		if events[i].Events&unix.EPOLLOUT != 0 {
			if _, ext := loop.AeFileEvents[getAeFileEventKey(int(events[i].Fd), FeWriteable)]; ext {
				fe := loop.AeFileEvents[getAeFileEventKey(int(events[i].Fd), FeWriteable)]
				fileEvents = append(fileEvents, fe)
			}
		}
	}
	// collect time events
	now = getTime()
	timeEvents := make([]*AeTimeEvent, 0)
	current := loop.AeTimeEvents
	for current != nil {
		if current.nextExecTime <= now {
			timeEvents = append(timeEvents, current)
		}
		current = current.next
	}
	return fileEvents, timeEvents, nil
}

// AeProcess 处理AeWait返回的文件描述符的回调
func (loop *AeLoop) AeProcess(fileEvents []*AeFileEvent, timeEvents []*AeTimeEvent) {
	for _, timeEvent := range timeEvents {
		timeEvent.proc(loop, timeEvent.id, timeEvent.extra)
		if timeEvent.mask == TeOnce {
			loop.RemoveTimeEvent(timeEvent.id)
		} else {
			timeEvent.nextExecTime = getTime() + timeEvent.interval
		}
	}
	for _, fileEvent := range fileEvents {
		fileEvent.proc(loop, fileEvent.fd, fileEvent.extra)
	}
}

// AeMain AeLoop 主流程
// AeWait <-> AeProcess
func (loop *AeLoop) AeMain() {
	for !loop.stop {
		fileEvents, timeEvents, err := loop.AeWait()
		if err != nil {
			log.Printf("[AE LOOP AEMAIN ERROR] AeWait error, err: %s", err)
			loop.stop = true
			return
		}
		if !loop.stop {
			loop.AeProcess(fileEvents, timeEvents)
		}
	}
}

// AeLoopCreate 创建一个AeLoop
func AeLoopCreate() (*AeLoop, error) {
	// 系统调用：创建EPOLL监听文件描述符
	epollFd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	loop := &AeLoop{
		AeFileEvents:    make(map[int]*AeFileEvent, 0),
		AeTimeEvents:    nil,
		timeEventNextId: 1,
		fileEventFd:     epollFd,
		stop:            false,
	}
	return loop, nil
}
