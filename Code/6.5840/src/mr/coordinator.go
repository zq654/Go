package mr

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock          sync.Mutex
	statusMap     sync.Map //记录原始数据状态 0代表未执行，1代表执行中，2代表执行成功
	fileCancelMap sync.Map
	dataChan      chan File
	zoneChan      chan Zone
	wgMap         sync.WaitGroup
	wgReduce      sync.WaitGroup
	intermediates []*[]KeyValue //代表长度为NReduce 的KeyValue数组 的channel

}
type File struct {
	Name string
	Data string
}
type Zone struct {
	Index         int
	Intermediates []KeyValue
}

var index = int64(0)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) GetFile(req *MapArgs, rsp *MapReply) error {
	select {
	case data := <-c.dataChan:
		status, _ := c.statusMap.Load(data.Name)
		if status.(int) == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			rsp.File = data
			c.fileCancelMap.Store(data.Name, cancel)
			c.statusMap.Store(data.Name, 1)
			rsp.HasData = true
			go func() {
				select {
				case <-ctx.Done():
					//如果超超时了将当初那个数据放回channel中
					if ctx.Err() == context.DeadlineExceeded {
						//fmt.Printf("Map执行超时%s\n", data.Name)
						c.statusMap.Store(data.Name, 0)
						c.fileCancelMap.Delete(data.Name)
						c.dataChan <- data
					} else if ctx.Err() == context.Canceled {
						//fmt.Printf("Map成功执行%s\n", data.Name)
						c.statusMap.Store(data.Name, 2)
						c.wgMap.Done()
					}
				}
			}()
			return nil
		}
		return nil
	default:
		rsp.HasData = false
		return nil
	}
}

//	func (c *Coordinator) ExecCancelFunc(args *MapArgs, reply *MapReply) error {
//		cancel, ok := c.fileCancelMap.Load(args.Name)
//		if ok {
//			cancelFunc := cancel.(context.CancelFunc)
//			cancelFunc()
//			reply.HasCancel = true
//			return nil
//		}
//		reply.HasCancel = false
//		return nil
//	}
func (c *Coordinator) ExecReduceCancelFunc(args *ReduceArgs, reply *ReduceReply) error {
	cancel, ok := c.fileCancelMap.Load(args.Index)
	if ok {
		cancelFunc := cancel.(context.CancelFunc)
		cancelFunc()
		return nil
	}
	return nil
}

func (c *Coordinator) Partition(args *MapArgs, reply *MapReply) error {
	KeyValues := args.KVS
	c.lock.Lock()
	status, _ := c.statusMap.Load(args.Name)
	cancel, ok := c.fileCancelMap.Load(args.Name)
	if status.(int) != 2 && ok {
		//fmt.Printf("------往临时文件中写入%s-------\n", args.Name)
		for i, _ := range KeyValues {
			indexNew := ihash(KeyValues[i].Key) % len(c.intermediates)
			*c.intermediates[indexNew] = append(*c.intermediates[indexNew], KeyValues[i])
		}
		cancelFunc := cancel.(context.CancelFunc)
		cancelFunc()
	}
	c.lock.Unlock()
	return nil
}
func (c *Coordinator) GetIntermediates(args *ReduceArgs, reply *ReduceReply) error {
	c.wgMap.Wait()
	select {
	case zone := <-c.zoneChan:
		status, _ := c.statusMap.Load(zone.Index)
		if status.(int) == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			reply.Zone = zone
			c.fileCancelMap.Store(zone.Index, cancel)
			c.statusMap.Store(zone.Index, 1)
			reply.HasZone = true
			go func() {
				select {
				case <-ctx.Done():
					//如果超超时了将当初那个数据放回channel中
					if ctx.Err() == context.DeadlineExceeded {
						//fmt.Printf("Reduce执行超时%d\n", zone.Index)
						c.statusMap.Store(zone.Index, 0)
						c.fileCancelMap.Delete(zone.Index)
						c.zoneChan <- zone
					} else if ctx.Err() == context.Canceled {
						//fmt.Printf("Reduce成功执行%d\n", zone.Index)
						c.statusMap.Store(zone.Index, 2)
						c.wgReduce.Done()
					}
				}
			}()
		}
	default:
		reply.HasZone = false
	}
	return nil
}
func (c *Coordinator) MapDone(args *FlagArgs, reply *FlagReply) error {
	c.wgMap.Wait()
	reply.MapIsDone = true
	return nil
}

func (c *Coordinator) ReduceDone(args *FlagArgs, reply *FlagReply) error {
	c.wgReduce.Wait()
	reply.ReduceIsDone = true
	return nil
}

func getIndex() int {
	newIndex := int(atomic.AddInt64(&index, 1))
	return newIndex
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.wgReduce.Wait()
	ret := true
	// Your code here.
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	//往channel中放入原始数据 初始化原始数据的状态 增加等待锁
	c.dataChan = make(chan File, len(files))
	c.statusMap = sync.Map{}
	c.fileCancelMap = sync.Map{}
	c.wgMap = sync.WaitGroup{}
	c.wgReduce = sync.WaitGroup{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		c.dataChan <- File{Name: filename, Data: string(content)}
		c.statusMap.Store(filename, 0)
		c.wgMap.Add(1)
	}
	//初始化partition切片
	c.zoneChan = make(chan Zone, nReduce)
	c.intermediates = make([]*[]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kv := make([]KeyValue, 0)
		c.intermediates[i] = &kv
		c.wgReduce.Add(1)
	}
	go func() {
		c.wgMap.Wait()
		for i := 0; i < nReduce; i++ {
			zone := Zone{
				Index:         getIndex(),
				Intermediates: *c.intermediates[i],
			}
			c.zoneChan <- zone
			c.statusMap.Store(zone.Index, 0)
		}
	}()
	c.server()
	return &c
}
