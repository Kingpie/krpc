package krpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"krpc/codec"
	"log"
	"net"
	"sync"
)

type Call struct {
	Seq           uint64      //唯一id
	ServiceMethod string      //格式 "<service>.<method>"
	Args          interface{} //arguments
	Reply         interface{} //reply
	Error         error
	Done          chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	c        codec.Codec //编解码
	opt      *Option
	sending  sync.Mutex //保证有序发送
	header   codec.Header
	mu       sync.Mutex
	seq      uint64           // 唯一 id
	pending  map[uint64]*Call //存储未处理完的请求
	closing  bool             //我自己关闭
	shutdown bool             //server告诉我关闭
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shutdown")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.c.Close()
}

func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// registerCall 注册一个调用请求到客户端的待处理队列中
// 参数:
//
//	call: 需要注册的调用请求指针
//
// 返回值:
//
//	uint64: 分配给该调用的序列号
//	error: 如果客户端正在关闭或已关闭则返回ErrShutdown错误，否则返回nil
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	// 检查客户端是否正在关闭或已经关闭
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}

	// 为调用分配序列号并加入待处理队列
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// removeCall 从客户端的待处理调用映射中移除指定序列号的调用
// 参数:
//
//	seq - 要移除的调用的序列号
//
// 返回值:
//
//	*Call - 被移除的调用对象，如果不存在则返回nil
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()

	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// terminateCalls 终止所有待处理的调用
//
// 该函数会设置客户端的关闭状态，并将指定的错误分配给所有待处理的调用，
// 然后标记这些调用为完成状态。
//
// 参数:
//
//	err - 要分配给所有待处理调用的错误信息
func (client *Client) terminateCalls(err error) {
	// 锁定发送和客户端状态，确保线程安全
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()

	// 标记客户端为关闭状态
	client.shutdown = true

	// 遍历所有待处理的调用，设置错误并标记完成
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// receive 从客户端连接中接收响应消息并处理
// 该函数会持续读取服务端返回的响应，直到发生错误为止
// 参数: 无
// 返回值: 无
func (client *Client) receive() {
	var err error

	// 持续接收响应消息，直到出现错误
	for err == nil {
		var h codec.Header

		// 读取消息头
		if err = client.c.ReadHeader(&h); err != nil {
			log.Println("read header err:", err)
			break
		}

		// 根据序列号获取对应的调用请求
		call := client.removeCall(h.Seq)

		// 根据调用请求的处理情况分别处理
		switch {
		case call == nil:
			// 调用不存在，直接读取并丢弃消息体
			err = client.c.ReadBody(nil)
		case h.Error != "":
			// 服务端返回错误，设置调用错误并完成调用
			call.Error = fmt.Errorf(h.Error)
			err = client.c.ReadBody(nil)
			call.done()
		default:
			// 正常响应，读取消息体到调用的回复字段中
			err = client.c.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}

	// 终止所有未完成的调用
	client.terminateCalls(err)
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// send options with server
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		c:       cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// Dial 创建一个新的客户端连接
// network: 网络类型，如 "tcp", "udp" 等
// address: 目标地址，如 "localhost:8080"
// opts: 可选的配置选项列表
// 返回值 client: 成功创建的客户端实例
// 返回值 err: 连接过程中发生的错误
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		log.Println("parse option err:", err)
		return nil, err
	}
	conn, err := net.Dial(network, address)
	if err != nil {
		log.Println("net dial err:", err)
		return nil, err
	}
	// 如果客户端创建失败，确保关闭连接
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn, opt)
}

func (client *Client) send(call *Call) {
	// make sure that the client will send a complete request
	client.sending.Lock()
	defer client.sending.Unlock()

	// register this call.
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// prepare request header
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq

	// encode and send the request
	if err := client.c.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)

		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

func (client *Client) Call(serviceMethod string, args, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).Done
	return call.Error
}
