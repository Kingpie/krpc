package krpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"krpc/codec"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 9527

const (
	connected        = "200 Connected to KRPC"
	defaultRPCPath   = "/_kprc_"
	defaultDebugPath = "/debug/krpc"
)

// ServeHTTP handles HTTP requests for the server.
// It only accepts CONNECT method requests and upgrades them to raw TCP connections.
// This method implements the http.Handler interface.
//
// Parameters:
//
//	w: the HTTP response writer
//	req: the incoming HTTP request
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	// Hijack the HTTP connection to get direct access to the underlying TCP connection
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}

	// Send HTTP CONNECT response and start serving the connection
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// HandleHTTP registers an HTTP handler for RPC messages on rpcPath.
// It is still necessary to invoke http.Serve(), typically in a go statement.
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, server)
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

// HandleHTTP is a convenient approach for default server to register HTTP handlers
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}

type Option struct {
	MagicNumber    int //make sure this is a krpc request
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.JsonType,
	ConnectTimeout: 10 * time.Second,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (server *Server) Accept(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("rpc server accept error:", err)
			return
		}

		go server.ServeConn(conn)
	}
}

func Accept(l net.Listener) { DefaultServer.Accept(l) }

// serveCodec 处理客户端连接的编解码器，负责读取请求、处理请求并发送响应
// 参数:
//
//	cc: codec.Codec 接口，用于编解码请求和响应数据
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	// sending 互斥锁确保响应发送的完整性，避免多个 goroutine 同时发送数据造成混乱
	sending := new(sync.Mutex)
	// wg 等待组用于等待所有请求处理 goroutine 完成后再关闭连接
	wg := new(sync.WaitGroup)

	// 持续读取并处理客户端请求
	for {
		// 读取下一个请求
		req, err := server.readRequest(cc)
		if err != nil {
			// 如果请求为空，说明连接已断开或发生严重错误，无法恢复，退出循环
			if req == nil {
				break
			}
			// 设置错误信息并发送错误响应
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		// 增加等待组计数，启动新的 goroutine 处理请求
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}

	// 等待所有请求处理完成
	wg.Wait()

	_ = cc.Close()
}

// ServeConn 处理单个客户端连接的RPC服务
// 参数:
//
//	conn: 客户端连接，实现io.ReadWriteCloser接口，用于读取请求和写入响应
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	// 函数退出时确保连接被关闭
	defer func() { _ = conn.Close() }()

	var opt Option

	// 从连接中读取并解析RPC选项
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}

	// 验证魔数
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}

	// 根据指定的编解码器类型获取对应的创建函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	// 使用指定编解码器处理具体的RPC调用
	server.serveCodec(f(conn), &opt)
}

// when error occurs,invalidRequest is a placeholder for response
var invalidRequest = struct{}{}

// request stores all information of a call
type request struct {
	h           *codec.Header // header of request
	argv, reply reflect.Value // argv and replyv of request
	mtype       *methodType
	svc         *service
}

// readRequestHeader 从编解码器中读取RPC请求头信息
// 参数:
//
//	cc: 编解码器接口，用于读取数据
//
// 返回值:
//
//	*codec.Header: 读取到的请求头信息指针
//	error: 读取过程中发生的错误，如果读取成功则为nil
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		// 当读取头信息出错时，记录错误日志（除了EOF和意外EOF错误）
		if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// readRequest 从编解码器中读取并解析RPC请求
// 参数:
//
//	cc: 编解码器接口，用于读取请求数据
//
// 返回值:
//
//	*request: 解析后的请求结构体指针
//	error: 读取或解析过程中发生的错误
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	// 读取请求头信息
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	// 创建请求对象并设置请求头
	req := &request{h: h}

	// 查找对应的服务和方法
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}

	// 创建参数和回复值的反射对象
	req.argv = req.mtype.newArgv()
	req.reply = req.mtype.newReplyv()

	// 确保argvi是指针类型，因为ReadBody方法需要指针作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	// 读取请求体数据
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}

	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

// handleRequest 处理单个请求的主函数
// cc: 编解码器，用于序列化和反序列化数据
// req: 请求对象，包含服务调用的相关信息
// sending: 互斥锁，确保响应发送的线程安全
// wg: 等待组，用于同步多个请求的完成
func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		// 调用实际的服务方法
		err := req.svc.call(req.mtype, req.argv, req.reply)
		if err != nil {
			// 服务调用出错，设置错误信息并发送错误响应
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sending)
			return
		}

		// 服务调用成功，发送正常响应
		server.sendResponse(cc, req.h, req.reply.Interface(), sending)
		sent <- struct{}{}
	}()

	//无超时限制
	if timeout == 0 {
		<-called
		<-sent
		return
	}

	//超时处理
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

// Register publishes in the server the set of methods of the
// Register 注册一个新的服务到服务器中
// 参数:
//
//	rcvr - 需要注册的服务对象，通常是一个包含可导出方法的结构体实例
//
// 返回值:
//
//	error - 如果服务注册成功返回nil，如果服务名称已存在则返回错误信息
func (server *Server) Register(rcvr interface{}) error {
	// 创建新的服务实例
	s := newService(rcvr)

	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}

	return nil
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// findService 根据服务方法名称查找对应的服务和方法类型
// 参数:
//
//	serviceMethod - 服务方法名称，格式为"服务名.方法名"
//
// 返回值:
//
//	svc - 找到的服务实例
//	mtype - 找到的方法类型
//	err - 错误信息，如果查找失败则返回相应错误
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	// 解析服务名和方法名
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]

	// 从服务映射表中查找服务
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)

	// 查找方法类型
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}
