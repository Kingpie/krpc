package krpc

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

type methodType struct {
	method    reflect.Method //方法本身
	ArgType   reflect.Type   //第一个参数类型
	ReplyType reflect.Type   //第二个参数类型
	numCalls  uint64         //调用次数
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	// arg may be a pointer type, or a value type
	if m.ArgType.Kind() == reflect.Ptr {
		argv = reflect.New(m.ArgType.Elem())
	} else {
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

func (m *methodType) newReplyv() reflect.Value {
	// reply must be a pointer type
	reply := reflect.New(m.ReplyType.Elem())
	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		reply.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		reply.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}
	return reply
}

type service struct {
	name   string                 //映射结构体名称
	typ    reflect.Type           //映射结构体类型
	rcvr   reflect.Value          //映射结构体本身
	method map[string]*methodType //存储结构体方法
}

func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)

	//必须大写开头
	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server: %s is not a valid service name", s.name)
	}
	s.registerMethods()
	return s
}

// 注册结构体中的方法
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	methods := s.typ.NumMethod()
	for i := 0; i < methods; i++ {
		method := s.typ.Method(i)
		mType := method.Type

		//入参必须是2个(还有个this忽略掉)，返回值必须是1个
		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			continue
		}

		//返回值必须是error
		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			continue
		}

		argType, replyType := mType.In(1), mType.In(2)

		// 参数名首字母大写或者内置类型
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}
		s.method[method.Name] = &methodType{
			method:    method,
			ArgType:   argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server: register %s.%s\n", s.name, method.Name)
	}
}

func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	//调用计数
	atomic.AddUint64(&m.numCalls, 1)
	f := m.method.Func

	//按序调用方法
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}

// 判断参数是否是导出类型或者内置类型
func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
