package micro

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	pb "github.com/plimble/protobuf/protoc-gen-go/descriptor"
	"github.com/plimble/protobuf/protoc-gen-go/generator"
)

// Paths for packages used by code generated in this file,
// relative to the import_prefix of the generator.Generator.
const (
	microPkgPath = "github.com/plimble/micro"
)

func init() {
	generator.RegisterPlugin(new(micro))
}

// micro is an implementation of the Go protocol buffer compiler's
// plugin architecture.  It generates bindings for go-micro support.
type micro struct {
	gen *generator.Generator
}

// Name returns the name of this plugin, "micro".
func (g *micro) Name() string {
	return "micro"
}

// The names for packages imported in the generated code.
// They may vary from the final path component of the import path
// if the name is used by other packages.
var (
	microPkg string
)

// Init initializes the plugin.
func (g *micro) Init(gen *generator.Generator) {
	g.gen = gen
	microPkg = generator.RegisterUniquePackageName("micro", nil)
}

// Given a type name defined in a .proto, return its object.
// Also record that we're using it, to guarantee the associated import.
func (g *micro) objectNamed(name string) generator.Object {
	g.gen.RecordTypeUse(name)
	return g.gen.ObjectNamed(name)
}

// Given a type name defined in a .proto, return its name as we will print it.
func (g *micro) typeName(str string) string {
	return g.gen.TypeName(g.objectNamed(str))
}

// P forwards to g.gen.P.
func (g *micro) P(args ...interface{}) { g.gen.P(args...) }

// Generate generates code for the services in the given file.
func (g *micro) Generate(file *generator.FileDescriptor) {
	fmt.Println("@@@@@")
	if len(file.FileDescriptorProto.Service) == 0 {
		return
	}
	g.P("// Reference imports to suppress errors if they are not otherwise used.")
	// g.P("var _ ", contextPkg, ".Context")
	// g.P("var _ ", clientPkg, ".Option")
	// g.P("var _ ", serverPkg, ".Option")
	g.P()
	for i, service := range file.FileDescriptorProto.Service {
		g.generateService(file, service, i)
	}
}

// GenerateImports generates the import declaration for this file.
func (g *micro) GenerateImports(file *generator.FileDescriptor) {
	if len(file.FileDescriptorProto.Service) == 0 {
		return
	}
	g.P("import (")
	g.P(microPkg, " ", strconv.Quote(path.Join(g.gen.ImportPrefix, microPkgPath)))
	g.P(")")
	g.P()
}

// reservedClientName records whether a client name is reserved on the client side.
var reservedClientName = map[string]bool{
// TODO: do we need any in go-micro?
}

func unexport(s string) string { return strings.ToLower(s[:1]) + s[1:] }

// generateService generates all the code for the named service.
func (g *micro) generateService(file *generator.FileDescriptor, service *pb.ServiceDescriptorProto, index int) {
	path := fmt.Sprintf("6,%d", index) // 6 means service.

	origServName := service.GetName()
	serviceName := strings.ToLower(service.GetName())
	if pkg := file.GetPackage(); pkg != "" {
		serviceName = pkg
	}
	servName := generator.CamelCase(origServName)

	g.P()
	g.P("// Client API for ", servName, " service")
	g.P()

	// Client interface.
	g.P("type ", servName, "Client interface {")
	for i, method := range service.Method {
		g.gen.PrintComments(fmt.Sprintf("%s,2,%d", path, i)) // 2 means method in a service.
		g.P(g.generateClientRequestSignature(servName, method))
		g.P(g.generateClientPublishSignature(servName, method))
	}
	g.P("}")
	g.P()

	// Client structure.
	g.P("type ", unexport(servName), "Client struct {")
	g.P("c ", microPkg, ".Client")
	g.P("prefix string")
	g.P("}")
	g.P()

	// NewClient factory.
	g.P("func New", servName, "Client (prefix string, c ", microPkg, ".Client) ", servName, "Client {")
	g.P("return &", unexport(servName), "Client{")
	g.P("c: c,")
	g.P("prefix: prefix,")
	g.P("}")
	g.P("}")
	g.P()
	var methodIndex int
	serviceDescVar := "_" + servName + "_serviceDesc"
	// Client method implementations.
	for _, method := range service.Method {
		var descExpr string
		if !method.GetServerStreaming() {
			// Unary RPC method
			descExpr = fmt.Sprintf("&%s.Methods[%d]", serviceDescVar, methodIndex)
			methodIndex++
		}
		g.generateClientRequestMethod(serviceName, servName, serviceDescVar, method, descExpr)
		g.generateClientPublishMethod(serviceName, servName, serviceDescVar, method, descExpr)
	}

	g.P("// Server API for ", servName, " service")
	g.P()

	// Server interface.
	// serverType := servName + "Handler"
	// g.P("type ", serverType, " interface {")
	for _, method := range service.Method {
		// g.gen.PrintComments(fmt.Sprintf("%s,2,%d", path, i)) // 2 means method in a service.
		g.P(g.generateServerSignature(servName, method))
	}
	g.P()
	// Server QueueSubscribe.

	g.P("type ", servName, "QueueSubscribe struct", "{")
	g.P("m *", microPkg, ".Micro")
	g.P("prefix string")
	g.P("}")
	g.P()

	g.P("func New", servName, "QueueSubscribe (prefix string, m *", microPkg, ".Micro) *", servName, "QueueSubscribe {")
	g.P("return &", servName, "QueueSubscribe{")
	g.P("m: m,")
	g.P("prefix: prefix,")
	g.P("}")
	g.P("}")
	g.P()

	for _, method := range service.Method {
		g.generateServerQueueSubscribeMethod(servName, method)
	}

	g.P("type ", servName, "Subscribe struct", "{")
	g.P("m *", microPkg, ".Micro")
	g.P("prefix string")
	g.P("}")
	g.P()

	g.P("func New", servName, "Subscribe (prefix string, m *", microPkg, ".Micro) *", servName, "Subscribe {")
	g.P("return &", servName, "Subscribe{")
	g.P("m: m,")
	g.P("prefix: prefix,")
	g.P("}")
	g.P("}")
	g.P()

	for _, method := range service.Method {
		g.generateServerSubscribeMethod(servName, method)
	}
}

// generateClientSignature returns the client-side signature for a method.
func (g *micro) generateClientRequestSignature(servName string, method *pb.MethodDescriptorProto) string {
	origMethName := method.GetName()
	methName := generator.CamelCase(origMethName)
	if reservedClientName[methName] {
		methName += "_"
	}
	reqArg := "req *" + g.typeName(method.GetInputType())
	if method.GetClientStreaming() {
		reqArg = ""
	}
	respName := "*" + g.typeName(method.GetOutputType())
	if method.GetServerStreaming() || method.GetClientStreaming() {
		respName = servName + "_" + generator.CamelCase(origMethName) + "Client"
	}

	return fmt.Sprintf("%s(%s) (%s, error)", methName+"Request", reqArg, respName)
}

func (g *micro) generateClientPublishSignature(servName string, method *pb.MethodDescriptorProto) string {
	origMethName := method.GetName()
	methName := generator.CamelCase(origMethName)
	if reservedClientName[methName] {
		methName += "_"
	}
	reqArg := "req *" + g.typeName(method.GetInputType())
	if method.GetClientStreaming() {
		reqArg = ""
	}
	// respName := "*" + g.typeName(method.GetOutputType())
	if method.GetServerStreaming() || method.GetClientStreaming() {
		// respName = servName + "_" + generator.CamelCase(origMethName) + "Client"
	}

	return fmt.Sprintf("%s(%s) (error)", methName+"Publish", reqArg)
}

func (g *micro) generateClientPublishMethod(reqServ, servName, serviceDescVar string, method *pb.MethodDescriptorProto, descExpr string) {
	// reqMethod := fmt.Sprintf("%s.%s", servName, method.GetName())
	methName := generator.CamelCase(method.GetName())
	// inType := g.typeName(method.GetInputType())
	// outType := g.typeName(method.GetOutputType())

	g.P("func (c *", unexport(servName), "Client) ", g.generateClientPublishSignature(servName, method), "{")
	if !method.GetServerStreaming() && !method.GetClientStreaming() {
		// TODO: Pass descExpr to Invoke.
		g.P("return ", `c.c.Publish(c.prefix+".`, strings.ToLower(methName), `", req)`)
		g.P("}")
		g.P()
		return
	}
}

func (g *micro) generateClientRequestMethod(reqServ, servName, serviceDescVar string, method *pb.MethodDescriptorProto, descExpr string) {
	// reqMethod := fmt.Sprintf("%s.%s", servName, method.GetName())
	methName := generator.CamelCase(method.GetName())
	// inType := g.typeName(method.GetInputType())
	outType := g.typeName(method.GetOutputType())

	g.P("func (c *", unexport(servName), "Client) ", g.generateClientRequestSignature(servName, method), "{")
	if !method.GetServerStreaming() && !method.GetClientStreaming() {
		g.P("res := new(", outType, ")")
		// TODO: Pass descExpr to Invoke.
		g.P("err := ", `c.c.Request(c.prefix+".`, strings.ToLower(methName), `", req, res, micro.DefaultTimeout)`)
		g.P("if err != nil { return nil, err }")
		g.P("return res, nil")
		g.P("}")
		g.P()
		return
	}
}

// generateServerSignature returns the server-side signature for a method.
func (g *micro) generateServerSignature(servName string, method *pb.MethodDescriptorProto) string {
	origMethName := method.GetName()
	methName := generator.CamelCase(origMethName)
	if reservedClientName[methName] {
		methName += "_"
	}

	var reqArgs []string
	ret := "error"

	reqArgs = append(reqArgs, "*micro.Context")

	if !method.GetClientStreaming() {
		reqArgs = append(reqArgs, "*"+g.typeName(method.GetInputType()))
	}
	if !method.GetClientStreaming() && !method.GetServerStreaming() {
		reqArgs = append(reqArgs, "*"+g.typeName(method.GetOutputType()))
	}
	return "type " + methName + "Handler func(" + strings.Join(reqArgs, ", ") + ") " + ret
}

func (g *micro) generateServerQueueSubscribeMethod(servName string, method *pb.MethodDescriptorProto) {
	methName := generator.CamelCase(method.GetName())
	inType := g.typeName(method.GetInputType())
	outType := g.typeName(method.GetOutputType())

	g.P("func (dq *", servName, "QueueSubscribe) ", methName, "(h ", methName, "Handler) {")
	if !method.GetServerStreaming() && !method.GetClientStreaming() {
		g.P(`subj := dq.prefix+".`, strings.ToLower(methName), `"`)
		g.P(`dq.m.QueueSubscribe(subj, subj, func(ctx *`, microPkg, `.Context) error {`)
		g.P(`req := new(`, inType, `)`)
		g.P(`if err := ctx.Decode(ctx.Data, req); err != nil {`)
		g.P(`return err`)
		g.P(`}`)
		g.P()
		g.P(`res := new(`, outType, `)`)
		g.P(`if err := h(ctx, req, res); err != nil {`)
		g.P(`return err`)
		g.P(`}`)
		g.P()
		g.P(`if ctx.Reply != "" {`)
		g.P(`ctx.Publish(ctx.Reply, res)`)
		g.P(`}`)
		g.P()
		g.P(`return nil`)
		g.P(`})`)
		g.P(`}`)
		return
	}
}

func (g *micro) generateServerSubscribeMethod(servName string, method *pb.MethodDescriptorProto) {
	methName := generator.CamelCase(method.GetName())
	inType := g.typeName(method.GetInputType())
	outType := g.typeName(method.GetOutputType())

	g.P("func (ds *", servName, "Subscribe) ", methName, "(h ", methName, "Handler) {")
	if !method.GetServerStreaming() && !method.GetClientStreaming() {
		g.P(`subj := ds.prefix+".`, strings.ToLower(methName), `"`)
		g.P(`ds.m.Subscribe(subj, func(ctx *`, microPkg, `.Context) error {`)
		g.P(`req := new(`, inType, `)`)
		g.P(`if err := ctx.Decode(ctx.Data, req); err != nil {`)
		g.P(`return err`)
		g.P(`}`)
		g.P()
		g.P(`res := new(`, outType, `)`)
		g.P(`if err := h(ctx, req, res); err != nil {`)
		g.P(`return err`)
		g.P(`}`)
		g.P()
		g.P(`if ctx.Reply != "" {`)
		g.P(`ctx.Publish(ctx.Reply, res)`)
		g.P(`}`)
		g.P()
		g.P(`return nil`)
		g.P(`})`)
		g.P(`}`)
		return
	}
}
