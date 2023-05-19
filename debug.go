package geerpc

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
)

const debugText = `<html>
	<body>
	<title>GeeRPC Services</title>
	{{range .}}
	<hr>
	Service {{.Name}}
	<hr>
		<table>
		<th align=center>Method</th><th align=center>Calls</th>
		{{range $name, $mtype := .Method}}
			<tr>
			<td align=left font=fixed>{{$name}}({{$mtype.ArgType}}, {{$mtype.ReplyType}}) error</td>
			<td align=center>{{$mtype.NumCalls}}</td>
			</tr>
		{{end}}
		</table>
	{{end}}
	</body>
	</html>`

var debug = template.Must(template.New("RPC debug").Parse(debugText))

type debugHTTP struct {
	s *Server
}

type debugService struct {
	Name   string
	Method map[string]*methodType
}

func (s *debugHTTP) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Println("debug")
	var services []debugService
	s.s.serviceMap.Range(func(key, value interface{}) bool {
		svc := value.(*service)
		services = append(services, debugService{
			Name:   key.(string),
			Method: svc.method,
		})
		return true
	})
	err := debug.Execute(w, services)
	if err != nil {
		_, _ = fmt.Fprintln(w, "rpc: error executing template:", err.Error())
	}
}
