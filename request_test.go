package drpc

import (
	"testing"
)

func TestMethodToDRPC(t *testing.T) {
	testData := []struct {
		service string
		method  string
		expect  string
	}{
		{
			"helloworld",
			"Greeter.SayHello",
			"/Helloworld.Greeter/SayHello",
		},
		{
			"helloworld",
			"/Helloworld.Greeter/SayHello",
			"/Helloworld.Greeter/SayHello",
		},
		{
			"",
			"/Helloworld.Greeter/SayHello",
			"/Helloworld.Greeter/SayHello",
		},
		{
			"",
			"Greeter.SayHello",
			"/Greeter/SayHello",
		},
	}

	for _, d := range testData {
		method := methodToDRPC(d.service, d.method)
		if method != d.expect {
			t.Fatalf("expected %s got %s", d.expect, method)
		}
	}
}
