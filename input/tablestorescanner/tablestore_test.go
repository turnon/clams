package tablestorescanner

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestTablestoreInputut(t *testing.T) {
	conf, err := os.ReadFile("_config.yaml")
	if err != nil {
		t.Fatal(err)
	}

	tsConf, err := tablestoreConfigSpec.ParseYAML(string(conf), nil)
	if err != nil {
		t.Fatal(err)
	}

	tsInput, err := newTablestoreInput(tsConf)
	if err != nil {
		t.Fatal(err)
	}

	if err := tsInput.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	count := 0
	for {
		msg, ack, err := tsInput.Read(context.Background())
		if err != nil {
			if errors.Is(err, service.ErrEndOfInput) {
				break
			}
			t.Fatal(err)
		}

		structed, err := msg.AsStructured()
		if err != nil {
			t.Fatal(err)
		}
		t.Log(structed)
		count += 1

		if err := ack(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("msg count %d", count)

	if err := tsInput.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
