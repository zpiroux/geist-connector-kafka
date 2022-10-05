package notify

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zpiroux/geist/entity"
)

func TestNotify(t *testing.T) {

	sender := "someSender"
	instance := "someId"
	stream := "someStreamId"
	expectedMessage := "some stuff happened, foo=11"
	fmtstr := "some stuff happened, foo=%d"
	fmtval := 11

	ch := make(entity.NotifyChan, 3)
	notifier := New(ch, nil, 2, sender, instance, stream)

	// Test INFO
	notifier.Notify(entity.NotifyLevelInfo, fmtstr, fmtval)
	event := <-ch
	expectedEvent := entity.NotificationEvent{
		Level:    "INFO",
		Sender:   sender,
		Instance: instance,
		Stream:   stream,
		Message:  expectedMessage,
		Func:     "notify.TestNotify",
	}
	event.Timestamp = ""
	assert.Equal(t, expectedEvent, event)

	// Test WARN
	notifier.Notify(entity.NotifyLevelWarn, fmtstr, fmtval)
	event = <-ch
	expectedEvent.Level = "WARN"
	expectedEvent.File = "notify_test.go"
	expectedEvent.Line = 38
	event.Timestamp = ""
	event.File = filepath.Base(expectedEvent.File)
	assert.Equal(t, expectedEvent, event)

	// Test ERROR
	notifier.Notify(entity.NotifyLevelError, fmtstr, fmtval)
	event = <-ch
	expectedEvent.Level = "ERROR"
	expectedEvent.Line = 48
	event.Timestamp = ""
	event.File = filepath.Base(expectedEvent.File)
	assert.NotEmpty(t, event.StackTrace)
	event.StackTrace = ""
	assert.Equal(t, expectedEvent, event)

}
