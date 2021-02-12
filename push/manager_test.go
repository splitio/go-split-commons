package push

/*
import (
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/splitio/go-split-commons/v2/conf"
	"github.com/splitio/go-split-commons/v2/dtos"
	"github.com/splitio/go-split-commons/v2/push/mocks"
	"github.com/splitio/go-split-commons/v2/service/api"
	"github.com/splitio/go-toolkit/v3/logging"
)

func TestPushManagerCustom(t *testing.T) {
	syncMock := &mocks.LocalSyncMock{
		SynchronizeSplitsCall: func(x *int64) error {
			fmt.Printf("split sync con '%d'\n", *x)
			return nil
		},
		LocalKillCall: func(n string, df string, cn int64) {
			fmt.Printf("local kill con (%s:%s:%d)\n", n, df, cn)
		},
		SynchronizeSegmentCall: func(n string, cn *int64) error {
			fmt.Printf("segment sync con (%s:%d)\n", n, cn)
			return nil
		},
	}
	logger := logging.NewLogger(&logging.LoggerOptions{
		LogLevel:            logging.LevelDebug,
		StandardLoggerFlags: log.Llongfile,
	})
	feedback := make(chan int64, 100)
	cfg := conf.GetDefaultAdvancedConfig()
	cfg.SdkURL = "https://sdk.split-stage.io/"
	cfg.AuthServiceURL = "https://auth.split-stage.io"
	authAPI := api.NewAuthAPIClient("", cfg, logger, dtos.Metadata{})
	manager, err := NewManager(logger, syncMock, &cfg, feedback, authAPI)
	if err != nil {
		t.Error("no error expected. Got: ", err)
		return
	}

	go func() {
		for {
			fmt.Println("Goroutines: ", runtime.NumGoroutine())
			time.Sleep(5 * time.Second)
		}
	}()

	go func() {
		for {
			f := <-feedback
			fmt.Println("feedback: ", f)
			if f == StatusUp {
				manager.StartWorkers()
			}
		}
	}()

	manager.Start()

	for {
	}
}
*/
