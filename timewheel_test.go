package timewheel

import (
	"math/rand"
	"testing"
	"time"
)

func TestTimeWheel_AddTask(t *testing.T) {
	tw := NewTimeWheel("test", 3600, time.Second, 100)
	taskHandler := func() error {
		return nil
	}
	if id, err := tw.AddTask(time.Second*100, taskHandler); err != nil || len(id) == 0 {
		t.Errorf("%v with %s", err, id)
	}
}

func TestTimeWheel_Info(t *testing.T) {
	tw := NewTimeWheel("test", 3600, time.Second, 100)
	name, startTime, capacity := tw.Info()
	taskHandler := func() error {
		return nil
	}
	if name != "test" || capacity != 0 {
		t.Errorf("return %s %v %d", name, startTime, capacity)
	}
	if id, err := tw.AddTask(time.Second*100, taskHandler); err != nil || len(id) == 0 {
		t.Errorf("%v with %s", err, id)
	}
	name, startTime, capacity = tw.Info()
	if name != "test" || capacity != 1 {
		t.Errorf("return %s %v %d", name, startTime, capacity)
	}
}

func TestTimeWheel_Run(t *testing.T) {
	tw := NewTimeWheel("test", 3600, time.Second, 100)
	if err := tw.Run(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkTimeWheel_AddTask(b *testing.B) {
	tw := NewTimeWheel("test", 3600, time.Second, 100)
	if err := tw.Run(); err != nil {
		b.Fatal(err)
	}
	handler := func() error {
		return nil
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tw.AddTask(time.Second*time.Duration(rand.Uint64() + 1), handler); err != nil {
			b.Error(err)
		}
	}
}
