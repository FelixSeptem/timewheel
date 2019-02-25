package main

import (
	"fmt"
	"github.com/FelixSeptem/timewheel"
	"time"
)

func main() {
	tw := timewheel.NewTimeWheel("demo", 10, time.Second, 100)
	go func() {
		handleErr(tw)
	}()
	fun1s := func() error {
		fmt.Printf("running after 1 second with %s\n", time.Now().Format("2006-01-02 15:04:05.999"))
		return nil
	}
	fun7s := func() error {
		fmt.Printf("running after 7 second with %s\n", time.Now().Format("2006-01-02 15:04:05.999"))
		return nil
	}
	fun10s := func() error {
		fmt.Printf("running after 10 second with %s\n", time.Now().Format("2006-01-02 15:04:05.999"))
		return nil
	}
	fun15s := func() error {
		fmt.Printf("running after 15 second with %s\n", time.Now().Format("2006-01-02 15:04:05.999"))
		return nil
	}
	if err := tw.Run(); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second, fun1s); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second*7, fun7s); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second*17, fun7s); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second*10, fun10s); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second*20, fun10s); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second*30, fun10s); err != nil {
		fmt.Println(err.Error())
	}
	if _, err := tw.AddTask(time.Second*15, fun15s); err != nil {
		fmt.Println(err.Error())
	}
	if err := tw.BQuit(); err != nil {
		fmt.Println(err.Error())
	}
	time.Sleep(time.Second * 60)
}

func handleErr(tw *timewheel.TimeWheel) {
	errs := tw.HandleErr()
	for {
		fmt.Println(<-errs)
	}
}
