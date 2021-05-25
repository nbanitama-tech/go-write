package main

import (
	"fmt"
	"os"
	"log"
	"math/rand"
	"encoding/json"
	"time"
	"bufio"
	"compress/gzip"
)

var (
	LIMIT = 5000000
	BUFFER_SIZE = 1000000 // 1MB
	LETTER_RUNES = []rune("abcdefghijklmnopqrstuvwxyz")
)

type Record struct {
	ID int64 `json:"id"`
	Username string `json:"username"`
	AccountInfo AccountInfo `json:"account_info"`
	Links []int `json:"links"`
}

type AccountInfo struct {
	Name string `json:"name"`
	Address string `json:"address"`
}

func generateString(n int)string{
	res := make([]rune, n)
	for i := range res {
		res[i] = LETTER_RUNES[rand.Intn(len(LETTER_RUNES))]
	}

	return string(res)
}

func generateLinks()[]int{
	var res []int

	for i:=0 ; i<10 ; i++ {
		res = append(res, rand.Intn(LIMIT))
	}

	return res
}

func generateRecord()Record{
	var res Record

	id := rand.Intn(LIMIT)
	res.ID = int64(id)
	res.Username = generateString(20)
	res.AccountInfo = AccountInfo{
		Name: generateString(30),
		Address: generateString(20),
	}
	res.Links = generateLinks()

	return res
}

func main(){
	m := make(map[int]Record)
	prepareData(m)

	write1(m)
	// write(m)
	writeBuffer(m)
	writeBufferCompress(m)
	
    fmt.Println("Done!!!")
}

func prepareData(m map[int]Record) {
	startTime := time.Now()
	defer func(s time.Time){
		t := time.Since(s)
		fmt.Printf("Preparing %d data took %f minutes\n", len(m), t.Minutes())
	}(startTime)
	fmt.Println("Start preparing data....")
	
	for i:=0 ; i<LIMIT ; i++ {
		m[i] = generateRecord()
	}

	fmt.Println("Finished preparing data....")
}

func write(m map[int]Record){
	startTime := time.Now()
	defer func(s time.Time){
		t := time.Since(s)
		fmt.Printf("Writing files took %f minutes\n", t.Minutes())
	}(startTime)

	f, err := os.Create("data.json")

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

	counter := 1
	var writtenCounter int
	for _, value := range m {
		bytes, err := json.Marshal(value)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			continue
		}
		var n int
        n, err = f.WriteString(string(bytes) + "\n")
		writtenCounter = writtenCounter + n

        if err != nil {
            log.Fatal(err)
        }
		if counter%7000000 == 0 {
			fmt.Printf("Looping %d after %d bytes\n",counter, writtenCounter)
			writtenCounter = 0
		}

		counter++
    }
}

func write1(m map[int]Record){
	startTime := time.Now()
	defer func(s time.Time){
		t := time.Since(s)
		fmt.Printf("Writing #1 files took %f minutes\n", t.Minutes())
	}(startTime)

	f, err := os.Create("data1.json")

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

	counter := 1
	var writtenCounter int
	for _, value := range m {
		bytes, err := json.Marshal(value)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			continue
		}
		var n int
        n, err = fmt.Fprintln(f, string(bytes))
		writtenCounter = writtenCounter + n

        if err != nil {
            log.Fatal(err)
        }
		if counter%7000000 == 0 {
			fmt.Printf("Looping #1 %d after %d bytes\n",counter, writtenCounter)
			writtenCounter = 0
		}

		counter++
    }
}

func writeBuffer(m map[int]Record){
	startTime := time.Now()
	defer func(s time.Time){
		t := time.Since(s)
		fmt.Printf("Writing + buffer files took %f minutes\n", t.Minutes())
	}(startTime)

	f, err := os.Create("data2.json")

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

	writer := bufio.NewWriterSize(f, BUFFER_SIZE)
	defer writer.Flush()

	counter := 1
	var writtenCounter int
	for _, value := range m {
		bytes, err := json.Marshal(value)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			continue
		}
		var n int
        n, err = fmt.Fprintln(writer, string(bytes))
		writtenCounter = writtenCounter + n

        if err != nil {
            log.Fatal(err)
        }
		if counter%7000000 == 0 {
			fmt.Printf("Looping buffer %d after %d bytes\n",counter, writtenCounter)
			writtenCounter = 0
		}

		counter++
    }
}

func writeBufferCompress(m map[int]Record){
	startTime := time.Now()
	defer func(s time.Time){
		t := time.Since(s)
		fmt.Printf("Writing + buffer + compress files took %f minutes\n", t.Minutes())
	}(startTime)

	f, err := os.Create("data2.json.gz")

    if err != nil {
        log.Fatal(err)
    }

    defer f.Close()

	w := gzip.NewWriter(f)
	defer w.Close()

	writer := bufio.NewWriterSize(w, BUFFER_SIZE)
	defer writer.Flush()

	counter := 1
	var writtenCounter int
	for _, value := range m {
		bytes, err := json.Marshal(value)
		if err != nil {
			fmt.Printf("Err: %v\n", err)
			continue
		}
		var n int
        n, err = fmt.Fprintln(writer, string(bytes))
		writtenCounter = writtenCounter + n

        if err != nil {
            log.Fatal(err)
        }
		if counter%7000000 == 0 {
			fmt.Printf("Looping buffer compress %d after %d bytes\n",counter, writtenCounter)
			writtenCounter = 0
		}

		counter++
    }
}