package main

import (
	"encoding/json"
	"net/http"
	_ "os"
	"math/rand"
	"sync"
	"fmt"
	"context"
	"bytes"
	"time"
	"io/ioutil"
	"strconv"
)

type Address struct {
	City string `json:city`
	Country string `json:country`
}

type Person struct {
	Name string `json:name`
	Age int `json:age`
	Hobbies []string `json:hobbies`
	University string `json:university`
	PhoneNumber string `json:PhoneNumber`
	Addr Address `json:address`
}

var cityArray = []string {
	"nanjing", "shanghai", "beijing", "guiyang", "dalian", "duyun", "hangzhou", "suzhou", "gusu", "jiashan", "jiaxing", "kunshan",
	"zunyi", "xingyi", "hefei", "ningbo", "tianjing", "chuangzhou", "jinan", "xiamen", "qingdao", "chongqing", "haerbing", "taiyuan",
	"yantai", "wuxi", "foshan", "zhengzhou", "changchun", "shenyang", "nanchang", "tangshan", "zibo", "kunming", "nanning", "haikou",
	"sanya", "xuzhou", "weifang", "changzhou", "luoyang", "taizhou", "yangzhou", "shaoxing", "zhongshan", "xiangyang", "zhangzhou", "wuhu",
}

var hobbyArray  = []string {
	" basketball", " hill", " video-game", " football", " reading",
	" writing", " program", " singing", " dancing", " eating",
	" guita"," drawing", " card", " gamble", " drinking", " smoke",
}

var universityArray = []string {
	"tongji", "zheda", "shangjiao", "beida", "qinghua", "renda", "chongda",
	"zhongshan", "wuda", "huake", "nanda", "xijiaoda", "hagongda", "jida",
	"xiada", "guida", "shiyuan", "jixiao", "tianjingdaxue", "caida", "shifan",
	"guofangkejida", "zhongkeda", "haikoudaxue", "jiangnandaxue", "niujing",
}

var countryArray = []string {
	"China", "USA", "Russia", "Japan",
	"France", "Italy", "German", "Korea",
	"Astra", "Spanish",
}

var cityLen int
var hobbyLen int
var universityLen int
var numberOfMock int
var phoneNumberLen int
var countryLen int
var clientCh chan *http.Client
var msgCh chan []byte
var elasticSearchAddress string

func genRandomName(wg *sync.WaitGroup) <-chan string {
	wg.Add(1)
	ch := make(chan string, 10)
	go func() {
		defer wg.Done()
		for j := 0; j < numberOfMock; j++ {
			curString := ""
			curLen := rand.Int() % 6 + 3
			for i := 0; i < curLen; i++ {
				curString += string(rand.Int() % 26 + 97)
			}
			ch <-curString
		}
	}()
	return ch
}

func genRandomAge(wg *sync.WaitGroup) <-chan int {
	wg.Add(1)
	ch := make(chan int, 10)
	go func() {
		defer wg.Done()
		for j := 0; j < numberOfMock; j++ {
			ch <-(rand.Int() % 50 + 20)
		}
	}()
	return ch
}

func genRandomhobbies(wg *sync.WaitGroup) <-chan []string {
	wg.Add(1)
	ch := make(chan []string, 10)
	go func() {
		defer wg.Done()
		for i := 0; i < numberOfMock; i++ {
			var curArr []string
			curLen := rand.Int() % 3 + 1
			for j := 0; j < curLen; j++ {
				curArr = append(curArr, hobbyArray[rand.Int() % hobbyLen])
			}
			ch <-curArr
		}
	}()
	return ch
}

func genRandomUniversity(wg *sync.WaitGroup) <-chan string {
	wg.Add(1)
	ch := make(chan string, 10)
	go func() {
		defer wg.Done()
		for i := 0; i < numberOfMock; i++ {
			ch <-universityArray[rand.Int() % universityLen]
		}
	}()
	return ch
}

func genRandomPhoneNumber(wg *sync.WaitGroup) <-chan string {
	wg.Add(1)
	ch := make(chan string, 10)
	go func() {
		defer wg.Done()
		for i := 0; i < numberOfMock; i++ {
			curString := "1"
			for j := 1; j < phoneNumberLen; j++ {
				curString += string(rand.Int() % 10 + 48)
			}
			ch <-curString
		}
	}()
	return ch
}

func genRandomCity(wg *sync.WaitGroup) <-chan string {
	wg.Add(1)
	ch := make(chan string, 10)
	go func() {
		defer wg.Done()
		for i := 0; i < numberOfMock; i++ {
			ch <- cityArray[rand.Int() % cityLen]
		}
	}()
	return ch
}

func genRandomCountry(wg *sync.WaitGroup) <-chan string {
	wg.Add(1)
	ch := make(chan string, 10)
	go func() {
		defer wg.Done()
		for i := 0; i < numberOfMock; i++ {
			ch <- countryArray[rand.Int() % countryLen]
		}
	}()
	return ch
}

func pushMessageToElasticSearch(ctx context.Context) {
	for {
		msgBytes := <-msgCh
		select {
		case <-ctx.Done():
			return
		case curClient := <-clientCh:
			res, err := curClient.Post(elasticSearchAddress, "application/json", bytes.NewBuffer(msgBytes))
			if err != nil {
				fmt.Printf("Post Error is : %v.\n", err)
				continue
			}
			res.Body.Close()
			clientCh <-curClient
		}
	}
}

func initHttpClient() {
	for i := 0; i < 10; i++ {
		cClient := &http.Client{
			Timeout : time.Second * 5,
		}
		clientCh<-cClient
	}
}

func init() {
	cityLen = len(cityArray)
	hobbyLen = len(hobbyArray)
	universityLen = len(universityArray)
	countryLen = len(countryArray)
	numberOfMock = 10000000
	phoneNumberLen = 11
	clientCh = make(chan *http.Client, 10)
	msgCh = make(chan []byte, 10)
	elasticSearchAddress = "http://192.168.26.83:9200/people/info/"
	initHttpClient()
}

func main() {
	wg := new(sync.WaitGroup)
	nameCh := genRandomName(wg)
	ageCh := genRandomAge(wg)
	hobbyCh := genRandomhobbies(wg)
	universityCh := genRandomUniversity(wg)
	phoneCh := genRandomPhoneNumber(wg)
	cityCh := genRandomCity(wg)
	countryCh := genRandomCountry(wg)
	fmt.Printf("Start Loop.\n")
	//ctx, cancel := context.WithCancel(context.Background())
	//for i := 0; i < 10; i++ {
		//go pushMessageToElasticSearch(ctx)
	//}
	client := &http.Client {
		Timeout : time.Second * 5,
	}
	for i := 0; i < numberOfMock; i++ {
		person := Person{}
		person.Name = <-nameCh
		person.Age = <-ageCh
		person.Hobbies = <-hobbyCh
		person.University = <-universityCh
		person.PhoneNumber = <-phoneCh
		person.Addr.City = <-cityCh
		person.Addr.Country = <-countryCh
		curBytes, err := json.Marshal(person)
		if err != nil {
			fmt.Printf("Marshal Error is : %v.\n", err)
		}
		//msgCh <-curBytes
		//res, err := client.Post(elasticSearchAddress, "application/json", bytes.NewBuffer(curBytes))
		curReq, _ := http.NewRequest("PUT", elasticSearchAddress + strconv.Itoa(i), bytes.NewBuffer(curBytes))
		curReq.Header["Content-Type"] = []string{"application/json"}
		res, err := client.Do(curReq)
		if err != nil {
			fmt.Printf("Post Error : %v.\n", err)
		} else {
			resBytes, _ := ioutil.ReadAll(res.Body)
			fmt.Printf("The res is : %v.\n", string(resBytes))
		}
		res.Body.Close()
	}
	//cancel()
	wg.Wait()
}



