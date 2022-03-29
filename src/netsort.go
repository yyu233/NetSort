package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"

	//	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

const KEYSIZE = 10
const VALUESIZE = 90
const RECORDSIZE = 100

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type Client struct {
	Conn   net.Conn
	Record []byte
	AckFIN bool
}

func readServerConfigs(configPath string) ServerConfigs {
	fmt.Println("Enter readServerConfigs")
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}
	fmt.Println("Printing config file")
	fmt.Println(string(f))
	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func buildMap(filename string, numServer int) map[int][][]byte {
	log.Println("Enter buildMap")
	fmt.Println("Enter buildMap")
	// open file
	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// declare map
	serverRecordMap := make(map[int][][]byte)

	// read file
	reader := bufio.NewReader(f)
	reader = bufio.NewReaderSize(reader, 5000)
	//line := 0
	recordBuf := make([]byte, 0)
	for {
		buf := make([]byte, RECORDSIZE)
		//Need to be careful about the return bytes
		bytes, err := reader.Read(buf[:RECORDSIZE])
		recordBuf = append(recordBuf, buf[:bytes]...)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
	}
	//need to investigate: does RECORDSIZE include \n
	m := len(recordBuf) / RECORDSIZE
	var buf []byte
	for i := 0; i < m; i++ {
		buf, recordBuf = recordBuf[:RECORDSIZE], recordBuf[RECORDSIZE:]
		key := buf[0] // assume using less than 256 servers
		mask := (int)(8 - math.Log2((float64)(numServer)))
		//fmt.Printf("key: %b\n", key)
		serverID := (int)(key >> mask)
		//fmt.Printf("serverID: %d\n", serverID)
		value, ok := serverRecordMap[serverID]
		if ok {
			value = append(value, buf)
			serverRecordMap[serverID] = value
		} else {
			serverRecordMap[serverID] = append(make([][]byte, 0), buf)
		}
	}

	return serverRecordMap
}

func sendRecord(address string, port string, record [][]byte) {
	log.Println("Enter sendRecord")
	fmt.Println("Enter sendRecord")
	if strings.Compare("localhost", address) == 0 {
		address = "127.0.0.1"
	}
	dest := address + ":" + port

	var conn net.Conn
	try := 0
	for { //retry
		if try == 10 { // try 10 times
			fmt.Println(errors.New("Connection refused time out: " + address))
			log.Fatal(errors.New("Connection refused time out: " + address))
		}
		var err error
		conn, err = net.Dial("tcp", dest)
		if err == nil {
			break
		}
		if _, ok := err.(net.Error); ok {
			log.Println(strconv.FormatBool(strings.Contains(err.Error(), "connection refused")))
			log.Println(dest)
			fmt.Println(err)
			/*if nerr.Temporary() || strings.Contains(err.Error(), "connection refused") {
				time.Sleep(1 * time.Second)
				try++
				continue
			}*/
			fmt.Println(dest)
			time.Sleep(1 * time.Second)
			try++
		} else { // non network error
			fmt.Println(err)
			log.Fatal(err)
		}
	}
	defer conn.Close()

	log.Printf("Start sending records to %s\n", dest)
	for i := range record {
		if len(record[i]) == RECORDSIZE {
			n, err := conn.Write(record[i])
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Number of byte sent:  %d\n", n)
		} else {
			fmt.Println(errors.New("error: record size is wrong"))
			log.Fatal(errors.New("error: record size is wrong"))
		}
	}
	log.Printf("Number of Record: %d\n", len(record))
	log.Printf("Done sending records to %s\n", dest)
	fin := make([]byte, 100, 100)
	conn.Write(fin)
}

func sendFIN(address string, port string) {
	log.Println("Enter sendFIN")
	fmt.Println("Enter sendFIN")
	if strings.Compare("localhost", address) == 0 {
		address = "127.0.0.1"
	}
	dest := address + ":" + port
	var conn net.Conn
	line := 0
	for {
		if line == 10 {
			fmt.Println(errors.New("Connection refused time out: " + address))
			log.Fatal(errors.New("Connection refused time out: " + address))
		}
		var err error
		conn, err = net.Dial("tcp", dest)
		if err == nil {
			break
		}
		if _, ok := err.(net.Error); ok {
			log.Println(strconv.FormatBool(strings.Contains(err.Error(), "connection refused")))
			/*if nerr.Temporary() || strings.Contains(err.Error(), "connection refused") {
				time.Sleep(1 * time.Second)
				line++
				continue
			}*/
			fmt.Println(err)
			fmt.Println(dest)
			time.Sleep(1 * time.Second)
			line++
		} else {
			fmt.Println(err)
			log.Fatal(err)
		}
	}

	defer conn.Close()
	//TODO maybe add timeout handler?
	log.Printf("Start sending FIN to %s\n", dest)
	fin := make([]byte, 100, 100)
	conn.Write(fin)
	log.Printf("Done sending FIN to %s\n", dest)
}

//client code
func broadcast(serverRecordMap map[int][][]byte, myId int, scs ServerConfigs, ch chan<- bool) {
	fmt.Println("Start Broadcasting")
	log.Printf("Start Broadcasting")
	servers := scs.Servers
	for i := range servers {
		serverID := servers[i].ServerId
		// server is not localhost
		if serverID != myId {
			address, port, err := getServAddrAndPort(scs, serverID)
			if err != nil {
				log.Fatal(err)
			}
			// client has server record; send record
			if value, ok := serverRecordMap[serverID]; ok {
				//go sendRecord(address, port, value)
				sendRecord(address, port, value)
			} else {
				//go sendFIN(address, port)
				sendFIN(address, port)
			}
		} else {
			log.Printf("serverID: %d, myID: %d, don't need to send", serverID, myId)
		}
	}
	log.Printf("End Broadcasting")

	ch <- true
}

func getServAddrAndPort(scs ServerConfigs, serverID int) (string, string, error) {
	log.Println("Enter getServAddrAndPort")
	fmt.Println("Enter getServAddrAndPort")
	servers := scs.Servers

	for i := range servers {
		if servers[i].ServerId == serverID {
			return servers[i].Host, servers[i].Port, nil
		}
	}

	return "", "", errors.New("can't find serverID")
}

func slice1DTo2D(slice1D []byte) [][]byte {
	m := len(slice1D) / RECORDSIZE
	slice2D := make([][]byte, m)

	for i := range slice2D {
		slice2D[i], slice1D = slice1D[:RECORDSIZE], slice1D[RECORDSIZE:]
	}

	return slice2D
}

func slice2DAppend1D(slice2D [][]byte, slice1D []byte) [][]byte {
	m := len(slice1D) / RECORDSIZE

	for i := 0; i < m; i++ {
		slice2D, slice1D = append(slice2D, slice1D[:RECORDSIZE]), slice1D[RECORDSIZE:]
	}

	return slice2D
}

//server code
func handleConnection(conn net.Conn, ch chan<- Client) {
	// TODO: need to filter out client not in the cluser?
	log.Println("Enter handleConnection")
	fmt.Println("Enter handleConnection")
	var record []byte
	ackFIN := false
	for {
		buffer := make([]byte, RECORDSIZE)
		bytes, err := conn.Read(buffer)
		record = append(record, buffer[:bytes]...)
		if err != nil {
			if err != io.EOF {
				// should close connection?
				fmt.Println(err)
				log.Println(err)
			}
			break
		}
	}
	finRecord := make([]byte, 100, 100)
	if len(record) >= 100 {
		lastRecord := record[len(record)-100:]

		if bytes.Compare(lastRecord, finRecord) == 0 {
			ackFIN = true
		} else {
			fmt.Println(errors.New("didn't receive FIN"))
			log.Printf("Remote addr: %s\n", conn.RemoteAddr().(*net.TCPAddr))
			log.Printf("Number of bytes sent: %d", len(record))
			log.Fatal(errors.New("didn't receive FIN"))
		}
	} else {
		fmt.Println(errors.New("didn't receive FIN"))
		log.Printf("Remote addr: %s\n", conn.RemoteAddr().(*net.TCPAddr))
		log.Printf("Number of bytes sent: %d", len(record))
		log.Fatal(errors.New("didn't receive FIN"))
	}

	newRecord := Client{conn, record[:len(record)-100], ackFIN}
	ch <- newRecord
}

//server code
func listenforData(ch chan<- Client, serverID string, port string) {
	log.Println("Enter listenforData")
	fmt.Println("Enter listenforData")
	address := ":" + port
	/**out, err := exec.Command("hostname", "-i").Output()
	if err != nil {
		fmt.Println(err)
	}
	address := strings.TrimSuffix(string(out), "\n") + ":" + port**/
	l, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Got error on line 308")
		fmt.Printf("serverID passed in: %s  port passed in: %s\n   address concated: %s\n", serverID, port, address)
		fmt.Println(err)
		log.Fatal(err)
	}
	defer l.Close()
	hostname, _ := os.Hostname()
	for {
		log.Printf("Localhost address: %s, %s\n", address, hostname)
		log.Printf("Waiting for Client\n")
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		go handleConnection(conn, ch)
	}
}

//server code
func consumeChannel(ch <-chan Client, numClients int) ([][]byte, map[net.Addr]bool, map[net.Conn][][]byte) {
	log.Println("Enter consumeChannel")
	fmt.Println("Enter consumeChannel")
	dataToSort := make([][]byte, 0)
	mapFIN := make(map[net.Addr]bool)
	mapConn := make(map[net.Conn][][]byte)
	numClientsCompleted := 0

	for {
		fmt.Printf("NumClientsCompleted: %d\n", numClientsCompleted)
		log.Printf("NumClientsCompleted: %d\n", numClientsCompleted)
		if numClientsCompleted == numClients {
			break
		}
		client := <-ch
		record := client.Record
		record2D := slice1DTo2D(record)
		mapConn[client.Conn] = record2D
		clientAddr := client.Conn.RemoteAddr()
		ackFIN := client.AckFIN
		mapFIN[clientAddr] = ackFIN
		if ackFIN {
			dataToSort = append(dataToSort, record2D...)
			numClientsCompleted++
		}
	}

	return dataToSort, mapFIN, mapConn
}

func getLocalListenPort(scs ServerConfigs, serverID int) (string, error) {
	log.Println("Enter getLocalListenPort")
	fmt.Println("Enter getLocalListenPort")
	servers := scs.Servers
	for i := range servers {
		if servers[i].ServerId == serverID {
			fmt.Printf("local listen port got: %s\n", servers[i].Port)
			return servers[i].Port, nil
		}
	}
	return "", errors.New("can't find port")
}

func partition(arr [][]byte, low, high int) ([][]byte, int) {
	piv := arr[high][:KEYSIZE]
	i := low
	for j := low; j < high; j++ {
		if bytes.Compare(arr[j][:KEYSIZE], piv) < 0 {
			arr[i], arr[j] = arr[j], arr[i]
			i++
		}
	}
	arr[i], arr[high] = arr[high], arr[i]

	return arr, i
}

func quickSort(arr [][]byte, low, high int) [][]byte {
	if low < high {
		arr, p := partition(arr, low, high)
		arr = quickSort(arr, low, p-1)
		arr = quickSort(arr, p+1, high)
	}

	return arr
}

// test
func testBuildMap(serverRecordMap map[int][][]byte) {
	for key, val := range serverRecordMap {
		fmt.Printf("key: %d\n", key)
		for i := range val {
			fmt.Printf("val: %s\n", strings.ToUpper(hex.EncodeToString(val[i])))
		}
	}
}

func printMapFIN(mp map[net.Addr]bool) {
	fmt.Println("Starting printing mapFIN")
	for key, value := range mp {
		if addr, ok := key.(*net.TCPAddr); ok {
			fmt.Printf("key: %s value: %s\n", addr, strconv.FormatBool(value))
		}
	}
}

func printMapConn(mp map[net.Conn][][]byte) {
	fmt.Println("Starting printing mapConn")
	for _, value := range mp {
		for i := range value {
			fmt.Printf("val: %s\n", strings.ToUpper(hex.EncodeToString(value[i])))
		}
	}
}

func testLocalTestRecordToSortAndLocalRecord(recordToSort [][]byte, localRecord [][]byte) {
	if len(recordToSort) != len(localRecord) {
		log.Println("Error")
		return
	}
	s1 := quickSort(recordToSort, 0, len(recordToSort)-1)
	s2 := quickSort(localRecord, 0, len(localRecord)-1)

	for i := 0; i < len(s1); i++ {
		if bytes.Compare(s1[i], s2[i]) != 0 {
			log.Println("Error")
			return
		}
	}

	log.Println("Test Passed : record to sort and local record are equal")
}

func testLocalHost(serverRecordMap map[int][][]byte, serverId int, scs ServerConfigs) {
	go sendRecord("localhost", "8080", serverRecordMap[serverId])
	//receive record
	ch := make(chan Client)

	serverPort, err := getLocalListenPort(scs, serverId)
	if err != nil {
		log.Fatal(err)
	}
	go listenforData(ch, strconv.Itoa(serverId), serverPort)

	numClients := 1
	recordToSort, mapFIN, mapConn := consumeChannel(ch, numClients)

	printMapFIN(mapFIN)
	printMapConn(mapConn)
	// append local record
	localRecord := serverRecordMap[serverId]

	testLocalTestRecordToSortAndLocalRecord(recordToSort, localRecord)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/
	// get number of servers
	numServer := len(scs.Servers)
	fmt.Printf("number of server: %d\n", numServer)

	// build server record map
	serverRecordMap := buildMap(os.Args[2], numServer)

	//testBuildMap(serverRecordMap)

	//TODO corner case 1 server, server is localhost; server is not localhost
	sortedRecord := make([][]byte, 0)
	if numServer == 1 && serverId == scs.Servers[0].ServerId {
		recordToSort := serverRecordMap[serverId]
		sortedRecord = quickSort(recordToSort, 0, len(recordToSort))
	} else {
		ch := make(chan Client)

		serverPort, err := getLocalListenPort(scs, serverId)
		if err != nil {
			log.Fatal(err)
		}
		//go listenforData(ch, "server"+strconv.Itoa(serverId), serverPort)
		addr := ""
		for i := range scs.Servers {
			if scs.Servers[i].ServerId == serverId {
				addr = scs.Servers[i].Host
				break
			}
		}
		go listenforData(ch, addr, serverPort)
		//broadcast record
		brdCh := make(chan bool)
		time.Sleep(8 * time.Second)
		go broadcast(serverRecordMap, serverId, scs, brdCh)

		numClients := numServer - 1
		recordToSort, _, _ := consumeChannel(ch, numClients)

		//printMapFIN(mapFIN)
		//printMapConn(mapConn)
		// append local record
		localRecord := serverRecordMap[serverId]

		//testLocalTestRecordToSortAndLocalRecord(recordToSort, localRecord)
		recordToSort = append(recordToSort, localRecord...)

		//sort
		sortedRecord = quickSort(recordToSort, 0, len(recordToSort)-1)

		isbrdDone := <-brdCh
		if !isbrdDone {
			log.Fatal("broadcast error")
		}
	}
	//write to output file
	// create outputfile
	log.Println("Start writing sorted data to output file")
	of, err := os.Create(os.Args[3])
	if err != nil {
		log.Fatal(err)
	}
	defer of.Close()

	//write file
	writer := bufio.NewWriter(of)
	defer writer.Flush()

	line := len(sortedRecord)
	for i := 0; i <= line-1; i++ {
		writer.Write(sortedRecord[i][:RECORDSIZE])
	}
}
