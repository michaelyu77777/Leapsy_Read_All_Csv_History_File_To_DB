package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"time"

	//"labix.org/v2/mgo"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"golang.org/x/text/encoding/traditionalchinese" // 繁體中文編碼
	"golang.org/x/text/transform"
)

//設定檔
var config Config = Config{}
var worker = runtime.NumCPU()

// 指定編碼:將繁體Big5轉成UTF-8才會正確
var big5ToUTF8Decoder = traditionalchinese.Big5.NewDecoder()

//Log檔
var log_info *logrus.Logger
var log_err *logrus.Logger

//檔案的開始與結束日期(轉Time格式)
var dateStart time.Time
var dateEnd time.Time

// 設定檔
type Config struct {
	MongodbServerIP string //IP
	DBName          string
	Collection      string
	StartDate       string // 讀檔開始日
	EndDate         string // 讀檔結束日
	FolderPath      string // 目錄資料夾路徑
}

// 日打卡紀錄(.csv檔 有工號的)
type DailyRecord struct {
	Date       string    `date`
	Name       string    `name`
	CardID     string    `cardID`
	Time       string    `time`
	Message    string    `msg`
	EmployeeID string    `employeeID`
	DateTime   time.Time `dateTime`
}

/** 初始化配置 */
func init() {

	fmt.Println("執行init()初始化")

	/**設定LOG檔層級與輸出格式*/
	//使用Info層級
	path := "./log/info/info"
	writer, _ := rotatelogs.New(
		path+".%Y%m%d%H",                            // 檔名格式
		rotatelogs.WithLinkName(path),               // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(10080*time.Minute),    // 文件最大保存時間(保留七天)
		rotatelogs.WithRotationTime(60*time.Minute), // 日誌切割時間間隔(一小時存一個檔案)
	)

	// 設定LOG等級
	pathMap := lfshook.WriterMap{
		logrus.InfoLevel: writer,
		//logrus.PanicLevel: writer, //若執行發生錯誤則會停止不進行下去
	}

	log_info = logrus.New()
	log_info.Hooks.Add(lfshook.NewHook(pathMap, &logrus.JSONFormatter{})) //Log檔綁訂相關設定

	fmt.Println("結束Info等級設定")

	//Error層級
	path = "./log/err/err"
	writer, _ = rotatelogs.New(
		path+".%Y%m%d%H",                            // 檔名格式
		rotatelogs.WithLinkName(path),               // 生成软链，指向最新日志文件
		rotatelogs.WithMaxAge(10080*time.Minute),    // 文件最大保存時間(保留七天)
		rotatelogs.WithRotationTime(60*time.Minute), // 日誌切割時間間隔(一小時存一個檔案)
	)

	// 設定LOG等級
	pathMap = lfshook.WriterMap{
		//logrus.InfoLevel: writer,
		logrus.ErrorLevel: writer,
		//logrus.PanicLevel: writer, //若執行發生錯誤則會停止不進行下去
	}

	log_err = logrus.New()
	log_err.Hooks.Add(lfshook.NewHook(pathMap, &logrus.JSONFormatter{})) //Log檔綁訂相關設定

	fmt.Println("結束Error等級設定")
	log_info.Info("結束Error等級設定")

	/*json設定檔*/
	file, _ := os.Open("config.json")
	buf := make([]byte, 2048)

	//將設定讀到config變數中
	n, _ := file.Read(buf)
	fmt.Println(string(buf))
	err := json.Unmarshal(buf[:n], &config)
	if err != nil {
		panic(err)

		log_err.WithFields(logrus.Fields{
			"trace": "trace-0001",
			"err":   err,
		}).Error("將設定讀到config變數中失敗")

		fmt.Println(err)
	}
}

func main() {

	/**轉換逗號+有員工編號+csv檔*/
	//計算開始結束日期
	countDateStartAndEnd_CsvFile()

	//先清空所有指定日期的所有資料
	deleteAllRecordFromStartToEnd()

	runtime.GOMAXPROCS(runtime.NumCPU())
	//開始抓資料
	ImportDailyRecord_CsvFile()

}

func deleteAllRecordFromStartToEnd() {

	// 會包含dateEnd最後一天
	for myTime := dateStart; myTime != dateEnd.AddDate(0, 0, 1); myTime = myTime.AddDate(0, 0, 1) {
		deleteDailyRecordToday(myTime.Format("20060102"))
	}

}

/**
 * 刪除當日所有舊紀錄
 */
func deleteDailyRecordToday(date string) {

	log_info.Info("連接MongoDB")
	session, err := mgo.Dial(config.MongodbServerIP)
	//session, err := mgo.Dial("127.0.0.1")
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"err": err,
		}).Error("連接MongoDB發生錯誤(要刪除日打卡記錄時)")

		panic(err)
	}

	defer session.Close()
	c := session.DB(config.DBName).C(config.Collection)

	log_info.WithFields(logrus.Fields{
		"MongodbServer":  config.MongodbServerIP,
		"DBName":         config.DBName,
		"CollectionName": config.Collection, //date
	}).Info("設定檔:")

	log_info.Info("移除當日所有舊紀錄,日期為 date: ", date)
	info, err := c.RemoveAll(bson.M{"date": date}) //移除今天所有舊的紀錄(格式年月日)
	if err != nil {
		log_err.WithFields(logrus.Fields{
			"err":  err,
			"date": date,
		}).Error("移除當日所有舊紀錄失敗")

		os.Exit(1)
	}

	log_info.Info("發生改變的info: ", info)

}

/*導入每日打卡資料*/
func ImportDailyRecord_CsvFile() {

	// 建立 channel 存放 DailyRecord型態資料
	chanDailyRecord := make(chan DailyRecord)

	// 標記完成
	dones := make(chan struct{}, worker)

	// 將日打卡紀錄檔案內容讀出，並加到 chanDailyRecord 裡面
	go addDailyRecordForManyDays_CsvFile(chanDailyRecord)

	// 將chanDailyRecord 插入mongodb資料庫
	for i := 0; i < worker; i++ {
		go insertDailyRecord_CsvFile(chanDailyRecord, dones)
	}
	//等待完成
	awaitForCloseResult(dones)
	fmt.Println("日打卡紀錄插入完畢")
}

/*
 * 讀取有逗號+員工編號的打卡資料(多日)
 * 讀取的檔案().csv 或 .txt檔案)，編碼要為UTF-8，繁體中文才能正確被讀取
 */
func addDailyRecordForManyDays_CsvFile(chanDailyRecord chan<- DailyRecord) {

	// for myTime := time.Date(2020, 1, 1, 9, 0, 0, 0, time.Local); myTime != time.Date(2021, 1, 1, 9, 0, 0, 0, time.Local); myTime = myTime.AddDate(0, 0, 1) {
	// }

	// 會包含dateEnd最後一天
	for myTime := dateStart; myTime != dateEnd.AddDate(0, 0, 1); myTime = myTime.AddDate(0, 0, 1) {

		// 檔案名稱(年月日)
		fileName := "Rec" + myTime.Format("20060102") + ".csv"
		log_info.Info("讀檔: ", fileName)
		fmt.Println("讀檔:", fileName)

		// 判斷檔案是否存在
		_, err := os.Lstat(config.FolderPath + fileName)

		// 檔案不存在
		if err != nil {
			log_info.WithFields(logrus.Fields{
				"trace": "trace-00xx",
				"err":   err,
				//"fileName": "\\\\leapsy-nas3\\CheckInRecord\\20170605-20201011(st)\\201706\\" + fileName,
				"fileName": config.FolderPath + fileName,
			}).Info("檔案不存在")

		} else {
			//檔案若存在

			// 打開每日打卡紀錄檔案(本機要先登入過目的地磁碟機才能正常運作)
			file, err := os.Open(config.FolderPath + fileName)
			//file, err := os.Open("Z:\\" + fileName)

			if err != nil {
				fmt.Println("打開檔案失敗", err)
				log_err.WithFields(logrus.Fields{
					"trace":    "trace-0002",
					"err":      err,
					"fileName": fileName,
				}).Error("打開檔案失敗")
				return
			}

			// 最後回收資源
			defer file.Close()

			fmt.Println("開始讀取檔案")
			log_info.Info("開始讀取檔案: ", fileName)

			// 讀檔
			reader := csv.NewReader(file)

			//行號
			counter := 0

			// 一行一行讀進來
			for {

				line, err := reader.Read()
				counter++

				// 若讀到結束
				if err == io.EOF {

					fmt.Println(fileName, "此份檔案讀取完成")
					log_info.Info("此份檔案讀取完成: ", fileName)

					break
				} else if err != nil {

					close(chanDailyRecord)

					fmt.Println("關閉channel")
					fmt.Println("Error:", err)

					log_err.WithFields(logrus.Fields{
						"trace":    "trace-0003",
						"err":      err,
						"fileName": fileName,
					}).Error("關閉channel")

					break
				}

				// 處理Name編碼問題: 將繁體(Big5)轉成 UTF-8，儲存進去才正常
				log_info.Info("轉成UTF8配合繁體")
				big5Name := line[1]                                             // Name(Big5)
				utf8Name, _, _ := transform.String(big5ToUTF8Decoder, big5Name) // 轉成 UTF-8
				//fmt.Println(utf8Name) // 顯示"名字"

				date := line[0]
				cardId := line[2]
				time := line[3]
				msg := line[4]
				employeeID := line[5]
				dateTime := getDateTime(date, time)

				// 建立每筆DailyRecord物件
				log_info.WithFields(logrus.Fields{
					"檔名":         fileName,
					"行號":         counter,
					"date":       date,       //date
					"utf8Name":   utf8Name,   //name
					"cardId":     cardId,     //cardID
					"time":       time,       //time
					"msg":        msg,        //msg
					"employeeID": employeeID, //employeeID
					"dateTime":   dateTime,
				}).Info("dailyrecord")

				dailyrecord := DailyRecord{
					date,
					utf8Name,
					cardId,
					time,
					msg,
					employeeID,
					dateTime}

				// 存到channel裡面
				chanDailyRecord <- dailyrecord
			}

		}

	}

	close(chanDailyRecord) // 關閉儲存的channel
}

/** 組合年月+時間 */
func getDateTime(myDate string, myTime string) time.Time {

	// fmt.Println("myDate=", myDate)
	// fmt.Println("myTime=", myTime)

	//ex:20201104
	year, err := strconv.Atoi(myDate[0:4])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 year=", year)
		log_err.WithFields(logrus.Fields{
			"err":  err,
			"year": year,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("year=", year)

	month, err := strconv.Atoi(myDate[4:6])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 month=", month)
		log_err.WithFields(logrus.Fields{
			"err":   err,
			"month": month,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("month=", month)

	day, err := strconv.Atoi(myDate[6:8])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 day=", day)
		log_err.WithFields(logrus.Fields{
			"err": err,
			"day": day,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("day=", day)

	//ex:1418
	hr, err := strconv.Atoi(myTime[0:2])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 hr=", hr)
		log_err.WithFields(logrus.Fields{
			"err": err,
			"hr":  hr,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("hr=", hr)

	min, err := strconv.Atoi(myTime[2:4])
	if nil != err {
		fmt.Printf("字串轉換數字錯誤 min=", min)
		log_err.WithFields(logrus.Fields{
			"err": err,
			"min": min,
		}).Error("字串轉換數字錯誤")
	}
	// fmt.Println("min=", min)

	//sec, err := strconv.Atoi(myTime[6:8])
	// if nil != err {
	// 	fmt.Printf("字串轉換數字錯誤 sec=", sec)
	// }

	sec := 0

	// msec, err := strconv.Atoi("0")
	// if nil != err {
	// 	fmt.Printf("字串轉換數字錯誤 msec=", msec)
	// }

	msec := 0

	// fmt.Println("year=", year, "month", month, "day", day, "hr", hr, "sec", sec, "msec", msec)
	t := time.Date(year, time.Month(month), day, hr, min, sec, msec, time.Local)
	// fmt.Printf("%+v\n", t)
	return t

}

/**轉換開始結束日期格式 變成time.Time格式*/
func countDateStartAndEnd_CsvFile() {
	/** 取得開始日期 **/
	stringStartDate := config.StartDate

	// 取出年月日
	startYear, _ := strconv.Atoi(stringStartDate[0:4])
	startMonth, _ := strconv.Atoi(stringStartDate[4:6])
	startDay, _ := strconv.Atoi(stringStartDate[6:8])

	// 轉成time格式
	dateStart = time.Date(startYear, time.Month(startMonth), startDay, 0, 0, 0, 0, time.Local)
	fmt.Println("檔案開始日期:", dateStart)
	log_info.Info("檔案開始日期: ", dateStart)

	/** 取得結束日期 **/
	stringEndDate := config.EndDate

	// 取出年月日
	endYear, _ := strconv.Atoi(stringEndDate[0:4])
	endMonth, _ := strconv.Atoi(stringEndDate[4:6])
	endDay, _ := strconv.Atoi(stringEndDate[6:8])

	// 轉成time格式
	dateEnd = time.Date(endYear, time.Month(endMonth), endDay, 0, 0, 0, 0, time.Local)
	fmt.Println("檔案結束日期:", dateEnd)
	log_info.Info("檔案結束日期: ", dateEnd)
}

/*
 * 將所有日打卡紀錄，全部插入到 mongodb
 */
func insertDailyRecord_CsvFile(chanDailyRecord <-chan DailyRecord, dones chan<- struct{}) {
	//开启loop个协程

	session, err := mgo.Dial(config.MongodbServerIP)
	if err != nil {
		fmt.Println("打卡紀錄插入錯誤(insertDailyRecord)")

		log_err.WithFields(logrus.Fields{
			"trace": "trace-0004",
			"err":   err,
		}).Error("打卡紀錄插入錯誤(insertDailyRecord)")

		panic(err)
		return
	}

	defer session.Close()

	c := session.DB(config.DBName).C(config.Collection)

	for dailyrecord := range chanDailyRecord {
		fmt.Println("插入一筆打卡資料：")
		log_info.Info("插入一筆打卡資料:")

		c.Insert(&dailyrecord)
	}

	dones <- struct{}{}
}

// 等待結束
func awaitForCloseResult(dones <-chan struct{}) {
	for {
		<-dones
		worker--
		if worker <= 0 {
			return
		}
	}
}
